#include "abstracted_storage.h"
namespace nst = stx::storage;
namespace stored{
	static Poco::Mutex m;
	typedef std::unordered_map<std::string, _Allocations*> _AlocationsMap;
	static _AlocationsMap instances;
	_Allocations* _get_abstracted_storage(std::string name){
		_Allocations* r = NULL;
		r = instances[name];
		if(r == NULL){
			r = new _Allocations( std::make_shared<_BaseAllocator>( stx::storage::default_name_factory(name)) );
			instances[name] = r;
		}
		
		return r;
	}
	_Allocations* get_abstracted_storage(std::string name){
		_Allocations* r = NULL;
		{
			nst::synchronized ll(m);
			if(instances.empty())
				nst::journal::get_instance().recover();
			r = instances[name];
			if(r == NULL){
				r = new _Allocations( std::make_shared<_BaseAllocator>( stx::storage::default_name_factory(name)) );
				instances[name] = r;
			}
			r->set_recovery(false);
		}	
		r->engage();
		return r;
	}
	void reduce_all(){
		nst::synchronized ll(m);
		for(_AlocationsMap::iterator a = instances.begin(); a != instances.end(); ++a){
			(*a).second->reduce();
		}
	}
}; //stored

_LockList& get_locklist(){
	static _LockList ll;
	return ll;
}

#include "transactional_storage.h"
#include <fstream>
#include <Poco/BinaryWriter.h>
#include <Poco/BinaryReader.h>
#include <Poco/File.h>



struct journal_state{
public:
	typedef nst::journal_participant participant;
	typedef participant * participant_ref;
	typedef std::shared_ptr<nst::buffer_type> buffer_type_ptr;
	typedef std::map<long long, buffer_type_ptr> buffers_type;
	typedef std::shared_ptr<buffers_type> buffers_type_ptr;
	typedef std::unordered_map<std::string, buffers_type_ptr> block_registery_type ;
	typedef std::unordered_map<std::string, participant_ref> participants_type;
private:
	std::string journal_name;
	std::string compact_name;
	Poco::Mutex jlock;
	
	std::ofstream journal_ostr;
	std::ofstream compacted_ostr;
	Poco::BinaryWriter writer;
	Poco::BinaryWriter compacted_writer;
	size_t bytes_used;
	block_registery_type storages;
	participants_type participants;
public:	
	static const int o_mode = std::ios::binary|std::ios::app;
	journal_state()
	:	journal_name("treestore-journal.dat")
	,	compact_name("treestore-compact-journal.dat")
	,	journal_ostr(journal_name, o_mode)
	,	compacted_ostr(compact_name, o_mode)
	,	writer(journal_ostr)
	,	compacted_writer(compacted_ostr)
	,	bytes_used(0)
	{
	}
	~journal_state(){
	}
private:
	buffers_type_ptr get_buffers(const std::string &name)
	{

		buffers_type_ptr &r = storages[name];

		if(r==nullptr)
		{
			r = std::make_shared<buffers_type>();
		}
		return r;
	}

	buffer_type_ptr get_buffer(const std::string &name, long long addr){

		buffers_type_ptr buffers = get_buffers(name);

		buffer_type_ptr & r = (*buffers)[addr];

		if(r == nullptr)
		{
			r = std::make_shared<nst::buffer_type>();

		}

		return r;
	}

	void add_entry(Poco::UInt32 command, Poco::BinaryWriter &writer, const std::string &name, long long address, const nst::buffer_type& buffer)
	{

		writer.write7BitEncoded(command);
		writer.write7BitEncoded(name.size());
		writer.writeRaw(name );
		writer.write7BitEncoded((Poco::UInt64)address);
		writer.write7BitEncoded(buffer.size());
		if(!buffer.empty())
			writer.writeRaw((const char*)&buffer[0], buffer.size() );
		
	}
	void compact()
	{
		for(block_registery_type::iterator s = storages.begin(); s != storages.end(); ++s){
			std::string name = (*s).first;
			buffers_type_ptr bt = (*s).second;
			for(buffers_type::iterator b = bt->begin(); b != bt->end(); ++b){
				long long address = (*b).first;
				buffer_type_ptr buffer = (*b).second;
				add_entry(nst::JOURNAL_PAGE, compacted_writer, name, address, *buffer);
			}
		}
		compacted_writer.flush();
		
		compacted_writer.stream().flush();
		storages.clear();
		bytes_used = 0;
		
		journal_ostr.seekp(0ll); /// start over
		
	}
public:
	
	void engage_participant(nst::journal_participant* p)
	{
		nst::synchronized s(jlock);
		participants[p->get_name()] = p;
	}

	void release_participant(nst::journal_participant* p)
	{
		nst::synchronized s(jlock);
		participants.erase(p->get_name());
	}

	void add_entry(Poco::UInt32 command, const std::string& name, long long address, const nst::buffer_type& buffer)
	{
		nst::synchronized s(jlock);
		
		add_entry(command, writer, name, address, buffer);
		
	}
	struct _Command{
		Poco::UInt32 command;	
		std::string name;
		Poco::UInt64 address;
		nst::buffer_type buffer;

		void load(Poco::BinaryReader& reader){
			Poco::UInt64 size;
			reader.read7BitEncoded(command);				
			reader.read7BitEncoded(size);
			name.resize(size);
			reader.readRaw(size, name);
			reader.read7BitEncoded(address);
			reader.read7BitEncoded(size);
			buffer.resize(size);
			if(!buffer.empty())
				reader.readRaw( (char*)&buffer[0], buffer.size() );
		}
	};
	void recover(){
		
		typedef std::vector<_Command> _Commands;
		typedef std::unordered_map<std::string, stored::_Transaction*> _PendingTransactions;
		std::ifstream journal_istr(journal_name, std::ios::binary);
		Poco::BinaryReader reader(journal_istr);
		
		storages.clear();

		_PendingTransactions pending;
		_Commands commands;
		
		while(reader.good()){
			_Command entry;
			entry.load(reader);
			if(entry.command == 0 && entry.name.size() == 0){
				break;
			}
			if(entry.command == nst::JOURNAL_PAGE){
				commands.push_back(entry);
			}
			double MB = 1024.0*1024.0;
			if(entry.command == nst::JOURNAL_COMMIT){
				printf("recovering %lld entries at pos %.4g MB\n", (long long)commands.size(), (double)reader.stream().tellg()/MB );
				for(_Commands::iterator c = commands.begin(); c != commands.end(); ++c){
					buffer_type_ptr nbuf = get_buffer((*c).name, (*c).address);
					(*nbuf) = (*c).buffer;
					bytes_used += nbuf->capacity();
				}
				commands.clear();
				for(block_registery_type::iterator s = storages.begin(); s != storages.end(); ++s){
					std::string storage_name = (*s).first;
					buffers_type_ptr storage_buffers = (*s).second;
					stored::_Allocations* storage = stored::_get_abstracted_storage(storage_name);
					storage->set_recovery(true);
					stored::_Transaction* transaction = pending[storage_name];
					if(transaction == nullptr){
						transaction = storage->begin();
						pending[storage_name] = transaction;
					}
					//printf("Recovering %lld pages in %s...\n",(long long)storage_buffers->size(), storage_name.c_str());
					for(buffers_type::iterator b = storage_buffers->begin(); b != storage_buffers->end(); ++b){
						nst::stream_address address = (nst::stream_address)(*b).first;
						buffer_type_ptr buffer = (*b).second;
						nst::buffer_type& dest = transaction->allocate(address, nst::create);
						dest = (*buffer);
						transaction->complete();
					}					
				}
			}
		}
		for(_PendingTransactions::iterator p = pending.begin(); p != pending.end(); ++p){
				std::string storage_name = (*p).first;
				stored::_Transaction* transaction = pending[storage_name];
				stored::_Allocations* storage = stored::_get_abstracted_storage(storage_name);
				printf("recovering %s...\n", storage_name.c_str());
				storage->commit(transaction);
				storage->set_recovery(false);
				printf("recovered %s\n", storage_name.c_str());
		}
		storages.clear();
		bytes_used = 0;
		journal_istr.close();
		journal_ostr.close();
		Poco::File jf(journal_name);
		jf.remove();
		journal_ostr.open(journal_name, o_mode);
	}

	void synch()
	{
		nst::synchronized s(jlock);

		add_entry(nst::JOURNAL_COMMIT, "commit", 0, nst::buffer_type()); /// marks a commit boundary

		writer.flush();
		journal_ostr.flush();
		Poco::File jf(journal_name);
		if(jf.exists()){
			if(jf.getSize() > 1024ll*1024ll*1024ll*4ll || bytes_used > 1024ll*1024ll*1024ll*1ll){
			
				printf("journal file > 4 GB compacting\n");
				//compact();
				for(participants_type::iterator p = (*this).participants.begin(); p != (*this).participants.end(); ++p){				
					(*p).second->journal_commit();
				}
				printf("journal file compacting complete\n");
				journal_ostr.close();
				jf.remove();
				journal_ostr.open(journal_name, std::ios::binary);
			}
		}
	}
};

journal_state& js(){
	static journal_state r;
	return r;
}

namespace stx{
namespace storage{

	journal::journal(){
	}
	journal::~journal(){
	}
	void journal::recover(){
		js().recover();
	}
	void journal::engage_participant(journal_participant* p){
		js().engage_participant(p);
	}
	void journal::release_participant(journal_participant* p){
		js().release_participant(p);
	}
	journal& journal::get_instance(){
		
		static journal j;
		return j;
	}
	/// adds a journal entry written to disk

	void journal::add_entry(Poco::UInt32 command, const std::string &name, long long address, const buffer_type& buffer){
		js().add_entry(command, name, address, buffer);
	}
		
	/// ensures all journal entries are synched to storage

	void journal::synch(){
		js().synch();
	}
};
};