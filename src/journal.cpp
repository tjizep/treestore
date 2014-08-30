#include "abstracted_storage.h"
#include "transactional_storage.h"
#include <fstream>
#include <Poco/BinaryWriter.h>
#include <Poco/BinaryReader.h>
#include <Poco/File.h>
namespace nst = stx::storage;
extern void set_treestore_journal_size(nst::u64 ns);

extern nst::i64 get_treestore_journal_size();

extern nst::u64 get_treestore_journal_lower_max();

static Poco::Mutex llock;
std::ofstream creation("allocations.txt", std::ios::app);

void log_journal(const std::string &name, const std::string& jtype, nst::u64 sa){
	nst::synchronized _llock(llock);

	creation << name << " : " << jtype << " : " << sa << std::endl;
}

class invalid_journal_format : public std::exception{
public:
	/// the journal has a invalid format
	invalid_journal_format() throw(){};

};
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
	nst::u64 sequence;
	nst::u64 last_synch;
	participants_type participants;
public:
	static const std::ios_base::openmode o_mode = std::ios::binary|std::ios::app;
	journal_state()
	:	journal_name("treestore-journal.dat")
	,	compact_name("treestore-compact-journal.dat")
	,	journal_ostr(journal_name, o_mode)
	,	compacted_ostr(compact_name, o_mode)
	,	writer(journal_ostr)
	,	compacted_writer(compacted_ostr)
	,	bytes_used(0)
	,	sequence(0)
	,	last_synch(0)
	{
	}
	~journal_state(){
	}
private:


	void add_entry(Poco::UInt32 command, Poco::BinaryWriter &writer, const std::string &name, long long address, const nst::buffer_type& buffer)
	{
		//log_journal(name, "entry", address);
		writer.write7BitEncoded(++sequence);
		writer.write7BitEncoded(command);
		writer.write7BitEncoded(name.size());
		writer.writeRaw(name );
		writer.write7BitEncoded((Poco::UInt64)address);
		writer.write7BitEncoded(buffer.size());
		if(!buffer.empty())
			writer.writeRaw((const char*)&buffer[0], buffer.size() );
		else if(command != nst::JOURNAL_COMMIT)
			printf("the journal buffer is empty\n");
	}
	void compact()
	{


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
		nst::u64 sequence;
		Poco::UInt32 command;
		std::string name;
		Poco::UInt64 address;
		nst::buffer_type buffer;

		void load(Poco::BinaryReader& reader){
			Poco::UInt64 size;
			reader.read7BitEncoded(sequence);
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

	bool is_valid_storage_directory(const std::string& storage_name) const {
		try{
			Poco::Path pdir = Poco::Path::current();
			pdir.append(Poco::Path(storage_name.substr(2)));
			std::string dtest = pdir.toString();
			pdir.parseDirectory(dtest);
			pdir.popDirectory();
			dtest =  pdir.toString();
			Poco::File tmpDir(dtest);
			return tmpDir.isDirectory();
		}catch(...){
		}
		return false;
	}

	void recover(){

		typedef std::vector<_Command> _Commands;
		typedef std::unordered_map<std::string, stored::_Transaction*> _PendingTransactions;
		std::ifstream journal_istr(journal_name, std::ios::binary);
		Poco::BinaryReader reader(journal_istr);
		nst::u64 sequence = 0;
		nst::u64 last_synch = 0;
		_PendingTransactions pending;
		_Commands commands;
		const double MB = 1024.0*1024.0;
		try{
			while(reader.good()){
				_Command entry;
				entry.load(reader);
				if(entry.sequence != ++sequence){
					if(entry.sequence)
						printf("bad sequence number %lld received %lld expected\n", (nst::lld)entry.sequence, (nst::lld)sequence);

					break;
				}

				if(entry.command == nst::JOURNAL_PAGE){

					commands.push_back(entry);

				}

				if(entry.command == nst::JOURNAL_COMMIT){

					printf("recovering %lld entries at pos %.4g MB\n", (long long)commands.size(), (double)(reader.stream().tellg()/(double)MB) );
					for(_Commands::iterator c = commands.begin(); c != commands.end(); ++c){

						_Command &entry =  (*c);
						nst::stream_address add = entry.address;
						std::string storage_name = entry.name;

						stored::_Allocations* storage = stored::_get_abstracted_storage(storage_name);
						storage->set_recovery(true);
						stored::_Transaction* transaction = pending[storage_name];
						if(transaction == nullptr){

							if(is_valid_storage_directory(storage_name)){
								transaction = storage->begin();
								pending[storage_name] = transaction;
							}
						}
						if(storage->transactions_away() > 0){
							nst::buffer_type& dest = transaction->allocate(add, nst::create);
							dest = entry.buffer;
							transaction->complete();
						}

					}

					commands.clear();

				}
			}

		}catch(...){
			printf("unknown exception during recovery\n");
			exit(-1);
		}
		for(_PendingTransactions::iterator p = pending.begin(); p != pending.end(); ++p){
			std::string storage_name = (*p).first;
			stored::_Transaction* transaction = pending[storage_name];
			stored::_Allocations* allocations = stored::_get_abstracted_storage(storage_name);
			if(allocations->transactions_away()){
				allocations->commit(transaction);

				printf("recovered %s\n", storage_name.c_str());
			}
			allocations->set_recovery(false);

		}

		pending.clear();
		(*this).sequence = 0;
		(*this).last_synch = 0;
		bytes_used = 0;
		journal_istr.close();
		journal_ostr.close();
		Poco::File jf(journal_name);
		jf.remove();
		journal_ostr.open(journal_name, o_mode);
	}

	void synch(bool force)
	{
		nst::synchronized s(jlock);
		if(last_synch != sequence){
			
			add_entry(nst::JOURNAL_COMMIT, "commit", 0, nst::buffer_type()); /// marks a commit boundary

			writer.flush();
			journal_ostr.flush();
			journal_ostr.rdbuf()->pubsync();
			Poco::File jf(journal_name);
			if(jf.exists()){
				nst::u64 jsize = 0;
				try{
					jsize = jf.getSize();
				}catch(...){
					printf("error getting journal size\n");
					return;
				}
				set_treestore_journal_size( jsize );

				if(force || jsize > (nst::u64)get_treestore_journal_lower_max() ){

					nst::u64 singles = 0;
					for(participants_type::iterator p = (*this).participants.begin(); p != (*this).participants.end(); ++p){
						singles += (*p).second->make_singular() ? 1 : 0;
					}
					if(singles == participants.size()){
						set_treestore_journal_size( 0 );
						printf("journal file > n GB compacting\n");
						//compact();
						log_journal(journal_name,"commit",0);
						for(participants_type::iterator p = (*this).participants.begin(); p != (*this).participants.end(); ++p){
							(*p).second->journal_synch_start();
						}
						for(participants_type::iterator p = (*this).participants.begin(); p != (*this).participants.end(); ++p){
							/// test if the data files have not been deleted
							if(is_valid_storage_directory((*p).second->get_name())){
								(*p).second->journal_commit();
							}
						}
						for(participants_type::iterator p = (*this).participants.begin(); p != (*this).participants.end(); ++p){
							(*p).second->journal_synch_end();
						}
						printf("journal file compacting complete\n");
						journal_ostr.close();
						jf.remove();
						sequence = 0;
						last_synch = sequence;
						journal_ostr.open(journal_name, std::ios::binary);
						set_treestore_journal_size( jf.getSize() );
					}
				}
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

	void journal::log(const std::string &name, const std::string& jtype, stream_address sa){
		log_journal(name, jtype, sa);
	}

	/// adds a journal entry written to disk

	void journal::add_entry(Poco::UInt32 command, const std::string &name, long long address, const buffer_type& buffer){
		js().add_entry(command, name, address, buffer);
	}

	/// ensures all journal entries are synched to storage

	void journal::synch(bool force){
		js().synch(force);
	}
};
};
