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
	typedef std::unordered_map<nst::pool_string, buffers_type_ptr> block_registery_type ;
	typedef std::unordered_map<nst::pool_string, participant_ref> participants_type;
private:
	nst::pool_string journal_name;
	nst::pool_string compact_name;
	Poco::Mutex jlock;

	std::ofstream journal_ostr;
	std::ofstream compacted_ostr;
	Poco::BinaryWriter writer;
	Poco::BinaryWriter compacted_writer;
	size_t bytes_used;
	nst::u64 sequence;
	nst::u64 last_synch;
	nst::u64 last_check;
	participants_type participants;
	std::vector<nst::u8> journal_buffer;
public:
	static const std::ios_base::openmode o_mode = std::ios::binary|std::ios::app;
	journal_state()
	:	journal_name("treestore-journal.dat")
	,	compact_name("treestore-compact-journal.dat")
	,	journal_ostr(journal_name.c_str(), o_mode)
	,	compacted_ostr(compact_name.c_str(), o_mode)
	,	writer(journal_ostr)
	,	compacted_writer(compacted_ostr)
	,	bytes_used(0)
	,	sequence(0)
	,	last_synch(0)
	,	last_check(0)
	{
	}
	~journal_state(){
	}
private:

	struct log_entry{
		long long sequence;
		long command;
		long name_size;
		long long address;
		long long buffer_size;
	};
	void add_entry(Poco::UInt32 command, Poco::BinaryWriter &writer, const std::string &name, long long address, const nst::buffer_type& buffer)
	{
		//log_journal(name, "entry", address);
		log_entry entry;
		entry.sequence = ++sequence;
		entry.command = command;
		entry.name_size = (long)name.length();
		entry.address = address;
		entry.buffer_size = buffer.size();
		size_t jstart = journal_buffer.size();
		journal_buffer.resize(jstart + sizeof(entry) + name.length() + buffer.size());
		memcpy(journal_buffer.data() + jstart, &entry, sizeof(entry));		
		jstart += sizeof(entry);
		memcpy(journal_buffer.data() + jstart, name.data(), name.length());
		jstart += name.length();
		//writer.writeRaw((const char*)&entry,sizeof(entry));
		//writer.writeRaw(name);
		if(!buffer.empty())
			memcpy(journal_buffer.data() + jstart,buffer.data(), buffer.size());

		if(journal_buffer.size() > 4000000){
			writer.writeRaw((const char*)journal_buffer.data(), journal_buffer.size() );
			journal_buffer.clear();
		}
			//writer.writeRaw((const char*)&buffer[0], buffer.size() );
		/// else if(!(command == nst::JOURNAL_COMMIT || command == nst::JOURNAL_TEMP_INFO)) 	printf("the journal buffer is empty\n");
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
	void flush_buffer(){
		
		nst::synchronized s(jlock);

		if(journal_buffer.size() > 0){				
			writer.writeRaw((const char*)journal_buffer.data(), journal_buffer.size() );
			journal_buffer.clear();
		}
	}
	class _Command{
	public:
		_Command& operator=(const _Command& right){
			sequence = right.sequence;
			command = right.command;
			name = right.name;
			address = right.address;
			buffer = right.buffer;
			return *this;
		}
		_Command(const _Command& right){
			(*this) = right;
		}
		_Command(){
		}
		~_Command(){
		}
		nst::u64 sequence;
		Poco::UInt32 command;
		std::string name;
		Poco::UInt64 address;
		nst::buffer_type buffer;
		template<class _Vector>
		void assign_buffer(_Vector& dest) const {
			dest.clear();
			dest.reserve(buffer.size());
			std::copy(buffer.begin(),buffer.end(),std::back_inserter(dest));
		}
		void load(Poco::BinaryReader& reader){
			log_entry entry;
			memset(&entry, 0 ,sizeof(entry));
			reader.readRaw( (char*)&entry, sizeof(entry) );
			sequence = entry.sequence ;
			command = entry.command;			
			address = entry.address ;	
			buffer.resize(entry.buffer_size);
			name.resize(entry.name_size+1);			
			reader.readRaw( &name[0], entry.name_size ); 
			if(entry.buffer_size)
				reader.readRaw( (char*)buffer.data(), entry.buffer_size ); //

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
		nst::synchronized _llock(llock);
		typedef std::vector<_Command, sta::buffer_pool_alloc_tracker<_Command>> _Commands;
		typedef std::unordered_map<std::string, stored::_Transaction*> _PendingTransactions;
		std::ifstream journal_istr(journal_name.c_str(), std::ios::binary);
		Poco::BinaryReader reader(journal_istr);
		nst::u64 sequence = 0;
		_PendingTransactions pending;
		_Commands commands;
		const double MB = 1024.0*1024.0;
		bool is_error = false;
		try{
			double recovered,last_printed = 0.0;
			
			while(reader.good()){
				_Command record;	
				record.load(reader);
				if(sequence == 0){
					sequence = record.sequence;
				}else if(record.sequence != ++sequence){
					if(record.sequence){
						err_print("bad sequence number %lld received %lld expected", (nst::lld)record.sequence, (nst::lld)sequence);
						is_error = true;
					}
					break;
				}

				if(record.command == nst::JOURNAL_PAGE){

					commands.push_back(record);

				}				
				if(record.command == nst::JOURNAL_COMMIT){
					recovered = (double) reader.stream().tellg();
					if(last_printed + 100*MB < recovered ){
						inf_print("recovering %lld entries at pos %.4g MB", (long long)commands.size(), (double)(recovered/(double)MB) );
						last_printed = recovered;
					}
					for(_Commands::iterator c = commands.begin(); c != commands.end(); ++c){
						_Command &entry =  (*c);
						nst::stream_address add = entry.address;
						std::string storage_name = entry.name;						
						stored::_Allocations* storage = stored::_get_abstracted_storage(storage_name);
						storage->set_recovery(true);
						stored::_Transaction* transaction = pending[storage_name];
						if(transaction == nullptr){

							if(is_valid_storage_directory(storage_name)){
								transaction = storage->begin(true);
								pending[storage_name] = transaction;
							}
						}
						if(storage->transactions_away() > 0){
							nst::buffer_type& dest = transaction->allocate(add, nst::create);
							entry.buffer.swap(dest);
							transaction->complete();
						}
						

					}										
					commands.clear();
				}
			}

		}catch(...){
			err_print("[FATAL] unknown exception during recovery");
			exit(-1);
		}
		
		for(_PendingTransactions::iterator p = pending.begin(); p != pending.end(); ++p){
			std::string storage_name = (*p).first;
			stored::_Transaction* transaction = pending[storage_name];
			stored::_Allocations* allocations = stored::_get_abstracted_storage(storage_name);
			if(allocations->transactions_away()){
				if(is_error){
					allocations->discard(transaction);
				}else{
					allocations->commit(transaction);
				}

				inf_print("recovered %s", storage_name.c_str());
			}
			allocations->set_recovery(false);

		}

		pending.clear();
		(*this).sequence = 0;
		(*this).last_synch = 0;
		bytes_used = 0;
		journal_istr.close();
		journal_ostr.close();
		Poco::File jf(journal_name.c_str());
		const bool backup_log = true;
		if(backup_log){
			nst::pool_string stamp = std::to_string((size_t)os::millis()).c_str();
			nst::pool_string backup = journal_name + "_" +  stamp;
			jf.renameTo(backup.c_str());
		}else{
			jf.remove();
		}
		journal_ostr.open(journal_name.c_str(), o_mode);
	}

	void synch(bool force)
	{
		
		nst::synchronized _llock(llock);
		if(last_synch != sequence){

			add_entry(nst::JOURNAL_COMMIT, "commit", 0, nst::buffer_type()); /// marks a commit boundary
			flush_buffer();
			writer.flush();
			journal_ostr.flush();
			journal_ostr.rdbuf()->pubsync();
			if(force || os::millis() - last_check > 3000){
				last_check = os::millis();
				Poco::File jf(journal_name.c_str());
				if(jf.exists()){
					nst::u64 jsize = 0;
					try{
						jsize = jf.getSize();
					}catch(...){
						err_print("error getting journal size");
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
							inf_print("journal file > n GB compacting");
							//compact();
							log_journal(journal_name.c_str(),"commit",0);
							for(participants_type::iterator p = (*this).participants.begin(); p != (*this).participants.end(); ++p){
								(*p).second->journal_synch_start();
							}
							for(participants_type::iterator p = (*this).participants.begin(); p != (*this).participants.end(); ++p){
								/// test if the data files have not been deleted
								if(is_valid_storage_directory((*p).second->get_name().c_str())){
									(*p).second->journal_commit();
								}
							}
							for(participants_type::iterator p = (*this).participants.begin(); p != (*this).participants.end(); ++p){
								(*p).second->journal_synch_end();
							}
							flush_buffer();
							inf_print("journal file compacting complete");
							journal_ostr.close();
							jf.remove();
							sequence = 0;
							last_synch = sequence;
							journal_ostr.open(journal_name.c_str(), std::ios::binary);
							set_treestore_journal_size( jf.getSize() );
						}
					}
				}
			}
		}  /// last_check
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

	void journal::log(const pool_string &name, const pool_string& jtype, stream_address sa){
		log_journal(name.c_str(), jtype.c_str(), sa);
	}
	/// flush the write buffer

	void flush_buffer(){
		js().flush_buffer();
	}

	/// adds a journal entry written to disk
	
	void journal::add_entry(Poco::UInt32 command, const pool_string &name, long long address, const buffer_type& buffer){
		js().add_entry(command, name.c_str(), address, buffer);
	}

	/// ensures all journal entries are synched to storage

	void journal::synch(bool force){
		js().synch(force);
	}
};
};
