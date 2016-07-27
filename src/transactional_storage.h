/*****************************************************************************

Copyright (c) 2013, Christiaan Pretorius

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA

*****************************************************************************/
#ifndef _TRANSACTIONAL_STORAGE_H_
#define _TRANSACTIONAL_STORAGE_H_

/// currently treestore doesn't use extensions
/// should the need for them arise then this
/// is where it should be changed

#define TREESTORE_FILE_EXTENSION ""
#define TREESTORE_FILE_SEPERATOR "_"
#include <stdlib.h>
#include <memory>
#include <string>
#include <map>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include <rabbit/unordered_map>
#include <stx/storage/types.h>
#include <stx/storage/basic_storage.h>
#include "Poco/NumberFormatter.h"
#include "Poco/String.h"
#include "Poco/File.h"
#include "Poco/Mutex.h"
#include "Poco/Data/Common.h"
#include "Poco/Data/Session.h"
#include "Poco/Data/BLOB.h"
#include "Poco/Data/Limit.h"
#include "Poco/Data/SQLite/Connector.h"
#include "Poco/Exception.h"
#include "Poco/AtomicCounter.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/Logger.h"
#include "Poco/LogFile.h"
#include "Poco/StringTokenizer.h"
#include "Poco/Timestamp.h"
#include "Poco/DirectoryIterator.h"
#include "Poco/BinaryWriter.h"
#include "Poco/BinaryReader.h"
#include "Poco/File.h"
#include "Poco/ThreadLocal.h"
#include "rednode.h"
#include <stx/storage/pool.h>
#include <Poco/UUIDGenerator.h>
inline Poco::UUID create_version(){
	return Poco::UUIDGenerator::defaultGenerator().create();
}


extern char * treestore_contact_points;
extern char	treestore_block_read_ahead;
#ifdef _MSC_VER
#define fseek64 _fseeki64
#define ftell64 _ftelli64
#endif
/// TODODONE: initialize 'next' to address of last block available
/// TODODONE: unmerged version recovery
/// TODODONE: version storage naming
/// TODODONE: merge versions on commit or if there are too many versions
namespace ns_umap = rabbit; //rabbit;
namespace storage_workers{
	typedef asynchronous::QueueManager<asynchronous::AbstractWorker> _WorkerManager;


	extern unsigned int get_next_counter();
	extern const int MAX_WORKER_MANAGERS;
	extern _WorkerManager & get_threads(unsigned int id);
};
namespace stored{
    extern void reduce_all();
	extern void reduce_aged();
};
namespace stx{
namespace storage{
	extern u64 get_lr_timer();
	typedef std::basic_string<char, std::char_traits<char>, sta::buffer_pool_alloc_tracker<char> > pool_string;

	extern Poco::UInt64 ptime;
	extern Poco::UInt64 last_flush_time;		/// last time it released memory to disk
	extern Poco::Mutex& get_single_writer_lock();
	extern long long col_use;
	/// defines a concurrently accessable transactional storage providing ACID properties
	/// This class should be extended to provide encoding services for a b-tree or other
	/// storage oriented data structures

	/// a name factory for some persistent allocators
	struct default_name_factory{

		pool_string name;
		
		default_name_factory(const pool_string &name) : name(name){
		}

		default_name_factory (const default_name_factory& nf){
			*this = nf;
		}

		~default_name_factory(){

		}

		default_name_factory & operator=(const default_name_factory& nf){
			(*this).name = nf.name;
			return *this;
		}
		const pool_string & get_name() const {
			return (*this).name;
		}
	};
	/// defines a storage class based on sqlite for implementing virtual allocation
	/// the default allocation traits

	template<typename _AddressType>
	struct address_policy{// the default policy for any address
		static const int size = sizeof(_AddressType);
		/// how many elements in the tuple that is required to store an address
		static const int cardinality = 1;

		/// are there any conversions required like to string etc.

	};

	template<>
	struct address_policy<unsigned long long>{// the default 64-bit integer address policy
		static const int size = 8;
		static const int cardinality = 1;
	};

	template<>
	struct address_policy<std::vector<unsigned long long>>{// the default address tuple policy
		static const int size = 8;
		static const int cardinality = 8; //going to store eight integers per vector

	};

	template<>
	struct address_policy<unsigned long>{// the default 32-bit integer address policy
		static const int size = 4;
		static const int cardinality = 1;

	};
	template<typename _AddressType>
	struct memmory_allocation_policy{
	};

	class journal_participant
	{
	public:
		virtual void journal_commit() = 0;
		/// notify particpants that the journal synch has started
		virtual void journal_synch_start() = 0;
		virtual void journal_synch_end() = 0;
		/// notify the participant that any stray versions should
		/// be merged
		virtual bool make_singular() = 0;
		virtual pool_string get_name() const = 0;
		bool participating;
		journal_participant():participating(false){};

	};

	/// the global journal for durable and atomic transactions
	/// the journal is a singleton working on a single writer
	/// thread
	class journal{
	private:
		journal();
	public:
		static journal& get_instance();
	public:
		~journal();

		/// recovery interface
		/// add_recoverer

		/// commit if journal has reached its limit
		/// add_committer
		void engage_participant(journal_participant* p);
		void release_participant(journal_participant* p);
		/// adds a journal entry written to disk

		void add_entry(Poco::UInt32 command, const pool_string& name, long long address, const buffer_type& buffer);

		/// ensures all journal entries are synched to storage

		void synch(bool force = false);

		/// called during startup to recover unwritten transactions

		void recover();

		/// log new address creation

		void log(const pool_string &name, const pool_string& jtype, stream_address sa);
	};

	template <typename _AddressType, typename _BlockType = std::vector<u8>>
	class memory_allocator{
	public:
		typedef _AddressType address_type;
		typedef _BlockType block_type;
		typedef block_type * block_reference;
		typedef address_policy<address_type> allocation_policy;
		typedef memmory_allocation_policy<address_type> memmory_policy;

		block_type empty_block;
	private:
		
		typedef ns_umap::unordered_map<address_type, block_reference, rabbit::rabbit_hash<address_type>, std::equal_to<address_type>, sta::buffer_pool_alloc_tracker<address_type> > _Allocations;
		_Allocations allocations;
		address_type next;
	public:
		memory_allocator() : next(32){
		}
		~memory_allocator(){
		}

		/// returns the block defining the end - callers of allocate should check if this condition is reached

		const block_type& end() const {
			return empty_block;
		}

		/// returns true if this block is the end of storage condition

		bool is_end(const block_type& b) const {
			return (&b == &(*this).end());
		}

		/// allocate a new or existing buffer, new denoted by which == 0 else an existing
		/// buffer with the specfied stream address is returned - if the non nil address
		/// does not exist the end is returned

		block_type& allocate(address_type &which, storage_action ){
			block_reference result = NULL;
			if(which){
				result = allocations[which];
				if(!result){
					result = &empty_block;
				}
			}else{
				which = ++next;
				result = new block_type ;
				allocations[which] = result;
			}
			return *result;
		}

	};


	template<typename _AddressType>
	struct sqlite_allocation_policy{
		/// block size for sqlite allocation
		static const int block_size = 2048;
	};
	///using namespace Poco::Data::Keywords;
	using namespace Poco::Data;
	//using Poco::Data::Session;
	//using Poco::Data::Statement;
	typedef Poco::ScopedLockWithUnlock<Poco::Mutex> syncronized;
	/// a file io exception
	class FileIOException : public std::exception{
	public:	
		FileIOException() throw() {
		}
	};
	/// Exceptions that may be thrown in various circumstances
	class InvalidAddressException : public std::exception{
	public: /// The address required cannot exist
		InvalidAddressException() throw() {
		}
	};

	class InvalidStorageException : public std::exception{
	public: /// The storage has invalid meta data
		InvalidStorageException() throw() {
		}
	};
	class NonExistentAddressException : public std::exception{
	public: /// The address required does not exist
		NonExistentAddressException() throw() {
		}
	};


	class InvalidStorageAction : public std::exception{
		public: /// The storage action supplied is inconsistent with the address provided (according to contract)
		InvalidStorageAction() throw() {
		}
	};

	class WriterConsistencyViolation : public std::exception{
		public: /// The writing version has uncommitted dependencies
		WriterConsistencyViolation() throw() {
		}
	};

	class ConcurrentWriterError : public std::exception{
	public: /// There was more than one transaction writing simultaneously
		ConcurrentWriterError() throw() {
		}
	};

	class InvalidReaderDependencies : public std::exception{
		public: /// The reader has no locks
		InvalidReaderDependencies() throw() {
		}
	};

	class InvalidVersion : public std::exception{
		public: /// The writing version has uncommitted dependencies
		InvalidVersion() throw() {
		}
	};

	/// this is an exception thrown to enforce the single writer policy - if the single writer lock isn't used
	class InvalidWriterOrder : public std::exception{
		public: /// The writing transaction has committed to late, another writer already committed
		InvalidWriterOrder() throw() {
		}
	};
	// this is an exception thrown when a non writing transaction is trying to commit
	class InvalidTransactionType : public std::exception{
		public: /// The reading transaction should not commit
		InvalidTransactionType() throw() {
		}
	};

	class InvalidReaderCount : public std::exception{
		public: /// version released != engaged
		InvalidReaderCount() throw() {
		}
	};
	enum journal_commands{
		JOURNAL_PAGE = 0,
		JOURNAL_COMMIT,
		JOURNAL_TEMP_INFO
	};

	template <typename _AddressType, typename _BlockType>
	class sqlite_allocator {
	public:

		typedef _AddressType address_type;

		typedef _BlockType block_type;

		typedef block_type * block_reference;

		typedef std::shared_ptr<block_type> shared_block_reference;

		/// allocation policy for the kind of addresses (vector or scalar) which will be used to address allocations

		typedef address_policy<address_type> allocation_policy;

		/// defines how sqlite will be used as an allocator i.e. block sizes etc.

		typedef sqlite_allocation_policy<address_type> sqlite_policy;

		/// SQL address vector

		typedef std::vector<Poco::UInt64> _SQLAddresses;

		/// version map - address to version
		/// , rabbit::rabbit_hash<address_type>, std::equal_to<address_type>, sta::buffer_pool_alloc_tracker<address_type>
		/// , rabbit::rabbit_hash<address_type>, std::equal_to<address_type>, sta::buffer_pool_alloc_tracker<address_type> 
		typedef ns_umap::unordered_map<address_type, version_type, rabbit::rabbit_hash<address_type>, std::equal_to<address_type>, sta::buffer_pool_alloc_tracker<address_type> > _Versions; 



	private: /// private types
		static const u64 MAX_READAHEAD_BITS = 12;
		static const u64 MAX_READAHEAD = (1 << MAX_READAHEAD_BITS);
		struct block_descriptor {

			block_descriptor(version_type version) : clock(0), mod(read), use(0), compressed(false), version(version){
				//add_buffer_use(sizeof(*this));
			}
			void clear(version_type version){
				clock = 0;
				mod = read;
				compressed = false;
				(*this).version = version;
				block.clear();
			}
			block_descriptor(const block_descriptor& right) {
				//add_buffer_use(sizeof(*this));
				*this  = right;
			}
			block_descriptor& operator=(const block_descriptor& right){
				clock = right.clock;
				use = right.use;
				mod = right.mod;
				block = right.block;
				compressed = right.compressed;
				return *this;
			}
			~block_descriptor(){
				//remove_buffer_use(sizeof(*this));
			}
			void set_storage_action(storage_action action){
				if(mod != read) return;
				/// don't overide create actions - for replication
				if(mod != create)
					mod = action;
			}
			void set_written(){
				this->mod = read;
			}
			storage_action get_storage_action (){
				return mod;
			}
			bool is_modified() const {
				return (mod != read);
			}
			u32 clock;
			storage_action mod;
			ptrdiff_t use;
			bool compressed;
			version_type version;
			block_type block;
		} ;

		typedef block_descriptor* ref_block_descriptor;
		typedef std::vector<block_descriptor*, sta::buffer_pool_alloc_tracker<block_descriptor*> > _Descriptors;				
		typedef ns_umap::unordered_map<address_type, ref_block_descriptor, rabbit::rabbit_hash<address_type>, std::equal_to<address_type>, sta::buffer_pool_alloc_tracker<address_type> > _Allocations;
		typedef std::unordered_set<address_type, rabbit::rabbit_hash<address_type>, std::equal_to<address_type>, sta::buffer_pool_alloc_tracker<address_type>  > _Changed;
		typedef std::set<address_type> _Addresses;
	private:
		/// per instance members
		Poco::AtomicCounter references;
									/// counts references to this instance through release and engage methods

		mutable Poco::Mutex lock;	/// locks the instance

		bool transient;				/// if this is flagged then the resources held must be deleted

		bool busy;					/// instance is busy complete should be called

		_Versions versions;			/// map of transient versions

		_Allocations allocations;	/// the live unflushed allocations		

		//_Changed changed;			/// a list of addresses which changed during the current transaction

		address_type next;			/// next virtual address to allocate

		address_type currently_active;
									/// currently active address

		block_type empty_block;		/// the empty block returned as end of storage

		pool_string table_name;		/// table name in sqlite

		pool_string name;			/// external name to users of class

		pool_string extension;		/// extension of data file

		u64 changes;				/// counts the changes made

		version_type version;		/// the version of this allocator
		version_type allocated_version;
		/// to add a block

		std::shared_ptr<Poco::Data::Statement> insert_stmt ;

		/// to retrieve a block

		std::shared_ptr<Poco::Data::Statement> get_stmt ;

		/// to check if block exists

		std::shared_ptr<Poco::Data::Statement> exists_stmt ;

		/// has-a transaction been started

		bool transacted;

		std::shared_ptr<Session> _session;

		u32 clock;

		ptrdiff_t limit;			/// flush limit at which LRU partial flush starts

		ptrdiff_t _use;				/// the ammount of bytes allocated in buffers

		u64 decoders_away;			/// decoders busy decoding

		bool is_new;				/// flag set when storage file exists used to suppress file creation if there is nothing in it

		bool is_readahead;			/// flag set to enable read ahead

		bool is_all_loaded;			/// set when all was loaded

		bool is_read_cache;			/// control read caching - existing pages are kept

		//cassandras<block_type> lego;
		//sixsided::transacted_blocks<block_type>
		red::client_allocator_proxy lego;		
									/// file blocks

		_Descriptors recycled;		/// recycled blocks

		typename _Allocations::iterator finder;

		nst::u64 file_size;			/// file storage size
		
		nst::u64 timer;				/// timer - time when last allocation took place

		ptrdiff_t get_block_use(const block_descriptor& v){
			return v.block.capacity() + sizeof(block_type)+32; /// the 32 is for the increase in the allocations table
		}

		ptrdiff_t get_block_use(const ref_block_descriptor& v){
			return get_block_use(*v);
		}

		/// this function reflects the ammount of bytes changed since the blocks use where last updated

		ptrdiff_t reflect_block_use(block_descriptor& v){
			ptrdiff_t use = v.use;
			v.use = get_block_use(v); ///sizeof(shared_block_reference)
			return  v.use - use;///reflection
		}

		ptrdiff_t reflect_block_use(const ref_block_descriptor& v){
			return reflect_block_use(*v);
		}

		static pool_string get_storage_path(){

			return ""; //".\\";
		}

		/// does this table exist in storage

		bool has_table(pool_string name){

			int count = 0;
			get_session() << "SELECT count(*) FROM sqlite_master WHERE type in ('table', 'view') AND name = '" << name << "';", into(count), now;
			get_session() << "PRAGMA shrink_memory;", now;
			return count > 0;
		}

		void create_allocation_table(){
			if(!has_table(table_name)){
				 pool_string sql = "CREATE TABLE " + table_name + "(";
				 sql += "a1 integer primary key, ";
				 sql += "dsize integer, ";
				 sql += "data BLOB);";
				 get_session() << sql, now;
			}
		}
		typedef std::vector<address_type> _AddressList;
		
		typedef std::vector<Poco::Data::BLOB> _BLOBs;
		u64 selector_address;
		u64 current_address;
		u64 max_address;
		u64 exists_count;
		Poco::Data::BLOB encoded_block;
		Poco::UInt64 current_size;
		block_type current_block;
		block_type compressed_block;
		
		struct block_request{
			address_type start;
			_AddressList addresses;
			_AddressList sizes;
			_BLOBs blocks;
			_Descriptors decoded;
			void init_use(){
				for(_BLOBs::iterator b = blocks.begin(); b!=blocks.end(); ++b){
					add_buffer_use((*b).content().capacity());
				}
			}
			void decode_all(){
				//init_use();
				decoded.clear();

				for(_BLOBs::iterator b = blocks.begin(); b!=blocks.end(); ++b){
					block_descriptor* result = new block_descriptor(version_type());
					///decompress_zlibh(result->block, (*b).content());
					result->block.resize((*b).size());
					memcpy(&result->block[0], &((*b).content()[0]),(*b).size());					
					//remove_buffer_use((*b).content().capacity());
					(*b).clear(true);
					//std::vector<char> e;
					//(*b).swap(e);
					decoded.push_back(result);
				}
				///printf("decoded %lld blocks in %lld millis\n",(long long)blocks.size(),(long long)os::millis()-ts);

			}
		};


		address_type calc_block_start(address_type add ){
			return (add & (~((address_type)MAX_READAHEAD-(address_type)1)));
		}

		template <typename T> Binding<T>* bind(const T& t)
			/// Convenience function for a more compact Binding creation.
		{
			return new Binding<T>(t);
		}

		void create_statements(){
			insert_stmt = std::make_shared<Poco::Data::Statement>( get_session() );
			*insert_stmt << "INSERT OR REPLACE INTO " << (*this).table_name << " VALUES(?,?,?) ;", bind(current_address), bind(current_size), bind(encoded_block);

			get_stmt = std::make_shared<Poco::Data::Statement>( get_session() );

			*get_stmt << "SELECT a1, dsize, data FROM " << (*this).table_name << " WHERE a1 = ?;"
				, into(current_address), into(current_size), into(encoded_block)
				, bind(selector_address);

			exists_stmt = std::make_shared<Poco::Data::Statement>( get_session() );
			/// block readahead statements
			*exists_stmt << "SELECT count(*) FROM " << (*this).table_name << " WHERE a1 = ?;"
				, into(exists_count)
				, bind(selector_address);

			if(!lego.is_open()){

				max_address = 0;

				get_session() << "SELECT max(a1) AS the_max FROM " << (*this).table_name << " ;" , into(max_address), now;
				(*this).next = std::max<address_type>((address_type)max_address, (*this).next); /// next is pre incremented
			}

		}
		inline bool has_session() const {
			return _session != nullptr;
		}
		/// open the connection if its closed

		Session& get_session(){
			if(_session != nullptr){
				return *_session;
			}
			if(true){
				if(get_version() == version_type() && !lego.is_open()){
					lego.open(is_new,name.c_str());
					max_address = 0;
					max_address = std::max<address_type>(max_address, lego.max_block_address());
					(*this).next = std::max<address_type>((address_type)max_address, (*this).next); /// next is pre incremented
				}
			}
			if(_session == nullptr){
				pool_string npath = get_storage_path() + name + extension;
				_session = std::make_shared<Session>("SQLite", npath.c_str());//SessionFactory::instance().create
				is_new = false;
				create_allocation_table();
				create_statements();
				pool_string path = get_storage_path() + name + extension;
				Poco::File df ( path.c_str());
				file_size = df.getSize();
					
				if(lego.is_open()){
					
				}
				//printf("is_open %s\n", name.c_str());
			}
			return *_session;
		}
		void add_buffer(const address_type& w, const block_type& block){
			current_address = w;			
			current_size = block.size()*sizeof(typename block_type::value_type);			
			

			if(lego.is_open()){
				if(!block.empty()){
					lego.store(w, block);
					return;
				}
				return;
			}
			
			current_address = w;
			/// assumes block_type is some form of stl vector container
			current_size = block.size()*sizeof(typename block_type::value_type);
			encoded_block.clear();
			if(!block.empty()){

				//compress_block
				//compressed_block = block;
				//inplace_compress_zlibh(compressed_block);
				///current_size = compressed_block.size()*sizeof(typename block_type::value_type);
				current_size = block.size();
				encoded_block.clear();
				if(!lego.is_open())
					encoded_block.assignRaw((const char *)&block[0], (size_t)current_size);
			}

			insert_stmt->execute();
			
		}

		/// returns true if the buffer with address specified by w has been retrieved
		bool get_exists(const address_type& w){
			if(is_new) return false;
			if(w >= next) return false;
			get_session();
			
			
			selector_address = w;
			exists_count = 0;
			if(lego.is_open()){
				return lego.contains(w);					
			}
			exists_stmt->execute();
			return exists_count > 0;
		}
		bool get_buffer(const address_type& w){
			if(!w)
				throw InvalidAddressException();
			
			if(is_new) return false;
			get_session();
			///read_ahead(w+1, std::min<address_type>(w+8,next)); /// read 8 addresses at a time
			current_block.clear();
			if(lego.is_open()){
				version_type version;
				if( lego.get(w,version,current_block) ){
					update_versions(w) = version;
					current_size = current_block.size();
					current_address = w;
					return true;
				}
				return false;
			}
			selector_address = w;
			///queue_block_read(w);
			get_stmt->execute();
			current_block.clear();
			if(current_address == selector_address){
				//decompress_zlibh(current_block, encoded_block.content());
				current_block.resize(encoded_block.size());
				memcpy(&current_block[0], &(encoded_block.content()[0]),encoded_block.size());				
			}
			
			return (current_address == selector_address); /// returns true if a record was retrieved

		}

		
		
		bool inject(stream_address which, block_descriptor* descriptor){
			synchronized s(lock);
			if(allocations.count(which)==0){
				up_use(reflect_block_use(descriptor));
				descriptor->version = version_off(which);
				descriptor->set_storage_action(read);
				allocations[which] = descriptor;
				return true;
			}
			return false;
		}
		/// sort acording to the clock value on a descriptor address pair

		struct less_clock_descriptor{
			bool operator()(const std::pair<address_type, ref_block_descriptor>& left, const std::pair<address_type, ref_block_descriptor>& right){
				return left.second->clock < right.second->clock;
			}
		};

		/// sort acording to the address of a descriptor address pair

		struct less_address{
			bool operator()(const std::pair<address_type, ref_block_descriptor>& left, const std::pair<address_type, ref_block_descriptor>& right){
				return left.first < right.first;
			}
		};


		/// adds the argument change to the _use variable
		void up_use(ptrdiff_t change){
			/// add_buffer_use (change);
			_use += change;
		}
		void down_use(ptrdiff_t change){
			/// remove_buffer_use (change);
			_use -= change;
		}
		inline ptrdiff_t get_use() const {
			return _use;
		}

		/// flush any or all excess data to disk, the factor determines the fraction
		/// of the total use to release i.e. a value of 0 releases all
		/// the default value will release 25% (1-0.75) of the total used memory

		/// writes changed buffers to disk while [release]-ing until
		/// the memory used is a [factor] of the original
		void flush_back
		(	double factor = 0.75
		,	bool release = true
		,	bool write_all = false
		){
			if(result != nullptr){
				//up_use(reflect_block_use(*result));
				result = nullptr;
			}		
			
			ptrdiff_t before = get_use();
			typedef std::pair<address_type, ref_block_descriptor> _AddressedBlockPair;
			typedef std::vector<_AddressedBlockPair> _Blocks; //, ::sta::tracker<_AddressedBlockPair, ::sta::buffer_counter> 

			_Blocks blocks;
			_Blocks by_address;

			for(typename _Allocations::iterator a = allocations.begin(); a!=allocations.end(); ++a){
				up_use(reflect_block_use((*a).second)); /// update to correctly reflect
				if((*a).second != result)
					blocks.push_back((*a));
			}

			std::sort(blocks.begin(), blocks.end(), less_clock_descriptor());

			u64 mods = 0;
			for(typename _Blocks::iterator b = blocks.begin(); b != blocks.end(); ++b){
				if(write_all || (*b).second->is_modified()){
					/// write blocks will be written in address order
					by_address.push_back(*b);
					mods++;
				}else if(get_use() > (before*factor)){ /// flush read blocks in LRU order
					if(release)
					{
						versions[(*b).first] = (*b).second->version;	
						up_use(reflect_block_use((*b).second)); /// update to correctly reflect						
						allocations.erase((*b).first);

						down_use( get_block_use((*b).second) );
						delete (*b).second;
					}
				}

			}
			if(!mods && get_use() <= (before*factor)){
				/// printf("reduced storage %s\n", get_name().c_str());
				return;
			}

			/// flush io in ascending stream address order
			/// TODO: rather flush multiple consecutive pages in blocks regardless of
			/// clock value - maybe access by address rather than clock value to improve
			/// io further on reads. i.e. if you do read-aheads the clock value of
			/// of adjacent pages will be the same. To do the same thing on writes
			/// flushing of solitary pages should be deferred for as long as possible
			/// preferrably until they become less lonely and the group should be flushed
			/// together this also has the added benefit of keeping the storage
			/// less fragmented to begin with.
			/// thereby multiplying write speeds (because of access latency)
			/// but possibly reducing read speeds.
			/// although most workloads have their reading  and writing to and from the same
			/// pages - it may be possible to conjure workloads to become read-write
			/// adjacent - maybe there's a better word for that.

			std::sort(by_address.begin(), by_address.end(), less_address());
			_begin(); /// ensure storage can write
			for(typename _Blocks::iterator b = by_address.begin(); b != by_address.end(); ++b){

				mods--;
				versions[(*b).first] = (*b).second->version;				
				add_buffer((*b).first, (*b).second->block);
				(*b).second->set_written();
				up_use(reflect_block_use((*b).second)); /// update to correctly reflect
				if(release)
				{
					allocations.erase((*b).first);

					down_use( get_block_use((*b).second) );
					delete (*b).second;

				}
				if(mods == 0 && get_use() <= (before*factor)){
					break;
				}
			}
			if(release){
				clear_recycler();
			}
			get_session() << "PRAGMA shrink_memory;", now;
			if(write_all){
				//(*this).changed.clear();
				(*this).changes = 0;
			}
		}

		
		void check_use(){			
			/// this may cause problems
			if(!transient) return;
			
			//if(treestore_current_mem_use > treestore_max_mem_use){
			if(buffer_allocation_pool.is_near_depleted() && (*this)._use > 1024*1024*2){
				if(transient) inf_print("reduce transient block cache use - %s",this->get_name().c_str());
				ptrdiff_t before = get_use();
					
				flush_back(0.3,true);
				get_session() << "PRAGMA shrink_memory;", now;
				last_flush_time = ::os::millis();
				//printf("[TS] [DEBUG] flushed data %lld KiB - local before %lld KiB, now %lld KiB\n", (long long)total_use/1024, (long long)before/1024, (long long)get_use()/1024);
				
			}
		}
		version_type version_off(address_type which){
			version_type r = has_version(which);
			if(r == version_type()){

				r = (*this).get_version();
				update_versions(which) = r;
			}

			return r;
		}
		void clear_recycler(){
			while(!recycled.empty()){
				ref_block_descriptor r = nullptr;
				r = recycled.back();
				recycled.pop_back();				
				down_use( get_block_use(r) );
				delete r;
			}
		}
		ref_block_descriptor create_block(const address_type& which){
			ref_block_descriptor r = nullptr;
			if(!recycled.empty()){
				r = recycled.back();
				r->clear(version_off(which));
				recycled.pop_back();
			}else{
				r = new block_descriptor(version_off(which));
			}
			
			return r;
		}
		void recycle_block(const address_type& at, ref_block_descriptor block){
			up_use(reflect_block_use(block)); /// update to correctly reflect						
			
			if(allocations.erase(at)==1){
				recycled.push_back(block);			
			}else{
				down_use( get_block_use(block) );
				delete block;
			}
		}
		bool find_block(const address_type& which, ref_block_descriptor& result){
			
			return allocations.get(which,result);
			
			if(false){
				typename _Allocations::iterator fr = allocations.find(which);
				
				bool finder = fr != allocations.end(); //allocations.get(which,result);//
				if(finder){
					result = (*fr).second;
				}
				return finder;
			}
		}
	protected:
		block_type read_block;
		block_type & _allocate(address_type& which, storage_action how){
			
			lock.lock();
			touch();
			check_use();
			if(how != read)
				++((*this).changes);
			busy = true;
			allocated_version = version_type();
			
			currently_active = 0;
			result = nullptr;
			if(which){
				bool finder = find_block(which, result);
				
				if(!finder){
					/// load from storage
					
					if(how!=create && (*this).get_buffer(which))
					{
						
						if(!this->is_read_cache){
							read_block.clear();
							read_block.insert(read_block.begin(), current_block.begin(), current_block.end());
							currently_active = which;
							allocated_version = version_off(which);
							return read_block;
						}
						result = create_block(which);
						result->block.insert(result->block.begin(), current_block.begin(), current_block.end());
						allocations[which] = result;
						
					}else if(how == create)
					{
						result = create_block(which);
						allocations[which] = result;
						next = std::max<address_type>(which, next);

					}else
					{
						/// defines invalid block - caller must check ?
						return empty_block;
					}

				}else{
					///result = (*finder).second; /// allocations[which];
				}
				result->set_storage_action(how);
			}else{
				if(how != create) /// this is also probably a bit pedantic
					throw InvalidStorageAction();

				which = ++next;
				//printf("[CREATE ADDRESS] %lld %s\n", (long long)which, get_name().c_str());
				result = create_block(which); //std::make_shared<block_descriptor>();
				allocations[which] = result;
				result->set_storage_action(create);

			}
			//if(result->get_storage_action() != read){
			//	changed.insert(which); /// this action flags a change
			//
			//}

			up_use(reflect_block_use(result));

			result->clock = ++clock;
			currently_active = which;
			allocated_version = result->version;
			return result->block;
		}

		void assign_version(version_type version){
			if(result && currently_active) {
				update_versions(currently_active) = version;
				result->version = version;
			}else if(currently_active){
				err_print("version not set");
			}
		}
		version_type has_version(address_type which){

			typename _Versions::iterator v = versions.find(which);
			if(v != versions.end()){
				return (*v).second;
			}
			return version_type();

		}
		nst::version_type& update_versions(address_type v){
			return versions[v];
		}

	public:

		/// return a list of versions for the request of pairs only if larger one could be found
		template<typename _VersionRequests>
		nst::u64 get_greater_version_diff(_VersionRequests& request){
			nst::u64 responses = 0;
			
			for(typename _VersionRequests::iterator v = request.begin(); v != request.end(); ++v){
				version_type ver = version_type();
				bool found = false;
				///versions.get((*v).first,ver);
				/// TODO: IF there are intermittent crashes or corruptions this is the first place to look
				if(!found){
					//_Versions::iterator fv = versions.find((*v).first);
					if(versions.get((*v).first,ver)){
						//ver = (*fv).second;						
						found = true;
					}
				}
				if(!found){
					syncronized ul(lock);
					if(!found){
						finder = allocations.find((*v).first);
						if(finder != allocations.end()){
							ver = std::max<version_type>(ver, (*finder).second->version );
							versions[(*v).first] = ver;
							found = true;
						}
					}
				
					if(!found){
						//if(lego.is_open()){
						//	if(!lego.get_version((*v).first, ver)){
						//		ver = this->get_version();
						//	}
						//}else{
							if(get_exists((*v).first)){
								ver = this->get_version();
								versions[(*v).first] = ver;
							
							}
						//}
					}
				}

				if((*v).second < ver ){
					(*v).second = ver;
					++responses;
				}
			}
			return responses;


		}
		/// gets a version
		version_type get_version() const {
			return (*this).version;
		}
		/// sets a version
		void set_version(version_type version){
			(*this).version = version;
		}
		/// get the list of addresses stored
		
		void get_addresses(_Allocations& out){
			synchronized s(lock);
			if(_session != nullptr){
				_SQLAddresses addresses;
				get_session() << "select a1 from " << (*this).table_name << " ;" , into(addresses), now;
				for(_SQLAddresses::iterator sa = addresses.begin(); sa != addresses.end(); ++sa){
					out[(stream_address)(*sa)] = nullptr;
				}
			}
			for(typename _Allocations::iterator a = allocations.begin(); a!=allocations.end(); ++a){
				out[(stream_address)(*a).first] = (*a).second;
			}
			/// out contains a unique list of addresses in this allocator
		}
		
		void get_addresses(_Addresses& out){
			synchronized s(lock);
			if(_session != nullptr){
				_SQLAddresses addresses;
				get_session() << "select a1 from " << (*this).table_name << " ;" , into(addresses), now;
				for(_SQLAddresses::iterator sa = addresses.begin(); sa != addresses.end(); ++sa){
					out.insert((stream_address)(*sa));
				}
			}
			for(typename _Allocations::iterator a = allocations.begin(); a!=allocations.end(); ++a){
				out.insert((stream_address)(*a)) ;
			}
			/// out contains a unique list of addresses in this allocator
		}
		void get_modified_addresses(_Addresses& out){
			synchronized s(lock);
			if(_session != nullptr){
				_SQLAddresses addresses;
				get_session() << "select a1 from " << (*this).table_name << " ;" , into(addresses), now;
				for(_SQLAddresses::iterator sa = addresses.begin(); sa != addresses.end(); ++sa){
					out.insert((stream_address)(*sa));
				}
			}
			for(typename _Allocations::iterator a = allocations.begin(); a!=allocations.end(); ++a){
				if((*a).second->is_modified()){
					out.insert((stream_address)(*a).first);
				}
			}
			/// out contains a unique list of addresses in this allocator
		}

		/// set read cache to desired state
		void set_read_cache(bool is_read_cache){
			synchronized s(lock);
			this->is_read_cache = is_read_cache;
		}
		
		void load_all(){			
			//if(treestore_column_cache!=0) 
			//	return;
			synchronized s(lock);
			/// check if this file can be loaded
			if(!is_all_loaded){
				if
				(	treestore_block_read_ahead==TRUE
				&&	(buffer_allocation_pool.get_total_allocated() + allocation_pool.get_total_allocated() + this->file_size) < treestore_max_mem_use 
				){											
					///TODO: opportunistically read until memory exhausted
					nst::u64 start = 0;
					nst::u64 remaining = next;
					read_ahead(start, remaining); /// read until end
					
					is_all_loaded = true;
				}else if(treestore_block_read_ahead==TRUE){
					inf_print("cannot block read-ahead because insufficient memory");

				}
			}
		}
		
		/// read all the blocks into cache or until memory limit is reached
		void read_ahead(address_type start_address, u64 count){
			get_session();
			address_type start = start_address;
			u64 blocks = 0;
			size_t ts = os::millis(),tst = os::millis();
			
			typedef std::shared_ptr<block_request> block_request_ptr;
			inf_print("readahead %s starting %lld s", get_name().c_str(),(long long)count);
			//while(blocks < count){
				block_request_ptr block = std::make_shared<block_request>();
				block->start = start;
				/// to retrieve a page of blocks
				/// to retrieve a range of blocks

				std::shared_ptr<Poco::Data::Statement> block_stmt;
				block_stmt = std::make_shared<Poco::Data::Statement>( get_session() );
				//u64 ts = os::millis();
				*block_stmt << "SELECT a1, dsize, data FROM " << (*this).table_name << " WHERE a1 >= ? ;",
					into(block->addresses), into(block->sizes), into(block->blocks), bind(block->start), Poco::Data::Limit(count,false,false);
				block_stmt->execute();
				if(block->addresses.empty()){
					return;
				}
				blocks += block->addresses.size();
				
				block->decode_all();
				for(size_t a = 0; a < block->decoded.size(); ++a){
					if(!inject(block->addresses[a],block->decoded[a])){
						delete block->decoded[a];
					}
				}
				start = block->addresses.back();
				if(os::millis() - ts > 1000){
					inf_print("read %s %lld\n ", get_name().c_str(), (long long)start);
					ts = os::millis();
				}
			//}
			inf_print("readahead %s complete %.4g s", get_name().c_str(),(double)(os::millis()-tst)/1000.0);
		}
		
		/// move all data to dest wether it exists or not
		void move(sqlite_allocator& dest){

			_Allocations todo;
			get_addresses(todo);
			nst::u64 unloaded = 0;
			if(dest.get_name() != get_name()){
				//printf("[TX-MOvE] invalid name\n");
			}
			//printf("[TX MOVE] %s ver. %lld -> %lld [", dest.get_name().c_str(), (long long)get_version(), dest.get_version());
			for(_Allocations::iterator a = todo.begin(); a != todo.end(); ++a){
				stream_address at = a.get_key();
				ref_block_descriptor current = a.get_value();
				//printf("%lld, ", (long long)at);
				
				if(current!=nullptr){
					buffer_type &r = current->block;
					buffer_type *d = &(dest.allocate(at, create));
					version_type allocated_version = current->version;
					
					if(allocated_version <= dest.get_allocated_version()){
						throw InvalidVersion();
					}
					d->swap(r);	
					dest.assign_version(allocated_version);
					dest.complete();
					versions.erase(at);
					recycle_block(at,current);
				}else{
					++unloaded; 
					buffer_type &r = allocate(at, read);
					buffer_type *d = &(dest.allocate(at, create));
			
					if((*this).get_allocated_version() <= dest.get_allocated_version()){
						throw InvalidVersion();
					}
					d->swap(r);				
					dest.assign_version((*this).get_allocated_version());
					complete();
					dest.complete();
					versions.erase(at);
					if(find_block(at, current)){
						recycle_block(at,current);
					}
				}
				
			}			
			if(!allocations.empty()){
				err_print("not all addresses transferred");
			}
			changes = 0;
			versions.clear();
			(*this).set_transient();
			(*this).rollback();
			
			//printf("]\n");
		}

		void purge(){
			_Addresses todo;
			get_addresses(todo);
			for(_Addresses::iterator a = todo.begin(); a != todo.end(); ++a){
				stream_address at = (*a);
				/// get rid of the data
				if(allocations.count(at)){
					down_use( get_block_use(allocations[at]) );
					delete allocations[at];					
					allocations.erase(at);
					
				}
			}
			if(!allocations.empty()){
				printf("[TS] [WARNING] not all addresses transferred\n");
			}
			//changed.clear();
			changes = 0;
			versions.clear();
		}
		
		/// copy all data to dest wether it exists or not
		void copy(sqlite_allocator& dest){

			_Addresses todo;
			get_addresses(todo);
			if(dest.get_name() != get_name()){
				//printf("[TX-COPY] invalid name\n");
			}
			//printf("[TX COPY] %s ver. %lld -> %lld [", dest.get_name().c_str(), (long long)get_version(), dest.get_version());
			for(_Addresses::iterator a = todo.begin(); a != todo.end(); ++a){
				stream_address at = (*a);
				//printf("%lld, ", (long long)at);
				buffer_type &r = allocate(at, read);

				buffer_type *d = &(dest.allocate(at, create));
				//if(dest.is_end(*d)){
				//	dest.complete();
				//	d = &(dest.allocate(at, create));
				//}
				if((*this).get_allocated_version() <= dest.get_allocated_version()){
					throw InvalidVersion();
				}
				*d = r;

				dest.assign_version((*this).get_allocated_version());
				complete();
				dest.complete();

			}
			//printf("]\n");
		}

		/// The limit that the use variable may not permanently exceed

		void set_limit(ptrdiff_t limit){
			(*this).limit = limit;
		}

		/// construct a sqlite database with name specifying the table in which blocks will be stored
		/// in this case the name default is used
		/// TODO: the get_storage_path function provides a configurable path to the database file itself

		template<typename _NameFactory >
		sqlite_allocator
		(	_NameFactory namer			/// storage name
		,	bool is_new = false			/// basically a temporary transaction3
		,	address_type ma = 32		/// minimum address
		)
		:	transient(false)			/// the path should contain the trailing delimeter
		,	busy(false)					///	set to true during an allocation to false when complete is called
		,	next(ma)					/// logical address space iterative generator
		,   currently_active(0)         /// logical address after allocate and before complete is called
		,	table_name("blocks")		/// table name - nothing special
        ,   name(namer.get_name())		/// storage name can be a path should contain only numbers and letters no seperators commans etc
		,	extension(TREESTORE_FILE_EXTENSION) /// extension used by treestore
        ,	changes(0)					/// changes made since begin
        ,	version(0)					/// starts the version at 0
        ,	allocated_version(0)		/// the version after allocate
		,	transacted(false)			/// when true begin where called at least once since startup, commit or rollback
		,	clock(0)					/// initial clock value gets incremented with every access
		,	limit(128 * 1024 * 1024)	/// default memory limit
		,	_use(0)						/// current memory use in bytes
		,	decoders_away(0)			/// worker units busy decoding
		,	is_readahead(false)			/// disables read ahead
		,	is_all_loaded(false)		/// set when all was loaded to prevent repeat loads
		,	is_read_cache(true)			/// the read cache defaults to active
		,	result(nullptr)				///	save the last result after allocate is called to improve safety
		,	file_size(0)				/// no file size initialy
		,	timer(get_lr_timer())
					/// if true the data files are deleted on destruction

		{
		
			using Poco::File;
			
			if(!is_new){
				pool_string npath = get_storage_path() + name + extension;
				File df ( npath.c_str() );
				(*this).is_new = !df.exists();
				if(!(*this).is_new){
					get_session();
					
				}
			}else (*this).is_new = true;
			
		}
		virtual pool_string get_name() const {
			return (*this).name;
		}
		void set_readahead(bool is_readahead){
			this->is_readahead = is_readahead;
		}
		bool get_readahead() const {
			return this->is_readahead;
		}
		/// change the allocation offset only if the new one is larger
		void set_allocation_start(address_type start){
			next = std::max<address_type>(start,next);
		}

		~sqlite_allocator(){
			//printf("[TX DELETE] %s ver. %lld \n", get_name().c_str(), (long long)get_version());
			discard();
			if(transient && !is_new){
				using Poco::File;
				using Poco::Path;
				try{
					pool_string nname = get_name() + extension;
					File df (nname.c_str());
					if(df.exists()){
						df.remove();
					}
				}catch(std::exception& ){
					/// TODO: needs to be logged as warning and/or handled when transient resource starts up
				}
			}
		}



		/// returns the block defining the end - callers of allocate should check if this condition is reached

		const block_type & end() const {
			return empty_block;
		}

	private:
		/// throws an exception when an assignment is attempted

		sqlite_allocator& operator=(const sqlite_allocator&){
			throw std::exception();
			return *this;
		}

		/// this is held to assist in detecting changes

		ref_block_descriptor result ;

	public:

		/// touch 
		void touch(){
			this->timer = get_lr_timer();
		}
		/// return the time since this object was used
		u64 get_age() const {
			return get_lr_timer() - this->timer;
		}
		/// transient: true;

		void set_transient(){
			(*this).transient = true;
		}

		/// transient: false;

		void set_permanent(){
			(*this).transient = false;
		}

		/// is transient == true

		bool is_transient() const {
			return (*this).transient;
		}

		/// discard all data and internal state if references are 0

		void discard(){

			syncronized ul(lock);
			if(references == 0){
				if((*this).modified() && !transient){
					(*this).begin_new();
					(*this).commit();
				}
				if(_session!=nullptr){
					if (!transient){
						printf(" discarding storage %s\n",name.c_str());
						rollback();
					}else
						printf(" discarding transient %s\n",name.c_str());
					insert_stmt = nullptr;
					get_stmt = nullptr;
					exists_stmt = nullptr;
					_session = nullptr;
					pool_string npath = get_storage_path() + name + extension;
					if(transient){
						using Poco::File;
						File df ( npath.c_str() );					
						if(df.exists()){
							df.remove(false);
						}
						(*this).is_new = true;
					}
				}
				for(typename _Allocations::iterator a = allocations.begin(); a!=allocations.end(); ++a){
					up_use( reflect_block_use((*a).second) );
					down_use( get_block_use((*a).second) );
					delete (*a).second;
				}
				clear_recycler();
				allocations.clear();
				versions.clear();
				if(lego.is_open()) 
					lego.close();
			}
		}

		/// initialize a specific address to exist if it didn't before otherwise return

		void initialize(const address_type& which){
			syncronized ul(lock);
			if(!(*this).get_buffer(which)){
				ref_block_descriptor result = new block_descriptor(version_type());
				result->set_storage_action(create);
				allocations[which] = result;
				//changed.insert(which);
				++changes;
			}
		}
		/// return the storage size of this allocator
		nst::u64 get_storage_size() const {
			return this->file_size;
		}
		/// return the last allocated address

		address_type last() const{
			syncronized ul(lock);

			return (*this).next;
		}

		/// returns true if this block is the end of storage condition

		bool is_end(const block_type& b) const {
			return (&b == &(*this).end());
		}

		/// allocate(1)
		/// if 'how' is not 'create' the address must exist prior to calling this function
		/// else end is returned (is_end(result)==true).
		/// if 'how' is create the storage address can be 0 or more if it does not exist then
		/// the storage pair is created
		/// returns the end() if the non nil address requested does not exist
		block_type & allocate(address_type& which, storage_action how){

			block_type & allocated = _allocate(which, how);

			return allocated;
		}
		/// contains
		/// returns true if an address exists
		bool contains(const address_type& which){
			NS_STORAGE::synchronized s(lock);//lock.lock();

			check_use();
			if(which){
				if(allocations.count(which) == 0){
					if((*this).get_buffer(which))
						return true;
				}else return true;
			}
			return false;
		}

		/// allocate(2)
		/// allocate and copy to the destination parameter cpy. see allocate (1) for further parameters
		block_type & allocate(block_type & cpy, address_type& which, storage_action how){
			block_type & allocated = _allocate(which, how);
			cpy = allocated;
			complete();
			return cpy;
		}

		/// get_allocated_version
		/// returns the allocated version of the block provided
		version_type get_allocated_version() const {
			return allocated_version;
		}

		/// complete the current allocation - for cleanup unlock etc- because we returned a dangling ptr
		/// this function is idempotent when called concurrently and only the calling thread of allocate
		/// can effectively change it
		/// (it changes busy to false only once unless allocate is called)
		void complete(){
			///NS_STORAGE::synchronized s(lock);

			allocated_version = version_type();
			if(busy){
				if(nullptr != result){
					up_use(reflect_block_use(result)); /// update changes made to last allocation
				}

				busy = false;
				lock.unlock();
			}

		}
	public:
		/// start a transaction valid during the lifetime of this storage

		void begin(){
			syncronized ul(lock);

			if(!is_new){ /// this flag is used to supress file creation
				_begin();
			}
		}
		/// a special case of begin only called by mvcc_coordinator
		/// this version ignores the is_new flag and starts a transaction
		/// anyway
		void begin_new(){
			syncronized ul(lock);
			_begin();
			if(!is_new){}
		}
		void open(){
			syncronized ul(lock);
			
		}
		/// NL
		bool is_local() const {
			return !lego.is_open();
		}
		/// close all handles and opened files and release memory held in caches, unwritten pages are not flushed
		void close(){
			discard();
		}
		/// engages an instance reference
		void engage(){
			syncronized ul(lock);

			++references;
		}
		void release(){ //s an instance reference
			syncronized ul(lock);
			if(references>0)
				--references;
			//printf("references for %s reached %ld\n", name.c_str(), (long)references);
			discard();
		}
		bool modified() const {
			return changes > 0;
		}
	private:
		void _begin(){
			if(!transacted){
				get_session() << "PRAGMA shrink_memory;", now;
				get_session() << "begin transaction;", now;
				if(lego.is_open()) lego.begin(true);
				transacted = true;
			}
		}
		/// helper function to finalize commit to storage after all
		/// modified pages have been flushed

		void commit_storage(){
			if(transacted){
				get_session() << "commit;", now;
				if(lego.is_open()) lego.commit();
			}
			
			transacted = false;
		}

	public:

		/// commit a transaction valid during the lifetime of this storage

		void commit(){
			syncronized ul(lock);
			touch();
			if(transacted){
				flush_back(0.8, false, false); /// write all changes to disk or pigeons etc.
			}
			commit_storage();

		}

		virtual void journal_commit() {
		}



		void journal(const pool_string& name){
			_Addresses todo;
			syncronized ul(lock);
			get_modified_addresses(todo);
			for(_Addresses::iterator a = todo.begin(); a != todo.end(); ++a){
				stream_address at = (*a);
				buffer_type &r = allocate(at, read);
				if(is_end(r)){
					throw InvalidAddressException();
				}
				journal::get_instance().add_entry(JOURNAL_PAGE, name, at, r);
				complete();
			}

		}
		void flush(){
			if(transient) return;
			//if(version > 3) return;
			syncronized ul(lock);
			flush_back(0.85, false); /// write all changes to disk or pigeons etc.
			get_session() << "PRAGMA shrink_memory;", now;
		}
		/// reverse any changes made by the current transaction

		void rollback(){
			syncronized ul(lock);
			if(has_session()){
				if(transacted){
					get_session() << "rollback;", now;
					if(lego.is_open()) lego.rollback();
				}
				get_session() << "PRAGMA shrink_memory;", now;
				insert_stmt = nullptr;
				get_stmt = nullptr;
				exists_stmt = nullptr;
				_session = nullptr;
				pool_string npath = get_storage_path() + name + extension;
				if(transient){
					using Poco::File;
					File df ( npath.c_str() );					
					if(df.exists()){
						df.remove(false);
					}
					(*this).is_new = true;
				}
			}
			for(typename _Allocations::iterator a = allocations.begin(); a!=allocations.end(); ++a){
				up_use( reflect_block_use((*a).second) );
				down_use( get_block_use((*a).second) );
				delete (*a).second;
			}			
			if(!allocations.empty()) 
				allocations.clear();
			if(!versions.empty()) 
				versions.clear();
			changes = 0;
			transacted = false;
			
		}
		void reduce(){
			if(transient) return;
			if(version != version_type()) return;
			syncronized ul(lock);
			//if(modified()) return;
			
			//printf("reducing%sstorage %s\n",modified() ? " modified " : " ", get_name().c_str());
			if((*this)._use > 1024*1024*1)
				flush_back(0.45,true);
			get_session() << "PRAGMA shrink_memory;", now;
		}
		void reduce_perm(){
			if(transient) return;
			syncronized ul(lock);
			//printf("reducing%sstorage %s\n",modified() ? " modified " : " ", get_name().c_str());
			if((*this)._use > 1024*1024*1)
				flush_back(0.45,true);
			get_session() << "PRAGMA shrink_memory;", now;
		}
	};

	typedef std::shared_ptr<Poco::Mutex> mutex_ptr;
	typedef Poco::Mutex * mutex_ref;

	/// this defines a storage which is based on a previous version
	/// if the based member is nullptr then the version based storage
	/// puts it in its own member storage

	/// this interface supports multi threaded access on public functions
	/// except for contructors and destructors implying the users of instances
	/// of this class must release dependencies before its destruction

	/// example base storage typedef sqlite_allocator<_AddressType, _BlockType>

	template <typename _BaseStorage >
	class version_based_allocator {
	public:

		typedef _BaseStorage storage_allocator_type;

		typedef std::shared_ptr<storage_allocator_type> storage_allocator_type_ptr;

		typedef std::shared_ptr<version_based_allocator> version_based_allocator_ptr;

		typedef version_based_allocator* version_based_allocator_ref;

		typedef typename _BaseStorage::block_type block_type;

		typedef typename _BaseStorage::address_type address_type;

	private:

		/// the base storage provides the actual block storage

		storage_allocator_type_ptr allocations;	/// the storage for this version
		version_based_allocator_ptr based;		/// the version on which this storage is base
		version_based_allocator_ref last_base;
		version_type version;								/// the version number
		version_type allocated_version;
		u64 readers;
		u64 order;
		address_type initial;
		block_type empty_block;
		bool read_only;
		bool copy_reads;
		bool discard_on_end;
		bool merged;
		mutex_ref lock;
	private:
	public:
		/// returns the allocator or throws an exception if it isnt set

		storage_allocator_type& get_allocator(){
			if((*this).allocations != nullptr){
				return *(*this).allocations;
			}
			/// TODO: throws an exception
			throw std::exception();
		}
		storage_allocator_type& get_allocator() const {
			if((*this).allocations != nullptr){
				return *(*this).allocations;
			}
			/// TODO: throws an exception
			throw std::exception();
		}

		/// return a list of versions for the request of pairs
		template<typename _VersionRequests>
		nst::u64  get_greater_version_diff(_VersionRequests& request){
			nst::u64 responses = 0;
			if((*this).allocations != nullptr){
				responses += (*this).allocations->get_greater_version_diff(request);
			}
			if(based != nullptr){
				responses += (*this).based->get_greater_version_diff(request);
			}
			return responses;
		}
		void set_order(u64 order){
			this->order = order;
		}
		void set_initial(address_type initial){
			this->initial = initial;
		}
		void set_transient(){
			get_allocator().set_transient();
		}
		void set_permanent(){
			get_allocator().set_permanent();
		}
		bool is_transient() const {
			return get_allocator().is_transient();
		}
		void set_merged(){
			merged = true;
		}
		bool is_merged() const {
			return merged;
		}
		void clear_merged(){
			merged = false;
		}
		/// copy the allocations from this
		void copy(version_based_allocator_ref dest){
			get_allocator().copy(dest->get_allocator());
		}
		/// move the allocations from this
		void move(version_based_allocator_ref dest){
			get_allocator().move(dest->get_allocator());
		}
		/// remove version and cached data
		void purge(){
			if((*this).allocations != nullptr){
				get_allocator().purge();
			}
		}
		void set_discard_on_finalize(){
			discard_on_end = true;
		}
		void set_readonly(){
			read_only = true;
		}

		void unset_readonly(){
			read_only = false;
		}

		/// gets read only state
		bool is_readonly() const {
			return read_only;
		}

		/// sets the previous version of this version
		void set_previous(version_based_allocator_ptr based){
			syncronized _sync(*lock);
			(*this).based = based;
		}

		/// return the previous version of this version if there is one
		version_based_allocator_ptr get_previous(){
			syncronized _sync(*lock);
			return (*this).based;
		}

		/// sets the storage allocator used for the storing version
		/// specific blocks
		void set_allocator(storage_allocator_type_ptr allocator){

			(*this).allocations = allocator;
			if((*this).allocations != nullptr){
				(*this).get_allocator().set_version((*this).get_version());
			}

		}

		/// return the version of the storage
		version_type get_version() const {
			return (*this).version;
		}
		version_type get_allocated_version() const {
			return (*this).allocated_version;
		}
		/// sets the version of this storage
		void set_version(version_type version){
			(*this).version = version;
			if((*this).allocations != nullptr){
				(*this).get_allocator().set_version((*this).get_version());
			}
		}

		/// notify the addition of a new reader
		void add_reader(){
			syncronized _sync(*lock);
			(*this).readers++;
		}

		/// notify the release of an existing reader
		void remove_reader(){
			syncronized _sync(*lock);
			if((*this).readers == 0){
				throw InvalidReaderCount();
			}
			(*this).readers--;
		}

		/// how many dependencies are there
		u64 get_readers() const {

			return (*this).readers;
		}

		/// returns true if there are no dependants
		bool is_unused() const {

			return ((*this).readers == 0ull);
		}

		/// retruns true if there are dependants
		bool is_used() const {

			return ((*this).readers != 0ull);
		}

		/// return the order of the version
		u64 get_order() const {
			return order;
		}


		version_based_allocator(address_type initial, u64 order, version_type version, mutex_ptr lock)
		:	allocations(nullptr)
		,	based(nullptr)
		,	last_base(nullptr)
		,	version(version)
		,	allocated_version(0)
		,	readers(0)
		,	order(order)
		,	initial(initial)
		,	read_only(false)
		,	copy_reads(false)
		,	discard_on_end(false)
		,	merged(false)
		,	lock(lock.get())
		{

		}
#if _MAYBE_LATER_
		version_based_allocator
		(	mutex_ptr lock
		,	storage_allocator_type_ptr allocator
		,	version_based_allocator_ptr based
		)
		:	allocations(allocator)
		,	based(based)
		,	lock(lock)
		,	read_only(false)
		,	copy_reads(false)
		{

		}
#endif
		~version_based_allocator(){
			if(discard_on_end && allocations != nullptr){
				allocations->discard();
			}

		}

		/// determine if the input is actually one of the base version end states

		bool is_end(const block_type &r) const {
			syncronized _sync(*lock);
			const version_based_allocator * b  =this;
			while(b!=nullptr){
				if(b->get_allocator().is_end(r)){
					return true;
				}
				b = b->based.get();
			}
			return false;
		}
		void check_global_use(){
			//if(treestore_current_mem_use > treestore_max_mem_use*0.85){
			if(buffer_allocation_pool.is_near_depleted()){
				//stored::reduce_all();
			}
		}

		public:

		/// 'interceptor' function allocates or retrieves the latest version of a resource
		/// created before or in the current version.
		/// if [how] is not 'create' the address must exist prior to calling this function
		/// else allocate a new address and put it in [which] using the storage action [how].
		/// if [how] is 'create' the storage address must be 0 or a InvalidStorageAction
		/// Exception is thrown
		/// returns the end() if the non nil address requested does not exist
		/// if [how] is read and readonly is on the an InvalidStorageAction is thrown

		/// TODODONE: set previously committed versions to read only
		block_type & allocate(address_type& which, storage_action how){			
			last_base = nullptr;
			(*lock).lock();
			(*this).allocated_version = version_type();

			if(is_readonly() && how != read)
				throw InvalidStorageAction();

			if(!which && how == create){
				block_type & r = get_allocator().allocate(which,how);/// a new one or exception
				allocated_version = get_allocator().get_allocated_version();
				return r;
			}
			if((*this).allocations != nullptr){
				if(how != create){
					/// at this point the action can only be one of read and write
					/// given that the block exists as a local version
					/// if it was read previously it will change to a write
					/// when action is copy_reads and it doesnt exist in the local then
					/// it will be copied into the local storage
 					block_type& r= get_allocator().allocate(which, how);
					if( !get_allocator().is_end(r) ){

						allocated_version = get_allocator().get_allocated_version();
						return r;// it may be a previously written or read block
					}
					get_allocator().complete();
				}else if(how == create){
					/// the element wil not exist in any of the previous versions
					/// and is created right here
					block_type& r = get_allocator().allocate(which, how);
					allocated_version = get_allocator().get_allocated_version();

					return r;
				}
			}
			if(how==create)
				throw InvalidStorageAction();

			last_base  = this->based.get();
			while(last_base != nullptr){
				block_type& r = last_base->get_allocator().allocate(which, read); //can only read from previous versions
				if(!(last_base->get_allocator().is_end(r))){
					if
					(	how == write
					&&	(*this).allocations != nullptr
					)
					{

						block_type& u = get_allocator().allocate(which, create);	/// it might exist as a read-only block in the current version
						allocated_version = get_allocator().get_allocated_version();
						u = r;// copy the latest version into the transaction
						last_base->get_allocator().complete();
						last_base = nullptr;
						return u;
					}
					allocated_version = last_base->get_allocator().get_allocated_version();
					last_base->get_allocator().complete();
					last_base = nullptr;
					return r;
				}
				last_base->get_allocator().complete();
				last_base = last_base->based.get();
			}


			/// it was never found in the base or local versions
			return empty_block;

		}
		void complete(){
			//syncronized _sync(*lock);
			get_allocator().complete();
			if(last_base != nullptr){
				last_base->get_allocator().complete();
				last_base = nullptr;
			}
			(*lock).unlock();
		}
		void begin(){
			syncronized _sync(*lock);
			if(allocations!=nullptr)
				allocations->begin();
		}
		/// special case only called by mvcc_coordinator
		void begin_new(){
			syncronized _sync(*lock);
			if(allocations!=nullptr)
				allocations->begin_new();
		}

		void commit(){

			syncronized _sync(*lock);
			if(allocations!=nullptr)
				allocations->commit();
		}
		void flush(){

			syncronized _sync(*lock);
			if(allocations!=nullptr)
				allocations->flush();
		}

		/// write the allocated data to the global journal
		void journal(const pool_string &name){
			syncronized _sync(*lock);
			if(allocations!=nullptr)
				allocations->journal(name);
		}
		/// return the last address allocated during the lifetime of this version
		address_type last() const {
			syncronized _sync(*lock);
			return allocations->last();
		}

		bool modified() const {
			syncronized _sync(*lock);
			if(allocations!=nullptr)
				return allocations->modified();
			return false;
		}

	};
	/// list of file names actioned or to action
	typedef std::vector<std::string> _FileNames;
	/// remove all related tempfiles for a storage
	static _FileNames delete_temp_files_of(const pool_string& _pool_name){
		using Poco::StringTokenizer;
		using Poco::Path;
		using Poco::DirectoryIterator;
		std::string name = _pool_name.c_str();
		StringTokenizer components(name, "\\/");
		std::string path;
		std::string numbers = "0123456789";
		size_t e = (components.count()-1);

		for(size_t s = 0; s < e; ++s){
			path += components[s] ;
			if (s < e-1)
				path += Path::separator();
		}

		_FileNames files;
		std::string name_path = path + Path::separator();
		for(DirectoryIterator d(path); d != DirectoryIterator();++d){
			try{
				if((*d).isFile()){
					std::string full_name = name_path + d.name();
					if(full_name.substr(0,name.size())==name
						&& full_name.size() > name.size() + 1
						&& full_name[name.size()] == '.'
						&& numbers.find_first_of(full_name[name.size()+1]) != pool_string::npos
						){
						printf("found temp file %s\n",full_name.c_str());

						try{
							Poco::File df (full_name);
							if(df.exists()){
								df.remove();
							}
							files.push_back(full_name);
						}catch(...){
						}
					}
				}
			}catch( Poco::PathNotFoundException& ){
				/// can happen when alter table operation takes place
			}catch( Poco::FileNotFoundException& ){
				/// can happen when alter table operation takes place
			}
		}
		return files;
	}
	/// coordinates mvcc enabled storages - the mvcc ness of a storage is defined by its
	///
	/// set+get_version, add_reader, remove_reader and get_readers, set+get_order  members
	///
	/// usually only one instance per name per process
	/// example base storage typedef sqlite_allocator<_AddressType, _BlockType>
	///
	/// limitations
	///
	/// currently this storage will only support a single writer
	/// 2 or more concurrent writers will queue or an exception
	/// will be thrown. queuing of writers requires that a writing
	///	transaction registers its intention
	///
	template<typename _BaseStorage>
	class mvcc_coordinator : public journal_participant{
	public:

		typedef _BaseStorage storage_allocator_type;

		typedef std::shared_ptr<storage_allocator_type> storage_allocator_type_ptr;

		typedef version_based_allocator<storage_allocator_type> version_storage_type;

		typedef std::shared_ptr<version_storage_type> version_storage_type_ptr;

		typedef std::vector<version_storage_type_ptr> storage_container;

		typedef ns_umap::unordered_map<version_type, version_storage_type_ptr,rabbit::rabbit_hash<version_type>, std::equal_to<version_type>, sta::buffer_pool_alloc_tracker<version_type> > version_storage_map;

		typedef typename storage_allocator_type::address_type address_type;
		
	private:



		struct version_namer{
			pool_string name;
			pool_string to_string(const Poco::UUID& version){
				return version.toString().c_str();
			}
			pool_string to_string(const u32& version){
				std::string result;
				Poco::NumberFormatter::append(result, version);
				return result.c_str();
			}
			version_namer(const version_type &version, const pool_string extension) {

				name += extension;
				name += ".";
				name += to_string(version);				
			}

			version_namer (const default_name_factory& nf){
				*this = nf;
			}
			~version_namer(){
			}
			version_namer & operator=(const version_namer& nf){
				(*this).name = nf.name;
				return *this;
			}
			const pool_string & get_name() const {
				return (*this).name;
			}

		};

		Poco::AtomicCounter references;		/// counts references to this instance through release and engage methods

		u64 order;							/// transaction order set on commit

		u32 active_transactions;			/// transactions currently underway

		u32 writing_transactions;			/// writing transactions not committed or rolled back

		u32 writing_transaction_thread;		/// thread which started the writing transaction

		version_type next_version;			/// next version generator

		address_type last_address;			/// maximum address allocated

		storage_allocator_type_ptr initial;	/// initial storage

		storage_container storages;			/// list of versions already committed contains a min of 1 storages aftger construction

		storage_container recycler;			/// recycler for used transactions

		version_storage_map storage_versions;
											/// versions of each resource already committed
											/// used to check validity and re-aquire smart pointers

		mutex_ptr lock;						/// lock for access to member data from different threads

		bool recovery;						/// flags recovery mode so that journal isnt used

		bool journal_synching;				/// the journal is synching and transaction state should be kept

		u64 timer;							/// the timer value for age calculation

		version_storage_type* writing_transaction;
											/// the single writing transaction
	private:
		void save_recovery_info(){
			if(storages.size() > 1){
				pool_string names;
				typename storage_container::iterator c = storages.begin();
				typename storage_container::iterator c_begin = ++c;
				for(; c != storages.end(); ++c)
				{
					if( c != c_begin )
						names += ",";
					pool_string n = (*c)->get_allocator().get_name();
					names += n;
				}


				stream_address addr = INTITIAL_ADDRESS;
				buffer_type& content = initial->allocate(addr, create);
				content.resize(names.size());
				if(names.size() > 0)
					memcpy(&content[0], &names[0], names.size());
				initial->complete();
			}
		}

	public:

		/// merge idle transactions from latest to oldest
		/// merge unused transaction versions - releasing any held resources
		/// the merge should produce only one table on completion in idle state
		/// i.e. storages.size()==1
		void merge_down()
		{
			syncronized _sync(*lock);

			/// changing the transactional state during a journal synch cannot be recovered
			if(journal_synching) return;

			if(!storages.empty())
			{
				storage_container merged;
				typename storage_container::reverse_iterator latest_idle = storages.rbegin();
				storage_container idle;
				version_type version = version_type();
				for(typename storage_container::iterator c = storages.begin(); c != storages.end(); ++c)
				{
					if((*c)->get_version() <= version){						
						throw InvalidWriterOrder();
					}
					(*c)->clear_merged();
					version = (*c)->get_version();
				}

				for(;;)
				{
					bool Ok  = latest_idle != storages.rend();
					if(Ok && (*latest_idle)->is_unused())
					{

						idle.push_back((*latest_idle));		/// collect idle transactions


					}else if(!idle.empty())
					{
						version_storage_type_ptr latest;
						typename storage_container::iterator i = idle.begin();
						latest = (*i);
						++i;								/// at least 2 consecutive idle versions
															/// are needed to actually merge their
															/// resources
						for(; i != idle.end(); ++i)
						{
							if(latest->get_version() <= (*i)->get_version())
								throw InvalidWriterOrder();
							if(!(*i)->is_unused()){
								throw InvalidVersion();
							}

							//latest->copy((*i).get());		/// latest -> i
							latest->move((*i).get());		/// latest => i
							latest->set_merged();	/// flagged as copied or merged
							latest = (*i);

						}

						latest->clear_merged();				/// its not merged with something lower down

						idle.clear();						/// cleanup references

					}else idle.clear();
					if(!Ok){
						break;
					}
					++latest_idle;
				}

				for(typename storage_container::iterator c = storages.begin(); c != storages.end(); ++c)
				{
					if((*c)->is_merged())
					{
						if((*c)->is_unused()){
							(*c)->set_transient();				/// let any resources be cleaned up

							storage_versions.erase((*c)->get_version());
							(*c)->set_previous(nullptr);
							//delete (*c);
						}else{
							throw InvalidVersion();
						}

					}else{
						if(recovery){

							(*c)->begin_new();
							(*c)->commit();

						}

						merged.push_back((*c));
					}
				}
				storages.swap(merged);

				version_storage_map verify;
				version_storage_type_ptr prev = nullptr;

				for(typename storage_container::iterator c = storages.begin(); c != storages.end(); ++c){
					if(verify.count((*c)->get_version()) != 0){
						throw InvalidVersion();
					}
					(*c)->set_previous(prev);

					if(prev && (*c)->get_version() <= prev->get_version())
						throw InvalidWriterOrder();
					verify[(*c)->get_version()] = (*c);
					prev = (*c);
				}

				if(storages.empty()){
					throw InvalidVersion();
				};
				save_recovery_info();
				/// algorithm error
				//if((*storages.begin()) != initial) {}; ///algorithm error

			}
		}

		storage_allocator_type_ptr get_initial(){
			return initial;
		}
		static const stream_address INTITIAL_ADDRESS = 16;
		mvcc_coordinator
		(	storage_allocator_type_ptr initial			/// initial version - to locate storage
		)
		:   order(1)
		,	active_transactions(0)
		,	writing_transactions(0)
		,	writing_transaction_thread(0)
		//,	next_version(1)
		,	last_address ( initial->last() )
		,	initial(initial)
		,   lock(std::make_shared<Poco::Mutex>())
		,	recovery(false)
		,	journal_synching(false)
		,	timer(get_lr_timer())
		,	writing_transaction(nullptr)
		{
			//initial->engage();
			delete_temp_files_of(initial->get_name());
			version_storage_type_ptr b = std::make_shared< version_storage_type>(last_address, order, create_version(), (*this).lock);
			b->set_allocator(initial);
			b->set_previous(nullptr);
			storages.push_back(b);
			pool_string names;
			stream_address addr = INTITIAL_ADDRESS;
			buffer_type& content = initial->allocate(addr, read);
			if(!initial->is_end(content) && content.size() > 0){
				names.resize(content.size());
				memcpy(&names[0], &content[0], names.size());
				printf("found initial versions %s\n",names.c_str());
			}

			initial->complete();
			if(!names.empty()){
				bool delete_temp_files = true;
				if(delete_temp_files){

				}else {
					Poco::StringTokenizer tokens(names.c_str(), ",");
					for(Poco::StringTokenizer::Iterator t = tokens.begin(); t != tokens.end(); ++t){
						storage_allocator_type_ptr allocator = std::make_shared<storage_allocator_type>(default_name_factory((*t).c_str()));
						last_address = std::max<stream_address>(last_address, allocator->last() );
						version_storage_type_ptr b = std::make_shared< version_storage_type>(last_address, ++order, create_version(), (*this).lock);
						allocator->set_allocation_start(last_address);
						b->set_allocator(allocator);
						storages.push_back(b);
						storage_versions[b->get_version()] = b;
					}
					merge_down();
					if(storages.size() > 1){
						throw InvalidStorageException();
					}
				}

			}
			initial->begin_new();
			initial->allocate(addr, create).clear();
			initial->complete();
			initial->commit();
			journal::get_instance().engage_participant(this);
		}

		virtual ~mvcc_coordinator(){
			if(references != 0)
				err_print("non zero references on destruction");

			journal::get_instance().release_participant(this);

			initial->release();
		}
		/// set the timer value to current
		void touch(){
			this->initial->touch();
		}
		/// the time since last operation ocurred
		u64 get_age() const {
			return this->initial->get_age(); 
		}
		/// return the storage size of the initial storage
		nst::u64 get_storage_size() const {			
			return this->initial->get_storage_size();			
		}

		/// set read cache to desired state
		void set_read_cache(bool is_read_cache){
			syncronized _sync(*lock);
			initial->set_read_cache(is_read_cache);
		}

		/// load all loads the complete storage into memory
		void load_all(){
			syncronized _sync(*lock);
			initial->load_all();
		}

		/// true if the storage is local
		bool is_local() const {
			return initial->is_local();
		}

		/// engages the instance - its resources may not be released if references > 0

		void engage(){
			syncronized _sync(*lock);
			if(references==0)
				initial->engage();
			++references;
		}

		/// releases a reference to this coordinatron

		void release(){
			syncronized _sync(*lock);
			--references;
			/// TODO: if references == 0 the handles held to resources
			///	need to let go so that maintenance can happen (i.e. drop table)
			if(references==0){

				/// merge_down();
				if(storages.size()==1){
					initial->release();
				}/// else TODO: its an error since the merge should produce only one table on completion in idle state
			}
		}

		/// check if idle for maintenance

		bool is_idle(){
			syncronized _sync(*lock);
			return ( 0 == references && 0 == active_transactions );
		}

		/// reduces use

		void reduce(){
			syncronized _sync(*lock);

			for(typename storage_container::iterator c = storages.begin(); c != storages.end(); ++c)
			{
				(*c)->get_allocator().reduce_perm();

			}


		}

		public:
			/// return a list of greater versions for the request of pairs less or same versions are
			/// not returned
		/// template<typename _VersionRequests>
		nst::u64 get_greater_version_diff(_VersionRequests& request){
			nst::u64 responses = 0;
			syncronized _sync(*lock);
			for(typename storage_container::iterator c = storages.begin(); c != storages.end(); ++c)
			{
				responses += (*c)->get_greater_version_diff(request);
			}
			return responses;

		}
		/// start a new version with a dependency on the previously commited version or initial storage
		/// smart pointers are not thread safe so only use them internally

		version_storage_type* begin(bool writer){

			if(writer){
				//get_single_writer_lock().lock();
			}


			syncronized _sync(*lock);
			touch();
			/// TODODONE: reuse an existing unmerged transaction - possibly share unmerged transactions
			/// TODODONE: lazy create unmerged transactions only when a write occurs
			/// TODO: optimize mergeable transactions
			/// TODODONE: merge unused transactions into single transaction
			if(writer && writing_transactions){
				err_print("writing transaction already active");
				throw InvalidWriterOrder();
			}
			if(references==0){
				err_print("started transaction without active references");
			}
			version_storage_type_ptr b = nullptr;
			if(writer || recycler.empty()){
				b = std::make_shared< version_storage_type>(last_address, order, create_version(), (*this).lock);
				storage_allocator_type_ptr allocator = std::make_shared<storage_allocator_type>(version_namer(b->get_version(),initial->get_name()),true);
				allocator->set_allocation_start(last_address);
				b->set_allocator(allocator);
			}else {
				b = recycler.back();
				recycler.pop_back();
			}
			if(writer)
				b->unset_readonly();
			else
				b->set_readonly();

			storage_versions[b->get_version()] = b;
			if(!storages.empty()){
				b->set_previous(storages.back());
				for(typename storage_container::iterator c = storages.begin(); c != storages.end(); ++c)
				{
					(*c)->add_reader();
				}

				++active_transactions;
				if(writer){
					++writing_transactions;
					writing_transaction_thread = Poco::Thread::currentTid();
					writing_transaction = b.get();
				}
			}else{

				throw WriterConsistencyViolation();
			}

			return b.get();
		}
		/// get readahead
		bool is_readahead() const {
			return (*this).initial->get_readahead();
		}
		void set_readahead(bool is_readahead){
			(*this).initial->set_readahead(is_readahead);
		}

		/// return the transaction order of this coordinator

		u64 get_order() const {
			///syncronized _sync(*lock);
			return (*this).order;
		}

		u32 transactions_away() const {
			syncronized _sync(*lock);
			return active_transactions;
		}

		/// finish a version and commit it to storage
		/// the commit is coordinated with other mvcc storages
		/// via the journal
		/// the version may be merged with dependent previous versions
		/// returns true if the commit could take place
		bool commit(version_storage_type* transaction){
			//if(treestore_current_mem_use > treestore_max_mem_use){
			bool writer = !transaction->is_readonly();
			if(!writer){
				err_print("cannot commit: this is not a writing transaction");
				throw InvalidTransactionType();
			}
			{
				syncronized _sync(*lock);
				touch();
				if(active_transactions == 0){
					discard(transaction);
					err_print("cannot commit: no more active transactions");
					throw InvalidWriterOrder();

					// return false;
				}
				//printf("[COMMIT MVCC] [%s] %lld at v. %lld\n", transaction->get_allocator().get_name().c_str(), (long long)transaction->get_allocator().get_version());

				if(writer && transaction->modified() && transaction->get_order() < order){
					discard(transaction);
					err_print("invalid writer order");
					throw InvalidWriterOrder();


				}
				--active_transactions;
				if(writer)
					--writing_transactions;
				version_storage_type_ptr version = storage_versions.at(transaction->get_version());

				if(version == nullptr){
					throw InvalidVersion();
				}


				version->set_readonly();

				if (!recovery)		/// dont journal during recovery
					transaction->journal((*this).initial->get_name());


				last_address = std::max<address_type>(last_address, version->last());

				version_storage_type_ptr prev = version->get_previous();
				if(prev==nullptr)
					throw WriterConsistencyViolation();
				if(prev != storages.back()){
					throw ConcurrentWriterError();
				}
				while(prev != nullptr){
					prev->remove_reader();
					prev = prev->get_previous();
				}
				storages.push_back(version);

				++order;			/// increment committed transaction count

				merge_down();		/// merge unused transaction versions
									/// releasing any held resources
			}
			if(writer){
				//get_single_writer_lock().unlock();
			}
			return true;
		}
		/// finish a version and merge it to storage
		/// the merge is coordinated with other mvcc storages
		/// via the journal
		/// the version may be merged with dependent previous versions
		/// returns true if the merge could take place
		bool merge(version_storage_type* transaction){
			
			bool writer = !transaction->is_readonly();
			if(!writer){
				err_print("cannot commit: this is not a writing transaction\n");
				throw InvalidTransactionType();
			}
			if(transaction != writing_transaction){
				err_print("cannot commit: this is not the previously given writing transaction\n");
				throw InvalidWriterOrder();
			}
			if (!recovery)		/// dont journal during recovery
					transaction->journal((*this).initial->get_name());
			{
				syncronized _sync(*lock);
				touch();
				if(active_transactions == 0){
					discard(transaction);
					err_print("cannot commit: no more active transactions\n");
					throw InvalidWriterOrder();

					// return false;
				}
				//printf("[COMMIT MVCC] [%s] %lld at v. %lld\n", transaction->get_allocator().get_name().c_str(), (long long)transaction->get_allocator().get_version());

				if(writer && transaction->modified() && transaction->get_order() < order){
					discard(transaction);
					err_print("invalid writer order\n");
					throw InvalidWriterOrder();
				}
				
				version_storage_type_ptr version = storage_versions.at(transaction->get_version());

				if(version == nullptr){
					throw InvalidVersion();
				}


				last_address = std::max<address_type>(last_address, version->last());

				version_storage_type_ptr prev = version->get_previous();
				if(prev==nullptr)
					throw WriterConsistencyViolation();
				if(prev != storages.back()){
					throw ConcurrentWriterError();
				}
				if(storages.empty()){
					throw WriterConsistencyViolation();
				}
				version->move(storages.back().get());

				++order;			/// increment committed transaction count
				
				transaction->set_order(order);
				
				storage_versions.erase(version->get_version());

				transaction->set_version(create_version());

				storage_versions[version->get_version()] = version;
									/// it needs a higher version
				
			}
			if(writer){
				//get_single_writer_lock().unlock();
			}
			return true;
		}
		/// discard a new version similar to rollback

		void discard(version_storage_type* transaction){

			bool writer = !transaction->is_readonly();



			{
				synchronized _sync(*lock);
				if(active_transactions==0){
					err_print("cannot rollback: no active transactions away");
				}
				if(writer)
					--writing_transactions;
				--active_transactions;
				//printf("[DISCARD MVCC] [%s] at v. %lld\n", transaction->get_allocator().get_name().c_str(), (long long)transaction->get_allocator().get_version());

				if(transaction->get_previous() == nullptr)
				{
					throw InvalidVersion();
				}///  an error - there is probably no initial version

				transaction->set_readonly();
				transaction->set_discard_on_finalize();
				transaction->set_transient();
				version_storage_type_ptr version = storage_versions.at(transaction->get_version());

				if(version == nullptr){
					throw InvalidVersion();
				}
				if(version.get() != transaction){
					throw InvalidVersion();
				}
				version_storage_type_ptr prev = version->get_previous();

				while(prev != nullptr){
					prev->remove_reader();
					prev = prev->get_previous();
				}
				
				/// purge all dirty data
				//version->purge(); 
				if(!writer)
					recycler.push_back(version);
				storage_versions.erase(transaction->get_version());
			}

			if(writer){
				//get_single_writer_lock().unlock();
			}

		}

		void set_recovery(bool recovery){
			(*this).recovery = recovery;
		}

		/// journal participation functions
		/// the journal asks this participant to
		/// commit its storage
		virtual void journal_commit() {


			synchronized _sync(*lock);

			touch();
			for(typename storage_container::iterator c = storages.begin(); c != storages.end(); ++c)
			{
				(*c)->set_permanent();
				(*c)->begin_new();

				(*c)->commit();

			}

		}
		virtual bool make_singular()  {
			synchronized _sync(*lock);
			merge_down();

			return ((*this).storages.size() == 1);
		}
		virtual void journal_synch_start(){
			synchronized _sync(*lock);
			journal_synching = true;
		}
		virtual void journal_synch_end(){
			synchronized _sync(*lock);
			journal_synching = false;
			this->reduce();
		}
		virtual pool_string get_name() const {
			return this->initial->get_name();
		}
	};
/// complete namespaces
};/// storage
};///stx
#endif //_TRANSACTIONAL_STORAGE_H_
