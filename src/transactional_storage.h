/*****************************************************************************

Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
Copyright (c) 2008, 2009 Google Inc.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2012, Facebook Inc.
Copyright (c) 2013, Christiaan Pretorius

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

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
#include <stdlib.h>
#include <memory>
#include <string>
#include <map>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <stx/storage/types.h>
#include <stx/storage/basic_storage.h>

#include "Poco/NumberFormatter.h"
#include "Poco/String.h"
#include "Poco/Mutex.h"
#include "Poco/Data/Common.h"
#include "Poco/Data/Session.h"
#include "Poco/Data/BLOB.h"
#include "Poco/Data/SQLite/Connector.h"
#include "Poco/Exception.h"
#include "Poco/AtomicCounter.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/Logger.h"
#include "Poco/LogFile.h"
#include "Poco/StringTokenizer.h"
#include <lz4.h>
///:sqlite_storage

/// TODO: initialize 'next' to address of last block available

///:mvcc controller

/// TODO: unmerged version recovery
/// TODODONE: version storage naming
/// TODODONE: merge versions on commit or if there are too many versions

extern ptrdiff_t MAX_EXT_MEM ;

extern ptrdiff_t  _reported_memory_size();

namespace stx{
namespace storage{
	
	extern void add_total_use(long long added);
	extern void remove_total_use(long long removed);
	extern long long total_use;
	extern unsigned int ptime;
	extern long long last_flush_time;		/// last time it released memory to disk
	/// defines a concurrently accessable transactional storage providing ACID properties
	/// This class should be extended to provide encoding services for a b-tree or other
	/// storage oriented data structures

	/// a name factory for some persistent allocators
	struct default_name_factory{
		std::string name;
			
		default_name_factory(const std::string &name) : name(name){
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
		const std::string & get_name() const {
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
		virtual std::string get_name() const = 0;
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

		void add_entry(Poco::UInt32 command, const std::string& name, long long address, const buffer_type& buffer);
		
		/// ensures all journal entries are synched to storage

		void synch();

		/// called during startup to recover unwritten transactions

		void recover();

		/// log new address creation
		
		void log(const std::string &name, const std::string& jtype, stream_address sa);
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
		typedef std::unordered_map<address_type, block_reference> _Allocations;
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
	typedef Poco::ScopedLockWithUnlock<Poco::Mutex> scoped_ulock;
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
	enum journal_commands{
		JOURNAL_PAGE = 0,
		JOURNAL_COMMIT
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

		typedef std::unordered_map<address_type, version_type> _Versions;
	private: /// private types
		
		struct block_descriptor {
			block_descriptor(version_type version) : use(0), clock(0), mod(read), compressed(false), version(version){
			}
			block_descriptor(const block_descriptor& right) {
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
			void set_storage_action(storage_action action){
				/// don't overide create actions - for replication
				if(mod != create)
					mod = action;
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

		typedef std::unordered_map<address_type, ref_block_descriptor> _Allocations;

		typedef std::unordered_set<address_type> _Changed;

	private:
		/// per instance members
		Poco::AtomicCounter references; 
									/// counts references to this instance through release and engage methods

		mutable Poco::Mutex lock;	/// locks the instance 

		bool transient;				/// if this is flagged then the resources held must be deleted

		bool busy;					/// instance is busy complete should be called

		_Versions versions;			/// map of transient versions

		_Allocations allocations;	/// the live unflushed allocations

		_Changed changed;			/// a list of addresses which changed during the current transaction

		address_type next;			/// next virtual address to allocate

		address_type currently_active;
									/// currently active address

		block_type empty_block;		/// the empty block returned as end of storage

		std::string table_name;		/// table name in sqlite

		std::string name;			/// external name to users of class

		u64 changes;				/// counts the changes made

		version_type version;		/// the version of this allocator
		version_type allocated_version;
		/// to add a block

		std::shared_ptr<Poco::Data::Statement> insert_stmt ;

		/// to retrieve a block

		std::shared_ptr<Poco::Data::Statement> get_stmt ;

		/// has-a transaction been started

		bool transacted;

		std::shared_ptr<Session> _session;

		u32 clock;

		ptrdiff_t limit;			/// flush limit at which LRU partial flush starts

		ptrdiff_t _use;				/// the ammount of bytes allocated in buffers

		bool is_new;				/// flag set when storage file exists used to suppress file creation if there is nothing in it

		ptrdiff_t get_block_use(const block_descriptor& v){
			return v.block.capacity() + sizeof(block_type);
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

		static std::string get_storage_path(){

			return ""; //".\\";
		}

		/// does this table exist in storage

		bool has_table(std::string name){

			int count = 0;
			get_session() << "SELECT count(*) FROM sqlite_master WHERE type in ('table', 'view') AND name = '" << name << "';", into(count), now;

			return count > 0;
		}

		void create_allocation_table(){
			if(!has_table(table_name)){
				 std::string sql = "CREATE TABLE " + table_name + "(";
				 sql += "a1 integer primary key, ";
				 sql += "dsize integer, ";
				 sql += "data BLOB);";
				 get_session() << sql, now;
			}
		}
		
		Poco::UInt64 selector_address;
		Poco::UInt64 current_address;
		Poco::UInt64 max_address;
		Poco::Data::BLOB current_block;
		Poco::UInt64 current_size;

		template <typename T> Binding<T>* bind(const T& t)
			/// Convenience function for a more compact Binding creation.
		{
			return new Binding<T>(t);
		}

		void create_statements(){
		
				insert_stmt = std::make_shared<Poco::Data::Statement>( get_session() );
				*insert_stmt << "INSERT OR REPLACE INTO " << (*this).table_name << " VALUES(?,?,?) ;", bind(current_address), bind(current_size), bind(current_block);

				get_stmt = std::make_shared<Poco::Data::Statement>( get_session() );
				*get_stmt << "SELECT a1, dsize, data FROM " << (*this).table_name << " WHERE a1 = ?;" , into(current_address), into(current_size), into(current_block) , bind(selector_address);

				max_address = 0;

				get_session() << "SELECT max(a1) AS the_max FROM " << (*this).table_name << " ;" , into(max_address), now;
				(*this).next = std::max<address_type>((address_type)max_address, (*this).next); /// next is pre incremented
			
		}
		/// open the connection if its closed
		
		Session& get_session(){
			if(_session == nullptr){
				_session = std::make_shared<Session>("SQLite", get_storage_path() + name);//SessionFactory::instance().create
				is_new = false;
				create_allocation_table();
				create_statements();
				//printf("opened %s\n", name.c_str());
			}
			return *_session;
		}
		void add_buffer(const address_type& w, block_type& block){
			current_address = w;
			/// assumes block_type is some form of stl vector container
			current_size = block.size()*sizeof(typename block_type::value_type);
			current_block.clear();
			if(!block.empty()){				
				
				//compress_block
				
			
				current_block.assignRaw((const char *)&block[0], (size_t)current_size);
			}
			
			insert_stmt->execute();
		}

		/// returns true if the buffer with address specified by w has been retrieved

		bool get_buffer(const address_type& w){
			if(!w)
				throw InvalidAddressException();
			if(is_new) return false;
			get_session();
			selector_address = w;
			current_address = 0;
			get_stmt->execute();
			
			return (current_address == selector_address); /// returns true if a record was retrieved

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
		
		/// compress a single block and set its status 
		
		void _compress_block(block_descriptor& rb){
			if(!rb.compressed){
				
			}
		}
		void compress_block(ref_block_descriptor ref){
			if(ref != result){
				_compress_block(*ref);
			}
		}
		
		/// decompress a single block and set its status 

		void _decompress_block(block_descriptor& rb){
			rb.compressed = false;
		}
		void decompress_block(ref_block_descriptor rb){
			_decompress_block(*rb);
		}
		/// compress all uncompressed blocks in memory except the currently active one
		void compress_blocks(){
			
		}

		/// flush any or all excess data to disk, the factor determines the fraction
		/// of the total use to release i.e. a value of 0 releases all
		/// the default value will release 25% of the total used memory
		void up_use(ptrdiff_t change){
			add_total_use (change);
			_use += change;
		}
		void down_use(ptrdiff_t change){
			remove_total_use (change);
			_use -= change;
		}
		inline ptrdiff_t get_use() const {
			return _use;
		}
		/// writes changed buffers to disk while [release]-ing until
		/// the memory used is a [factor] of the original
		void flush_back
		(	double factor = 0.75
		,	bool release = true
		){
			if(result != nullptr){
				up_use(reflect_block_use(*result));
				result = nullptr;
			}

			ptrdiff_t before = get_use();

			typedef std::vector<std::pair<address_type, ref_block_descriptor> > _Blocks;

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
				if((*b).second->is_modified()){
					/// write blocks will be written in address order
					by_address.push_back(*b);
					mods++;
				}else if(get_use() > (before*factor)){ /// flush read blocks in LRU order
					if(release)
					{
						up_use(reflect_block_use((*b).second)); /// update to correctly reflect

						allocations.erase((*b).first);
					
						down_use( get_block_use((*b).second) );
						delete (*b).second;
					}
				}

			}
			if(!mods && get_use() <= (before*factor)){
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
				add_buffer((*b).first, (*b).second->block);

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
		}
		
		void print_use(){
			
			if(::GetTickCount()-ptime > 3000){
				//printf("total use blocks %.4g MiB\n", (double)total_use/(1024.0*1024.0));
				ptime = ::GetTickCount();
			}
		}
		void check_use(){
			print_use();
			
			//if(total_use > limit){
			if(NS_STORAGE::total_use+btree_totl_used > MAX_EXT_MEM){
				if(get_use() > 1024*1024*2){
					ptrdiff_t before = get_use();
					
					flush_back(0.4);
					last_flush_time = ::GetTickCount();
					//printf("flushed data %lld KiB - local before %lld KiB, now %lld KiB\n", (long long)total_use/1024, (long long)before/1024, (long long)get_use()/1024);
				}
			}
		}
		version_type version_off(address_type which){
			_Versions::iterator i = versions.find(which);
			if(i!=versions.end()){
				return (*i).second;
			}
			versions[which] = (*this).get_version();
			return (*this).get_version();
		}
	protected:
		block_type read_block;
		block_type & _allocate(address_type& which, storage_action how){
			
			lock.lock();
			
			busy = true;
			allocated_version = 0;
			check_use();
			currently_active = 0;
			result = nullptr;
			if(which){

				if(allocations.count(which) == 0){
					/// load from storage
					if((*this).get_buffer(which))
					{
						
						result = new block_descriptor(version_off(which)); 
						result->block.insert(result->block.begin(), current_block.begin(), current_block.end());
						allocations[which] = result;
						
					}else if(how == create)
					{
						result = new block_descriptor(version_off(which)); 
						allocations[which] = result;
						next = std::max<address_type>(which, next);
						
					}else
					{
						/// defines invalid block - caller must check ?
						return empty_block;
					}

				}else{
					result = allocations[which];
				}
				result->set_storage_action(how);
			}else{
				if(how != create) /// this is also probably a bit pedantic
					throw InvalidStorageAction();
				
				which = ++next;
				//printf("[CREATE ADDRESS] %lld %s\n", (long long)which, get_name().c_str());
				result = new block_descriptor(version_off(which)); //std::make_shared<block_descriptor>();
				allocations[which] = result;
				result->set_storage_action(create);

			}
			if(result->get_storage_action() != read){
				changed.insert(which); /// this action flags a change
				++changes;
			}
			
			up_use(reflect_block_use(result));

			result->clock = ++clock;
			currently_active = which;
			allocated_version = result->version;
			
			return result->block;
		}
		void assign_version(version_type version){
			if(result && currently_active) {
				versions[currently_active] = version;
				result->version = version;
			}else{
				printf("[WARNING] version not set\n");
			}
		}
		
	public:
		/// gets a version
		version_type get_version() const {
			return (*this).version;
		}
		/// sets a version
		void set_version(version_type version){
			(*this).version = version;
		}
		/// get the list of addresses stored
		typedef std::set<stream_address> _Addresses;
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
				out.insert((stream_address)(*a).first);
			}
			/// out contains a unique list of addresses in this allocator
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
				
				buffer_type *d = &(dest.allocate(at, write));
				if(dest.is_end(*d)){
					dest.complete();
					d = &(dest.allocate(at, create));
				}
				if((*this).get_allocated_version() < dest.get_allocated_version()){
					printf("low version copy error\n");
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
		,	address_type ma = 32		/// minimum address
		)
		:	name(namer.get_name())		/// storage name can be a path should contain only numbers and letters no seperators commans etc
										/// the path should contain the trailing delimeter
		,	_use(0)						/// current memory use in bytes
		,	limit(128 * 1024 * 1024)	/// default memory limit
		,	clock(0)					/// initial clock value gets incremented with every access
		,	transacted(false)			/// when true begin where called at least once since startup, commit or rollback
		,	next(ma)					/// logical address space iterative generator
		,	table_name("blocks")		/// table name - nothing special
		,	changes(0)					/// changes made since begin
		,	busy(false)					///	set to true during an allocation to false when complete is called
		,	result(nullptr)				///	save the last result after allocate is called to improve safety 
		,	transient(false)			/// if true the data files are deleted on destruction
		,	version(0)					/// starts the version at 0
		{
			using Poco::File;
			File df (get_storage_path() + name );
			is_new = !df.exists();
			if(!is_new){
				get_session();
			}
		}
		virtual std::string get_name() const {
			return (*this).name;
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
					File df (get_name());
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
			
			scoped_ulock ul(lock);
			if(references == 0){
				
				if(_session!=nullptr){
					printf(" discarding storage %s\n",name.c_str());
					rollback();
					insert_stmt = nullptr;
					get_stmt = nullptr;
					_session = nullptr;
				}
				for(typename _Allocations::iterator a = allocations.begin(); a!=allocations.end(); ++a){					
					down_use( get_block_use((*a).second) );
					delete (*a).second;
				}
				allocations.clear();
				versions.clear();
			}
		}

		/// initialize a specific address to exist if it didn't before otherwise return

		void initialize(const address_type& which){
			scoped_ulock ul(lock);
			if(!(*this).get_buffer(which)){
				result = new block_descriptor(); //std::make_shared<block_descriptor>();
				result->set_storage_action(create);
				allocations[which] = result;
				changed.insert(which);
				++changes;
			}
		}

		/// return the last allocated address

		address_type last() const{
			scoped_ulock ul(lock);
			
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
			NS_STORAGE::synchronized s(lock);
			
			if(busy){
				if(nullptr != result){
					up_use(reflect_block_use(result)); /// update changes made to last allocation
				}
				
				busy = false;
				lock.unlock();
			}
			allocated_version = 0;
		}
	public:
		/// start a transaction valid during the lifetime of this storage

		void begin(){
			scoped_ulock ul(lock);
			
			if(!is_new){ /// this flag is used to supress file creation
				_begin();
			}
		}
		/// a special case of begin only called by mvcc_coordinator
		/// this version ignores the is_new flag and starts a transaction
		/// anyway
		void begin_new(){
			scoped_ulock ul(lock);
			_begin();
			if(!is_new){}
		}
		void open(){
			scoped_ulock ul(lock);
			open_session();			
		}
		/// close all handles and opened files and release memory held in caches, unwritten pages are not flushed
		void close(){
			discard();
		}
		/// engages an instance reference
		void engage(){
			scoped_ulock ul(lock);
			
			++references;
		}
		void release(){ //s an instance reference
			scoped_ulock ul(lock);
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
				get_session() << "begin transaction;", now;
				transacted = true;
			}
		}
		/// helper function to finalize commit to storage after all
		/// modified pages have been flushed

		void commit_storage(){
			if(transacted){
				get_session() << "commit;", now;
			}
			transacted = false;
		}

	public:

		/// commit a transaction valid during the lifetime of this storage

		void commit(){
			scoped_ulock ul(lock);
			if(transacted){
				flush_back(0.0, false); /// write all changes to disk or pigeons etc.
			}
			commit_storage();

		}
		
		virtual void journal_commit() {
		}

		
		
		void journal(const std::string& name){
			_Addresses todo;
			scoped_ulock ul(lock);			
			get_addresses(todo);
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
			scoped_ulock ul(lock);
			flush_back(0.0, false); /// write all changes to disk or pigeons etc.
			
		}
		/// reverse any changes made by the current transaction

		void rollback(){
			scoped_ulock ul(lock);
			if(transacted)
				get_session() << "rollback;", now;
			transacted = false;
		}
		void reduce(){
			scoped_ulock ul(lock);
			flush_back(0.1);
		}
	};

	typedef std::shared_ptr<Poco::Mutex> mutex_ptr;
	typedef Poco::Mutex * mutex_ref;

	/// this defines a storage which is based on a previous version if this member is nullptr then
	/// the version based storage puts it in its own member storage

	/// this interface supports multi threaded access on public functions 
	/// except for contructors and destructors implying the users of instances
	/// of this class must release dependencies before its destruction
	
	/// example base storage typedef sqlite_allocator<_AddressType, _BlockType>
	
	template <typename _BaseStorage >
	class version_based_allocator {
	public:

		typedef _BaseStorage storage_allocator_type;

		typedef std::shared_ptr<storage_allocator_type> storage_allocator_type_ptr;

		typedef version_based_allocator* version_based_allocator_ptr;

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
			scoped_ulock _sync(*lock);
			(*this).based = based;
		}

		/// return the previous version of this version if there is one
		version_based_allocator_ptr get_previous(){
			scoped_ulock _sync(*lock);
			return (*this).based;
		}

		/// sets the storage allocator used for the storing version
		/// specific blocks
		void set_allocator(storage_allocator_type_ptr allocator){

			(*this).allocations = allocator;
			(*this).get_allocator().set_version((*this).get_version());

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
		}

		/// notify the addition of a new reader
		void add_reader(){
			
			(*this).readers++;
		}
		
		/// notify the release of an existing reader
		void remove_reader(){
			
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
		,	lock(lock.get())
		,	read_only(false)
		,	version(version)
		,	copy_reads(false)
		,	discard_on_end(false)
		,	readers(0)
		,	initial(initial)
		,	order(order)
		,	merged(false)
		,	allocated_version(0)
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
			scoped_ulock _sync(*lock);
			const version_based_allocator * b  =this;
			while(b!=nullptr){
				if(b->get_allocator().is_end(r)){
					return true;
				}
				b = b->based;
			}
			return false;
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
			scoped_ulock _sync(*lock);
			(*this).allocated_version = 0;

			if(is_readonly() && how != read)
				throw InvalidStorageAction();

			if(!which && how == create){
				block_type & r = get_allocator().allocate(which,how);/// a new one or exception
				allocated_version = get_allocator().get_allocated_version();	
				return r;
			}

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
			if(how==create)
				throw InvalidStorageAction();

			last_base  = this->based;
			while(last_base != nullptr){
				block_type& r = last_base->get_allocator().allocate(which, read); //can only read from previous versions 
				if(!(last_base->get_allocator().is_end(r))){
					if
					(	how == write 
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
				last_base = last_base->based;
			}
			
			
			/// it was never found in the base or local versions
			return empty_block;
			
		}
		void complete(){
			scoped_ulock _sync(*lock);
			get_allocator().complete();
			if(last_base != nullptr){
				last_base->get_allocator().complete();
				last_base = nullptr;
			}
			
		}
		void begin(){
			scoped_ulock _sync(*lock);
			if(allocations!=nullptr)
				allocations->begin();
		}
		/// special case only called by mvcc_coordinator
		void begin_new(){
			scoped_ulock _sync(*lock);
			if(allocations!=nullptr)
				allocations->begin_new();
		}
		
		void commit(){

			scoped_ulock _sync(*lock);
			if(allocations!=nullptr)
				allocations->commit();
		}
		void flush(){

			scoped_ulock _sync(*lock);
			if(allocations!=nullptr)
				allocations->flush();
		}

		/// write the allocated data to the global journal
		void journal(const std::string &name){
			scoped_ulock _sync(*lock);
			if(allocations!=nullptr)
				allocations->journal(name);
		}
		/// return the last address allocated during the lifetime of this version
		address_type last() const {
			scoped_ulock _sync(*lock);
			return allocations->last();
		}

		bool modified() const {
			scoped_ulock _sync(*lock);
			if(allocations!=nullptr)
				return allocations->modified();
			return false;
		}
		
	};

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

		typedef version_storage_type* version_storage_type_ptr;
		
		typedef std::vector<version_storage_type_ptr> storage_container;

		typedef std::unordered_map<u64, version_storage_type_ptr> version_storage_map;

		typedef typename storage_allocator_type::address_type address_type;
	private:
		struct version_namer{
			std::string name;
			
			version_namer(u64 version, const std::string extension) {
				
				name += extension;
				name += ".";
				Poco::NumberFormatter::append(name, version);				
				
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
			const std::string & get_name() const {
				return (*this).name;
			}
		
		};

		Poco::AtomicCounter references;		/// counts references to this instance through release and engage methods

		u64 order;							/// transaction order set on commit

		version_type next_version;			/// next version generator

		address_type last_address;			/// maximum address allocated

		storage_allocator_type_ptr initial;	/// initial storage

		storage_container storages;			/// list of versions already committed contains a min of 1 storages aftger construction

		version_storage_map storage_versions; 
											/// versions of each resource already committed
											/// used to check validity and re-aquire smart pointers								

		mutex_ptr lock;						/// lock for access to member data from different threads

		bool recovery;						/// flags recovery mode so that journal isnt used

	public:
		
		/// merge idle transactions from latest to oldest
		/// merge unused transaction versions - releasing any held resources
		/// the merge should produce only one table on completion in idle state
		/// i.e. storages.size()==1
		void merge_down()
		{
			scoped_ulock _sync(*lock);
			
			if(!storages.empty())
			{
				storage_container merged;
				storage_container::reverse_iterator latest_idle = storages.rbegin();
				storage_container idle;
			
				for(;;)
				{
					bool rok  = latest_idle != storages.rend();
					if(rok) (*latest_idle)->clear_merged();
					if(rok && (*latest_idle)->is_unused())
					{
						
						idle.push_back((*latest_idle));		/// collect idle transactions
						
						
					}else if(!idle.empty())
					{
						version_storage_type_ptr first;						
						storage_container::iterator i = idle.begin();
						first = (*i);
						++i;								/// at least 2 consecutive versions 
															/// are needed to actually merge their 
															/// resources
						for(; i != idle.end(); ++i)
						{
							first->copy((*i));		/// first -> i
							
							first->set_merged();			/// flagged as copied or merged
							first = (*i);
							
						}
						
						first->clear_merged();				/// its not merged with something lower down
						
						idle.clear();						/// cleanup references

					}else idle.clear();
					if(!rok){
						break;
					}
					++latest_idle;
				}
				
				for(storage_container::iterator c = storages.begin(); c != storages.end(); ++c)
				{
					if(!(*c)->is_merged())
					{
						if(recovery){
							/// only during recovery shall this be done
							(*c)->begin_new();
							(*c)->commit();
						}
						merged.push_back((*c));
					}else{
						if((*c)->is_unused()){
							(*c)->set_transient();				/// let any resources be cleaned up

							storage_versions.erase((*c)->get_version());
							(*c)->set_previous(nullptr);
							delete (*c);
						}else{
							throw InvalidVersion();
						}
					}
				}
				storages.swap(merged);

				version_storage_map tester;
				version_storage_type_ptr prev = nullptr;
				
				for(storage_container::iterator c = storages.begin(); c != storages.end(); ++c){
					if(tester.count((*c)->get_version()) != 0){
						throw InvalidVersion();
					}
					(*c)->set_previous(prev);
					tester[(*c)->get_version()] = (*c);
					prev = (*c);
				}
				
				if(storages.empty()){
					throw InvalidVersion();
				};					
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
		:	initial(initial)
		,	next_version(1)
		,	lock(std::make_shared<Poco::Mutex>())
		,	order(1)
		,	last_address ( initial->last() )
		,	recovery(false)
		{
			initial->engage();
			version_storage_type_ptr b = new version_storage_type(last_address, order, ++next_version, (*this).lock);
			b->set_allocator(initial);
			b->set_previous(nullptr);			
			storages.push_back(b);
			std::string names;
			stream_address addr = INTITIAL_ADDRESS;
			buffer_type& content = initial->allocate(addr, create);
			if(!initial->is_end(content) && content.size() > 0){
				names.resize(content.size());
				memcpy(&names[0], &content[0], names.size());
				printf("found initial versions %s\n",names.c_str());
			}
			initial->complete();
			if(!names.empty()){
				Poco::StringTokenizer tokens(names, ",");
				for(Poco::StringTokenizer::Iterator t = tokens.begin(); t != tokens.end(); ++t){
					storage_allocator_type_ptr allocator = std::make_shared<storage_allocator_type>(default_name_factory((*t)));
					last_address = std::max<stream_address>(last_address, allocator->last() );
					version_storage_type_ptr b = new version_storage_type(last_address, ++order, ++next_version, (*this).lock);						
					allocator->set_allocation_start(last_address);
					b->set_allocator(allocator);
					storages.push_back(b);
					storage_versions[b->get_version()] = b;			
				}
				merge_down();
				if(storages.size() > 1){
					throw InvalidStorageException();
				}
				initial->begin_new();
				initial->allocate(addr, create).clear();
				initial->complete();
				initial->commit();
			}
			journal::get_instance().engage_participant(this);
		}

		~mvcc_coordinator(){

			journal::get_instance().release_participant(this);
			initial->discard();
		}

		/// engages the instance - its resources may not be released if references > 0

		void engage(){
			scoped_ulock _sync(*lock);
			++references;
			
		}

		/// releases a reference to this coordinatron

		void release(){
			scoped_ulock _sync(*lock);
			--references;
			/// TODO: if references == 0 the handles held to resources 
			///	need to let go so that maintenance can happen (i.e. drop table)
			if(0==references){
				
				//merge_down();
				if(storages.size()==1){
					initial->release();
				}/// else TODO: its an error since the merge should produce only one table on completion in idle state
			}
		}

		/// reduces use

		void reduce(){
		}

		/// start a new version with a dependency on the previously commited version or initial storage
		/// smart pointers are not thread safe so only use them internally 

		version_storage_type* begin(){
			scoped_ulock _sync(*lock);
			/// TODODONE: reuse an existing unmerged transaction - possibly share unmerged transactions	
			/// TODODONE: lazy create unmerged transactions only when a write occurs
			/// TODO: optimize mergeable transactions
			/// TODODONE: merge unused transactions into single transaction
			version_storage_type_ptr b = new version_storage_type(last_address, order, ++next_version, (*this).lock);
			storage_allocator_type_ptr allocator = std::make_shared<storage_allocator_type>(version_namer(b->get_version(),initial->get_name()));
			allocator->set_allocation_start(last_address);
			b->set_allocator(allocator);
			storage_versions[b->get_version()] = b;			
			if(!storages.empty()){
				b->set_previous(storages.back());
				storages.back()->add_reader();
			}else{
				/// TODO: should be an err
				throw WriterConsistencyViolation();
			}

			return b;
		}

		/// return the transaction order of this coordinator
		
		u64 get_order() const {
			scoped_ulock _sync(*lock);
			return (*this).order;
		}

		/// finish a version and commit it to storage
		/// the commit is coordinated with other mvcc storages
		/// via the journal
		/// the version may be merged with dependent previous versions

		void commit(version_storage_type* transaction){
			
			scoped_ulock _sync(*lock);
			
			//printf("[COMMIT MVCC] [%s] %lld at v. %lld\n", transaction->get_allocator().get_name().c_str(), (long long)transaction->get_allocator().get_version());
			
			if(transaction->modified() && transaction->get_order() < order){
				discard(transaction);
				throw InvalidWriterOrder();				
			}
			
			version_storage_type_ptr version = storage_versions.at(transaction->get_version());
			
			if(version == nullptr){
				throw InvalidVersion();				
			}
			
			
			version->set_readonly();	
			transaction->set_transient();

			if (!recovery)		/// dont journal during recovery
				transaction->journal((*this).initial->get_name());


			last_address = std::max<address_type>(last_address, version->last());
			
			version_storage_type_ptr prev = version->get_previous();
			if(prev==nullptr)
				throw WriterConsistencyViolation();
			if(prev != storages.back()){
				throw ConcurrentWriterError();
			}
			prev->remove_reader();
			
			storages.push_back(version);

			++order;			/// increment transaction count

			merge_down();		/// merge unused transaction versions 
								/// releasing any held resources
			
			
		}

		/// discard a new version similar to rollback

		void discard(version_storage_type* transaction){

			synchronized _sync(*lock);

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
			if(version != transaction){
				throw InvalidVersion();
			}
			version_storage_type_ptr prev = version->get_previous();
			
			if(prev!=nullptr)		
				prev->remove_reader();
	
			storage_versions.erase(transaction->get_version());
			delete transaction;
			merge_down();	/// merge unused transaction versions 
							/// releasing any held resources
		}

		void set_recovery(bool recovery){
			(*this).recovery = recovery;
		}

		/// journal participation functions
		/// the journal asks this participant to 
		/// commit its storage
		virtual void journal_commit() {
			
			std::string names; 
			storage_container::iterator c = storages.begin();
			storage_container::iterator c_begin = ++c;
			for(; c != storages.end(); ++c)
			{
				if( c != c_begin )
					names += ",";
				std::string n = (*c)->get_allocator().get_name();
				names += n;
			}
			

			stream_address addr = INTITIAL_ADDRESS;
			buffer_type& content = initial->allocate(addr, create);
			content.resize(names.size());
			if(names.size() > 0)
				memcpy(&content[0], &names[0], names.size());
			initial->complete();
			for(storage_container::iterator c = storages.begin(); c != storages.end(); ++c)
			{
				(*c)->begin_new();
				
				(*c)->commit();
				
			}
		}

		virtual std::string get_name() const {
			return this->initial->get_name();
		}
	};
/// complete namespaces
};/// storage
};///stx
#endif //_TRANSACTIONAL_STORAGE_H_
