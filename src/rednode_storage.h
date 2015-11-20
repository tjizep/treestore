#include "red_includes.h"
#include "rednode.h"
namespace red{
	using namespace Poco::Data;
	typedef rabbit::unordered_map<address_type, version_type> _Versions;
	template<typename _T>
		class symbol_stream{
		private:
			_T& st;
			bool buffering;
			nst::buffer_type buffer;
		private:
			bool readn(void * out, nst::u32 size){
				nst::u32 total = 0;
				nst::u32 remaining = size;
				while(total < size){
					int n = st.receiveBytes(&((nst::u8*)(out))[total], remaining);
					if(n<=0){
						return false;
					}
					total += (nst::u32)n;
					remaining -= (nst::u32)n;
				}			
				return true;
			}
			nst::u8 read_type(){
				nst::u8 r = 0;						
				readn(&r, sizeof(r));
				return r;
			
			}
			bool write_buffer(const void * b, size_t s){
				if(s > 0xFFFFFFFFl){
					return false;
				}
				if(buffering){
					const nst::u8 * bytes = (const nst::u8 *)b;
					for(int b = 0; b < s; ++b){
						buffer.push_back(bytes[b]);
					}
				}else{
					st.sendBytes(b, (int)s);
				}
				return buffering;
			}
			bool write_type(nst::u8 t){
				write_buffer(&t, sizeof(t));
				return true;
			}
		public:
			symbol_stream(_T& s): st(s), buffering(false){
			}
			void start_buffering(){
				buffering = true;
			}
			void flush_buffer(){
				buffering = false;
				write_buffer(buffer.data(), (int)buffer.size());
				buffer.clear();
				
			}
			
			bool write(bool symbol){			
				write_type(type_bool);
				write_buffer(&symbol, sizeof(symbol));
				
				return true;
			}

			bool write(nst::u32 symbol){
			
				write_type(type_u32);
				write_buffer(&symbol, sizeof(symbol));
				return true;
			}
		
			bool write(nst::u64 symbol){
				write_type(type_u64);
				write_buffer(&symbol, sizeof(symbol));
				return true;
			}
		
			bool write(const nst::buffer_type &data){
				if(data.size() > 0xFFFFFFFFl){
					return false;
				}
				write_type(type_buffer);
				write((nst::u32)data.size());
				write_buffer(data.data(), data.size());
				return true;
			}
		
			bool write(const std::string &data){
				
				write_type(type_string);
				write((nst::u32)data.size());
				write_buffer(data.data(), (int)data.size());
				return true;
			}
		
			bool write(const version_type& symbol){
				write_type(type_version);
				char buffer[16];
				symbol.copyTo(buffer);
				write_buffer(buffer, sizeof(buffer));
				return true;
			}
			
			bool read(bool &symbol){
				nst::u8 t = read_type();
				if(t!=type_bool) return false;
				return readn(&symbol, sizeof(symbol));
			}
			bool read(nst::u32 &symbol){
				nst::u8 t = read_type();
				if(t!=type_u32) return false;
				return readn(&symbol, sizeof(symbol));
			}
		
			bool read(nst::u64 &symbol){
				nst::u8 t = read_type();
				if(t!=type_u64) return false;
				return readn(&symbol, sizeof(symbol));
			}
		
			bool read(nst::buffer_type &data){
				nst::u8 t = read_type();
				if(t!=type_buffer) return false;
				nst::u32 l = 0;
				if(!read(l)) return false;
				data.resize(l);
				return readn(&data[0], l);
			}

			bool read(std::string &data, size_t mx = 0){
				nst::u8 t = read_type();
				if(t!=type_string) return false;
				nst::u32 l = 0;
				if(!read(l)) return false;
				if(mx && l != mx) return false;
				data.resize(l);
				return readn(&data[0], l);
			}

			bool read(version_type& symbol){				
				char buffer[16];
				nst::u8 t = read_type();
				if(t!=type_version) return false;				
				bool r = readn(&buffer[0], sizeof(buffer));
				if(r)
					symbol.copyFrom(buffer);
				return r;
			}

			~symbol_stream(){
			}
		};
	class sqlite_allocator {
	public:

		/// per instance members
		Poco::AtomicCounter references;
									/// counts references to this instance through release and engage methods

	private: /// private types

		typedef std::shared_ptr<block_type> block_type_ptr;
		
		typedef rabbit::unordered_map<address_type, block_type_ptr> _BufferMap;

		struct version_rec{
			version_rec(){
				this->version = version_type();
				this->count = 0;
			}
			version_type version;
			nst::u64 count;			
		};
		typedef rabbit::unordered_map<address_type, version_rec> _VersionLockMap;
	private: /// private fields
		_Versions versions;

		_VersionLockMap locks;

		mutable Poco::Mutex lock;	/// locks the instance

		std::shared_ptr<Poco::Data::Statement> insert_stmt ;

		/// to retrieve a block

		std::shared_ptr<Poco::Data::Statement> get_stmt ;

		/// to check if block exists

		std::shared_ptr<Poco::Data::Statement> exists_stmt ;

		/// get a version from address

		std::shared_ptr<Poco::Data::Statement> version_stmt ;

		/// has-a transaction been started

		bool transacted;

		std::shared_ptr<Poco::Data::Session> _session;

		nst::u32 clock;

		std::string table_name;
		
		std::string name;

		std::string extension;

		nst::u64 file_size;

		bool is_new;
		
		_BufferMap	buffers;
	private:
		
		bool has_mem_buffer(address_type address) const {
			return buffers.count(address) != 0;
		}
		
		block_type& get_mem_buffer(address_type address){
			block_type_ptr& bp = buffers[address];
			if(bp == nullptr){
				bp = std::make_shared<block_type>();
			}
			return *(bp.get());
		}

		void evict_mem_buffers(){
			for(_BufferMap::iterator b = buffers.begin(); b!=buffers.end();++b){
				b.get_value() = nullptr;
			}
		}
		
		static std::string get_storage_path(){

			return ""; //".\\";
		}

		/// does this table exist in storage

		bool has_table(std::string name){

			int count = 0;
			get_session() << "SELECT count(*) FROM sqlite_master WHERE type in ('table', 'view') AND name = '" << name << "';", into(count), now;
			get_session() << "PRAGMA shrink_memory;", now;
			return count > 0;
		}

		void create_allocation_table(){
			if(!has_table(table_name)){
				 std::string sql = "CREATE TABLE " + table_name + "(";
				 sql += "a1 integer primary key, ";
				 sql += "dsize integer, ";
				 sql += "version BLOB, ";
				 sql += "packet integer, ";				 
				 sql += "data BLOB);";
				 get_session() << sql, now;
			}
		}
		nst::u64 selector_address;
		nst::u64 selector_packet;
		nst::u64 current_address;
		Poco::Data::BLOB current_version;
		nst::u64 current_packet;
		nst::u64 max_address;
		nst::u64 exists_count;
		Poco::Data::BLOB encoded_block;
		Poco::UInt64 current_size;
		block_type current_block;
		block_type compressed_block;
		nst::u64 next;
		
		template <typename T> Binding<T>* bind(const T& t)
			/// Convenience function for a more compact Binding creation.
		{
			return new Binding<T>(t);
		}

		void create_statements(){
			insert_stmt = std::make_shared<Poco::Data::Statement>( get_session() );
			*insert_stmt << "INSERT OR REPLACE INTO " << (*this).table_name << " VALUES(?,?,?,?,?) ;", 
				bind(current_address), bind(current_size), bind(current_version), bind(current_packet), bind(encoded_block);

			get_stmt = std::make_shared<Poco::Data::Statement>( get_session() );

			*get_stmt << "SELECT a1, dsize, data, version, packet FROM " << (*this).table_name << " WHERE a1 = ? AND packet = ?;"
				, into(current_address), into(current_size), into(encoded_block), into(current_version), into(current_packet)
				, bind(selector_address) , bind(selector_packet);
			
			version_stmt = std::make_shared<Poco::Data::Statement>( get_session() );

			*version_stmt << "SELECT version FROM " << (*this).table_name << " WHERE a1 = ? AND packet = ?;"
				, into(current_version), bind(selector_address), bind(selector_packet);

			exists_stmt = std::make_shared<Poco::Data::Statement>( get_session() );
			/// block readahead statements
			*exists_stmt << "SELECT count(*) FROM " << (*this).table_name << " WHERE a1 = ? AND packet = ?;"
				, into(exists_count)
				, bind(selector_address), bind(selector_packet);

			max_address = 0;

			get_session() << "SELECT max(a1) AS the_max FROM " << (*this).table_name << " ;" , into(max_address), now;
			(*this).next = std::max<address_type>((address_type)max_address, (*this).next); /// next is pre incremented
			

		}
		/// open the connection if its closed

		Session& get_session(){
			if(_session != nullptr){
				return *_session;
			}
			if(_session == nullptr){
				_session = std::make_shared<Session>("SQLite", get_storage_path() + name + extension);//SessionFactory::instance().create
				//is_new = false;
				create_allocation_table();
				create_statements();
				Poco::File df (get_storage_path() + name + extension );
				file_size = df.getSize();
					
				//printf("opened %s\n", name.c_str());
			}
			return *_session;
		}
		void copy(Poco::Data::BLOB& to, const version_type& from) const {
			char buffer[16];
			from.copyTo(buffer);
			to.assignRaw(buffer, sizeof(buffer));
		}
		void copy(version_type& to, const Poco::Data::BLOB& from) const {
			if(from.size()>=16)
				to.copyFrom(from.rawContent());
			else
				printf("[TS] [RED] invalid from buffer for version\n");
		}
		version_type convert(const Poco::Data::BLOB& from) const {
			version_type r;
			copy(r, from);
			return r;			
		}
		void add_buffer(const address_type& w,const version_type& version, const block_type& block){
			if(version==version_type()){
				printf("[TS] [RED] invalid version\n");
				return;
			}
			
			current_address = w;			
			current_size = block.size()*sizeof(block_type::value_type);			
			

			current_address = w;
			copy(current_version, version);			
			current_packet = 0;
			/// assumes block_type is some form of stl vector container
			current_size = block.size()*sizeof(block_type::value_type);
			encoded_block.clear();
			if(!block.empty()){

				//compress_block
				//compressed_block = block;
				//inplace_compress_zlibh(compressed_block);
				///current_size = compressed_block.size()*sizeof(typename block_type::value_type);
				current_size = block.size();
				encoded_block.clear();
				encoded_block.assignRaw((const char *)&block[0], (size_t)current_size);
				versions[w] = version;
			}

			insert_stmt->execute();
			get_mem_buffer(w) = block;
			
		}
		
		/// returns true if the buffer with address specified by w has been retrieved
		bool get_exists(const address_type& w){
			//if(is_new) return false;
			if(has_mem_buffer(w)) return true;
			if(w >= next) return false;
			get_session();
			selector_address = w;
			selector_packet = 0;
			exists_count = 0;
			exists_stmt->execute();
			return exists_count > 0;
		}

		bool get_buffer(const address_type& w){
			if(!w)
				throw InvalidAddressException();
			if(has_mem_buffer(w)){
				current_block = get_mem_buffer(w);
				_get(w);
				return true;
			}
			//if(is_new) return false;
			get_session();
			current_block.clear();
			
			selector_address = w;
			selector_packet = 0;			
			copy(current_version,version_type());
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
		/// unlocked version
		version_type _get(const address_type& w){
			copy(current_version,version_type());
			version_type r;
			if(versions.get(w,r)) {
				copy(current_version,r);
				return r;
			}
			get_session();
			selector_address = w;
			selector_packet = 0;
			version_stmt->execute();

			copy(r,current_version);
			if(r != version_type())
				versions[w] = r;
			return r;
		}
		bool _unlock_version(const address_type& w, const version_type& version){
			version_rec &version_locked = locks[w];

			if(version_locked.version != version){
				return false;
			}
			if(version_locked.count > 0){
				--(version_locked.count);
				if(version_locked.count == 0){
					locks.erase(w);
				}
			}else{
				printf("[TS] [RED] [ERROR] invalid lock count\n");
			}
			return true;

		}
	protected:
		
	public:

		
		/// construct a sqlite database with name specifying the table in which blocks will be stored
		/// in this case the name default is used
		/// TODO: the get_storage_path function provides a configurable path to the database file itself

		
		sqlite_allocator
		(	std::string name			/// storage name
		,	address_type ma = 32		/// minimum address
		)
		:	table_name("blocks")		/// table name - nothing special
        ,   name(name)					/// storage name can be a path should contain only numbers and letters no seperators commans etc
		,	extension("")				/// extension used by rednode
										/// if true the data files are deleted on destruction
		,	transacted(false)
		,	selector_packet(0)
		,	is_new(false)
		,	next(ma)
		{
			
			using Poco::File;
			//current_version.resize(16);

			if(!is_new){
				File df (get_storage_path() + name + extension );
				(*this).is_new = !df.exists();
				if(!(*this).is_new){
					get_session();
					
				}
			}else (*this).is_new = true;
			
		}
		virtual std::string get_name() const {
			return (*this).name;
		}
	

		~sqlite_allocator(){
			discard();
	
		}
			
	private:
		/// throws an exception when an assignment is attempted

		sqlite_allocator& operator=(const sqlite_allocator&){
			throw std::exception();
			return *this;
		}
		/// check system mem state and evict if required
		void check_use(){
			if(buffer_allocation_pool.is_near_depleted()){
				get_session() << "PRAGMA shrink_memory;", now;
				evict_mem_buffers();
			}
		}
	public:

		
		/// discard all data and internal state if references are 0

		void discard(){

			synchronized ul(lock);
		
		}

		/// initialize a specific address to exist if it didn't before otherwise return

		void initialize(const address_type& which){
			
		}
		/// return the storage size of this allocator
		nst::u64 get_storage_size() const {
			return this->file_size;
		}
		/// return the last allocated address

		address_type last() const{
			synchronized ul(lock);

			return (*this).next;
		}

		

		/// contains
		/// returns true if an address exists
		bool contains(const address_type& which){
			NS_STORAGE::synchronized s(lock);//lock.lock();
			if(which){
				if((*this).get_exists(which))
					return true;
				
			}
			return false;
		}
		
		
	public:
		/// start a transaction valid during the lifetime of this storage

		void begin(){
			synchronized ul(lock);

			//if(!is_new){ /// this flag is used to supress file creation
			_begin();
			//}
		}
		/// a special case of begin only called by mvcc_coordinator
		/// this version ignores the is_new flag and starts a transaction
		/// anyway
		void begin_new(){
			synchronized ul(lock);
			_begin();
			//if(!is_new){}
		}
		void open(){

			synchronized ul(lock);
			//(*this).open_session();
		}
		/// close all handles and opened files and release memory held in caches, unwritten pages are not flushed
		void close(){
			synchronized ul(lock);
			discard();
		}
		/// used checking versions n stuff
		Poco::Mutex& get_lock(){
			return this->lock;
		}
		/// engages an instance reference
		void engage(){
			synchronized ul(lock);

			++references;
		}

		void release(){ //s an instance reference
			synchronized ul(lock);
			if(references>0)
				--references;
			//printf("references for %s reached %ld\n", name.c_str(), (long)references);
			discard();
		}

		void set(const address_type& w, const version_type& version, const block_type& block){
			synchronized ul(lock);
			check_use();
			add_buffer(w,version, block);			
		}
		/// returns the vesion and value
		version_type get(const address_type& w, block_type& block){
			synchronized ul(lock);
			check_use();
			get_buffer(w);
			block = current_block;
			version_type r;
			copy(r,current_version);
			return r;
		}

		version_type get(const address_type& w){
			synchronized ul(lock);
			check_use();
			return _get(w);
		}
		/// lock a version of w
		/// if version != locked version version fails
		/// if a non 0 old_version is specified the 
		/// lock count on that is reuced (used for read locks)
		/// if function succeeds the lock count is incremented
		bool lock_version
		(	const address_type& w
		,	const version_type& version
		,	const version_type& old_version = version_type()){
			//if(version==version_type()) {
			//	printf("[TS] [RED] [ERROR] [STORAGE] invalid lock version\n");
			//	return false;
			//}
			
			synchronized ul(lock);
			if(old_version!=version_type()){
				_unlock_version(w, old_version);
			}
			version_rec &version_locked = locks[w];
			if(version_locked.version == version_type()){
				version_locked.version = version==version_type()? _get(w) : version;
			}
			if(version_locked.version != version_type() && version_locked.version != version){
				return false;
			}
			version_locked.version = version;
			++(version_locked.count);
			return true;
		}
		address_type max_block_address(){
			synchronized ul(lock);
			get_session();
			return (*this).next;
		}
		bool unlock_version(const address_type& w, const version_type& version){
			synchronized ul(lock);
			
			return _unlock_version(w, version);	
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
			synchronized ul(lock);
			check_use();
			commit_storage();
			_begin();

		}

		/// reverse any changes made by the current transaction

		void rollback(){
			synchronized ul(lock);
			if(transacted){
				get_session() << "rollback;", now;

			}
			
			transacted = false;
			check_use();
		}
		void reduce(){
			check_use();
			
		}
	};
}; //red namespace