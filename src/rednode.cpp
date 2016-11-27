#include "rednode.h"
#include "Poco/Thread.h"
#include "Poco/Glob.h"
#include <string>
#include "rednode_file.h"
#include "rednode_storage.h"
#include "NotificationQueueWorker.h"
#include "system_timers.h"
extern char * treestore_red_address;
static Poco::Mutex aloc_lock;
static Poco::Mutex sock_lock;

typedef red::sqlite_allocator _RedAlloc;
typedef std::shared_ptr<_RedAlloc> _RedAllocPtr;
typedef std::unordered_map<std::string, _RedAllocPtr> _AlocMap;
static _AlocMap allocs;
void red_println(const char * format,...){
	if(false){
		va_list vargs;
		va_start(vargs, format);
		printf("[RED INFO] ");
		vprintf(format, vargs);
		printf("\n");
		va_end(vargs);
	}
}
void red_error(const char * format,...){
	if(true){
		va_list vargs;
		va_start(vargs, format);
		printf("[RED ERROR] ");
		vprintf(format, vargs);
		printf("\n");
		va_end(vargs);
	}
}
_RedAlloc* get_red_store(const std::string &name){
	nst::synchronized l(aloc_lock);
	if(allocs.count(name)){
		return allocs[name].get();
	}
	_RedAllocPtr a = std::make_shared<_RedAlloc>(name+".red",32);		
	allocs[name] = a;
	return a.get();
		
}
static std::atomic<nst::u64> connection_ids = 0;
static std::atomic<nst::u64> client_connection_ids = 0;
namespace red_workers{
	
	
	typedef asynchronous::QueueManager<asynchronous::AbstractWorker> _WorkerManager;
    struct _red_worker{
        _red_worker() : w(1){
        }
        _WorkerManager w;
    };

	unsigned int ctr = 0;
	unsigned int get_next_counter(){
		return ++ctr;
	}
	const int MAX_WORKER_MANAGERS = 32;
	extern _WorkerManager & get_threads(nst::u64 which){
		static _red_worker _adders[MAX_WORKER_MANAGERS];
		return _adders[which % MAX_WORKER_MANAGERS].w;
	}
}

namespace red{
	static std::string PROTO_NAME = "MARVAL00";
	enum{
		cmd_set=1,
		cmd_get,
		cmd_open,
		cmd_close,
		cmd_begin,
		cmd_commit,
		cmd_checkpoint,
		cmd_rollback, ///types and commands must be mutex - so that stream can be more robust
		cmd_maxaddress,
		cmd_evict,
		cmd_unlock_all,
		cmd_max_block_address,
		cmd_version,
		cmd_contains,
		type_bool,
		type_u32,
		type_u64,
		type_string,
		type_buffer,
		type_data,
		type_version,
		err_versions,
		err_version_mismatch,
		err_version_locked,
		err_address_does_not_exist
	};
	inline void rmsg(nst::u32 r,const char * fun = nullptr){
		if(r!=0){
			red_println("[CLIENT] [%s] %s returned [%ld]",r!=0 ? "ERROR":"INFO",fun == nullptr ? "client operation":fun, r);
		}else{
			red_println("[CLIENT] [%s] %s returned [%ld]",r!=0 ? "ERROR":"INFO",fun == nullptr ? "client operation":fun, r);
		}
	}
	class red_socket{
	private:
		std::string					address;		
		Poco::Net::SocketAddress	sa;
		Poco::Net::StreamSocket		sock;
		Poco::Net::SocketStream		stream;
		bool open;
	public:
		red_socket(const std::string &address) 
		:	address(address)
		,	sa(address)
		,	sock()
		,	stream(sock)
		,	open(false){
			using Poco::Exception;
			// Initiate blocking connection with server.
			connect();
		}
		bool disconnect(){
			red_println( "[CLIENT] [SOCKET] Starting disconnect to %s:%ld" ,sa.host().toString().c_str(),sa.port());
			try {
				sock.close();
						
				
			}
			catch (Poco::Exception& error) {
				red_error("[CLIENT] [SOCKET] disconnection failed (Error: %s)",error.displayText().c_str());
			
			}
			open = false;
			return is_open();
			
		}
		bool connect(){
			red_println( "[CLIENT] [SOCKET] Starting connect to %s:%ld" ,sa.host().toString().c_str(),sa.port());
			try {
				sock.connect(sa);
						
				symbol_stream<Poco::Net::StreamSocket> symbols(sock);
				symbols.start_buffering();
				symbols.write(PROTO_NAME);			
				symbols.flush_buffer();
				open = true;
			}
			catch (Poco::Exception& error) {
				red_error("[CLIENT] [SOCKET] Connection failed (Error: %s)",error.displayText().c_str());
			
			}
			return is_open();
		}
		bool is_open() const {
			return this->open;
		}
		const std::string& get_address() const {
			return this->address;
		}
		Poco::Net::StreamSocket	&get_socket(){
			return this->sock;
		}
		~red_socket(){};
	};
	typedef std::shared_ptr<red_socket> red_socket_ptr;
	typedef rabbit::unordered_map<std::string, red_socket_ptr> _SockMap;
	static _SockMap sockets;

	red_socket* get_socket(const std::string &address){
		red_socket_ptr result;
		{
			synchronized context(sock_lock);		
			if(!sockets.get(address,result)){
				result = std::make_shared<red_socket>(address);
				sockets[address] = result;
			}
		}
		if(!result->is_open()){
			result->disconnect();
			result->connect();
		}
		return result.get();
	}
	void remove_socket(const std::string & address){
	}

	class client_allocator_connection{
	private:
		red_socket					* socket;
		std::string					address;
		std::string					name;
		bool						opened;
		nst::u64					id;
	private:
		Poco::Net::StreamSocket	&get_socket(){
			if(!this->socket->is_open()){
				socket_not_opended();
			}
			return this->socket->get_socket();
		}
		void comms_failure(Poco::Exception &e){
			red_error("[CLIENT] [CONNECTION] failure to communicate (Error: %s)",e.displayText().c_str());
			opened = false;
		}
		void socket_not_opended(){
			red_error("[CLIENT] [CONNECTION] client socket not opened");
			opened = false;
			throw Poco::Exception("client socket not opened");
		}
		bool not_opended(){
			red_error("[CLIENT] [CONNECTION] client not opened");
			return false;
		}
	public:
		
		client_allocator_connection(std::string address);
		~client_allocator_connection();
		bool open(const std::string &name);
		bool close();
		bool promise(address_type address);
		bool get_version(address_type address, version_type&version);
		bool contains(address_type address);
		
		bool store(version_type &failed, address_type address, version_type version, const block_type &data);
		bool get(address_type address, version_type version, block_type& data);
		bool get(address_type address, version_type version, version_type &ov, block_type& data);
		bool begin(version_type version, bool writer);
		bool commit();
		bool rollback();
		bool unlock_all();
		std::string get_address() const;
		bool is_open();
		address_type max_block_address();
	
	};
	client_allocator_connection::client_allocator_connection(std::string address)
	:	address(address),opened(false){
		using Poco::Net::StreamSocket;
		using Poco::Net::SocketStream;
		using Poco::Net::SocketAddress;
		using Poco::Exception;
		
		this->id = ++client_connection_ids;
		red_println("[CLIENT] [SOCKET] new connection (id:%lld)",this->id);
    
	}
	std::string client_allocator_connection::get_address() const {
		return this->socket->get_address();
	}
	bool client_allocator_connection::is_open(){
		return this->opened;
	}
	client_allocator_connection::~client_allocator_connection(){
		red_println("[CLIENT] [SOCKET] closing red client");
	}
	bool client_allocator_connection::open(const std::string &name){
		opened = false;
		socket = red::get_socket(address);
		if(!this->socket->is_open()) return not_opended();
		nst::u32 cmd = cmd_open;
		nst::u32 r = 0xFFFFFFFF;		
		try{
			
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.write(name);
			symbols.flush_buffer();		
			symbols.read(r);
			opened = (r == 0);
			if(opened)
				this->name = name;
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return true;
	}
	
	bool client_allocator_connection::promise(address_type address){
		return false;
	}
	
	bool client_allocator_connection::get_version(address_type address, version_type&version){
		if(!opened) return not_opended();
		nst::u32 r = 0xFFFFFFFF;		
		nst::u32 cmd = cmd_version;		
		try{
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.write(address);						
			symbols.flush_buffer();
			symbols.read(version);
			symbols.read(r);
			rmsg(r,"version");
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return r == 0;
	}

	bool client_allocator_connection::contains(address_type address){
		if(!opened) return not_opended();
		nst::u32 r = 0xFFFFFFFF;		
		nst::u32 cmd = cmd_contains;		
		try{
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.write(address);
			symbols.flush_buffer();		
			symbols.read(r);
			rmsg(r,"contains");
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return r == 0;
	}
	
	bool client_allocator_connection::store(version_type &failed, address_type address, version_type version,const block_type &data){
		failed = version; /// to simplify caller code
		if(!opened) return not_opended();
		nst::u32 r = 0xFFFFFFFF;		
		nst::u32 cmd = cmd_set;		
		try{
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.write(address);
			symbols.write(version);
			symbols.write(data);
			symbols.flush_buffer();
			symbols.read(failed);
			symbols.read(r);
			rmsg(r,"set");
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return r == 0;
	}
	bool client_allocator_connection::close(){
		if(!opened) return not_opended();
		nst::u32 r = 0xFFFFFFFF;		
		nst::u32 cmd = cmd_close;		
		try{
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.flush_buffer();		
			symbols.read(r);
			rmsg(r,"close");
			// ??? opened = false;
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return r == 0;
	}
	bool client_allocator_connection::get(address_type address, version_type version, block_type& data){
		if(!opened) return not_opended();
		nst::u32 r = 0xFFFFFFFF;
		nst::u32 cmd = cmd_get;		
		try{
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.write(address);
			symbols.write(version);
			symbols.flush_buffer();
			symbols.read(data);		
			version_type ov;
			symbols.read(ov);	
			symbols.read(r);
			rmsg(r,"get");
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return r == 0;
	}
	bool client_allocator_connection::get(address_type address, version_type version, version_type &ov, block_type& data){
		if(!opened) return not_opended();
		nst::u32 r = 0xFFFFFFFF;
		nst::u32 cmd = cmd_get;		
		try{
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.write(address);
			symbols.write(version);
			symbols.flush_buffer();
			symbols.read(data);					
			symbols.read(ov);	
			symbols.read(r);
			rmsg(r,"get version");
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return r == 0;
	}
	bool client_allocator_connection::begin(version_type version, bool writer){
		if(!opened) return not_opended();
		nst::u32 r = 0xFFFFFFFF;
		nst::u32 cmd = cmd_begin;		
		try{
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
		
			symbols.write(id);
			symbols.write(cmd);
			symbols.write(writer);
			symbols.write(version);
			symbols.flush_buffer();
		
			symbols.read(r);
			rmsg(r,"begin");
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return r == 0;
	}
	bool client_allocator_connection::commit(){
		if(!opened) return not_opended();
		nst::u32 cmd = cmd_commit;
		nst::u32 r = 0xFFFFFFFF;		
		try{
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.flush_buffer();
		
			symbols.read(r);
			rmsg(r,"commit");
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return r == 0;
	}
	bool client_allocator_connection::rollback(){
		if(!opened) return not_opended();
		nst::u32 cmd = cmd_rollback;
		nst::u32 r = 0xFFFFFFFF;
		try{
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.flush_buffer();		
			symbols.read(r);		
			rmsg(r,"rollback");
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return r == 0;
	}
	bool client_allocator_connection::unlock_all(){
		if(!opened) return not_opended();
		nst::u32 cmd = cmd_unlock_all;
		nst::u32 r =1;
		try{
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.flush_buffer();
		
			symbols.read(r);		
			rmsg(r,"unlock all");
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return r == 0;
	}


	address_type client_allocator_connection::max_block_address(){
		address_type ma = 0;
		if(!opened) {
			not_opended();
			return 0;
		}
		try{
			nst::u32 cmd = cmd_max_block_address;
			symbol_stream<Poco::Net::StreamSocket> symbols(get_socket());
			symbols.start_buffering();
			symbols.write(id);
			symbols.write(cmd);
			symbols.flush_buffer();
			symbols.read(ma);
			nst::u32 r =0;
			symbols.read(r);		
			rmsg(r,"max block address");
		}catch(Poco::Exception &e){
			comms_failure(e);
		}
		return ma;
	}

	/// this class implements a version of PAXOS which 
	/// combines multiple promises into a single operation
	/// promises are kept as 'locks' each version is defined by
	/// a orderable uuid
	class client_allocator_paxos_impl : public client_allocator{
	private:
		typedef std::vector<dist_info> _Infos;
		typedef std::vector<std::string> _Nodes;
		struct red_pair{
			red_pair() : started(false),node(nullptr){
			}
			red_pair(dist_info info) : started(false),node(nullptr){
				this->set_info(info);
			}
			~red_pair(){
				
			}
			bool close(){
				return get_node().close();
			}
			bool is_started() const {
				return this->started;
			}
			void set_info(dist_info info){
				this->info = info;
			}
			const dist_info& get_info(){
				return this->info;
			}
			
			void open(std::string name){			
				
				this->name = name;
								
			}
			
			bool contains(address_type address) {
				return get_node().contains(address);
			}
			
			bool get_version(address_type address, version_type& version) {
				return get_node().get_version(address,version);
			}

			bool store(version_type &failed, address_type address, version_type version, const block_type &data){
				return get_node().store(failed, address, version, data);
			}
			
			bool get(address_type address, version_type version, block_type& data){
				return get_node().get(address,version,data);
			}

			bool get(address_type address, version_type version, version_type &ov, block_type& data){
				return get_node().get(address,version,ov,data);
			}

			bool begin(const version_type& version, bool writer){
				bool r = false;
				if(!get_node().is_open()){
					node->open(name);
				}
				if(!get_node().is_open()){
					/// store 'node down' fact - for monitoring
					return r;
				}
				if(!started)
					r = get_node().begin(version, writer);
				started = true;
				return r;
			}
			bool commit(){
				bool r = false;
				if(started){
					r = get_node().commit();
				}
				started = false;
				return r;
			}
			bool rollback(){
				bool r = false;
				if(started){
					r = get_node().rollback();
				}
				/// if r == 0 ?
				started = false;
				return r;
			}
			bool unlock_all(){
				bool r = false;
				if(started){
					r = get_node().unlock_all();
				}
				/// if r == 0 ?
				started = false;
				return r;
			}
			client_allocator_connection &get_node(){
				if(node==nullptr){
					node = std::make_shared<client_allocator_connection>(info.node);
				}
				return *(node.get());
			}
			address_type max_block_address(){
				return get_node().max_block_address();
			}
		private:
			bool started;
			std::string name;
			dist_info info;
			std::shared_ptr<client_allocator_connection> node;
		};
		typedef std::vector<red_pair> _InfoNodes;
		typedef rabbit::unordered_map<version_type,size_t> _VersionsInvolved;
	private:
		rednodes resources;
		_InfoNodes connections;
		bool writer;
		version_type version;
		_VersionsInvolved involved_versions;
	public:
		client_allocator_paxos_impl() : writer(false){
		}
		virtual ~client_allocator_paxos_impl(){
		}
		bool matches(std::string pattern, std::string val){
			Poco::Glob matcher(pattern);
			return matcher.match(val);				
		}
		
		bool open(const std::string &name) {
			resources.open();/// connects to dht

			_Nodes nodes = resources.get_nodes();
			/// ask for coll info list can be fairly long - 200000+ on large systems
			_Infos cols = resources.get_colinfos();
			connections.reserve(cols.size());
			size_t con = 0;
			for(_Infos::iterator i = cols.begin(); i!= cols.end(); ++i){
				if(matches((*i).name, name)){ /// filters the nodes with col name 
					connections.push_back(red_pair((*i)));					
					connections.back().open(name);/// all nodes concerned are opened which may be many
					++con;
				}
			}
			red_println("[SERVER]  client allocator found %lld nodes - local [%s]",connections.size(),treestore_red_address);			
			return !connections.empty();
					
		};		
		bool contains(address_type address) {
			if(this->version == version_type()){			
				red_error("[SERVER] transaction not started for write");
			}
			if(!this->writer){
				red_error("[SERVER]  transaction is read only");
			}
			size_t stored = 0;
			for(_InfoNodes::iterator i = connections.begin(); i!= connections.end(); ++i){
				const dist_info &dist = (*i).get_info();
				if(address >= dist.start_row && address <= dist.end_row){
					
					(*i).begin(this->version,this->writer);
					
					if((*i).contains(address)){
						++stored;
					}
				}
			}
			return stored > 0;
		}

		bool get_version(address_type address, version_type& version) {
			if(this->version == version_type()){			
				red_error("[SERVER] transaction not started for write");
			}
			if(!this->writer){
				red_error("[SERVER]  transaction is read only");
			}
			size_t stored = 0;
			size_t involved = 0;
			involved_versions.clear();
			for(_InfoNodes::iterator i = connections.begin(); i!= connections.end(); ++i){
				const dist_info &dist = (*i).get_info();
				if(address >= dist.start_row && address <= dist.end_row){
					++involved;
					(*i).begin(this->version,this->writer);					
					if((*i).get_version(address,version)){
						involved_versions[version]++;
						++stored;
					}
				}
			}
			/// choose highest most popular version which has more than n/2 instances
			/// if the highest version is not the most popular then return an error 
			/// state(false)
			return stored > (involved/2);
		}

		/// in this implementation the PAXOS promise is combined with data
		/// for a little bit better efficiency. Not all nodes need to be 
		/// considered since some of them are not involved with some
		/// addresses
		bool store(address_type address, const block_type &data){
			if(this->version == version_type()){			
				red_error("[SERVER] transaction not started for write");
				return false;
			}
			if(!this->writer){
				red_error("[SERVER]  transaction is read only");
				return false;
			}
			size_t stored = 0;			
			size_t involved = 0;
			bool version_order_issue = false;
			version_type failed;
			_InfoNodes failed_connections;
			_InfoNodes success_connections;
			for(_InfoNodes::iterator i = connections.begin(); i!= connections.end(); ++i){
				const dist_info &dist = (*i).get_info();
				if(address >= dist.start_row && address <= dist.end_row){
					++involved;
					(*i).begin(this->version,this->writer);
					/// store/promise can fail because a conflicting promise
					/// has been made already or the node is down
					
					if((*i).store(failed,address,this->version,data)){
						++stored;
						success_connections.push_back((*i));
					}else if(this->version < failed){
						
						/// this means the operation cannot be retried
						/// so unlock other successfull nodes giving
						/// the winner the stage
						version_order_issue = true;
						stored = 0;
						break;
					}else{
						failed_connections.push_back((*i));
					}
				}
			}
			if(version_order_issue){
				/// unlock all previously success full nodes to avoid algorithm never completing globally
				for(_InfoNodes::iterator i = success_connections.begin(); i!= success_connections.end(); ++i){
					(*i).unlock_all();					
				}
			}else if(stored <= (involved/2)){
				/// retry failed if the where no ordering issues
				for(_InfoNodes::iterator i = failed_connections.begin(); i!= failed_connections.end(); ++i){
					(*i).begin(this->version,this->writer);
					/// store/promise can fail because a conflicting promise
					/// has been made already or the node is down
					
					if((*i).store(failed,address,this->version,data)){
						++stored;
					}
				}
			}
			/// > (involved-1)/2 copies must be stored or the promise has failed
			
			return stored > (involved/2);
		};
		/// the PAXOS retrieve
		bool get(address_type address, block_type& data){
			if(this->version==version_type()){			
				red_error("[SERVER] transaction not started for read");
				return false;
			}
			size_t ok = 0;
			size_t involved = 0;
			for(_InfoNodes::iterator i = connections.begin(); i!= connections.end(); ++i){
				const dist_info &dist = (*i).get_info();
				if(address >= dist.start_row && address <= dist.end_row){
					++involved;
					(*i).begin(this->version,this->writer);

					if((*i).get(address,this->version,data)){
						++ok;
					}
				}
			}
			/// > (involved-1)/2 copies of the latest version must be retrieved or the operation failed

			return ok > (involved/2);
		};
		bool get(address_type address, version_type& ov, block_type& data){
			if(this->version==version_type()){			
				red_error("[SERVER] transaction not started for read");
				return false;
			}
			size_t ok = 0;
			size_t involved = 0;
			for(_InfoNodes::iterator i = connections.begin(); i!= connections.end(); ++i){
				const dist_info &dist = (*i).get_info();
				if(address >= dist.start_row && address <= dist.end_row){
					++involved;
					(*i).begin(this->version,this->writer);

					if((*i).get(address,this->version, ov, data)){
						++ok;
					}
				}
			}
			/// > (involved-1)/2 copies of the latest version must be retrieved or the operation failed
			return ok > (involved/2);
		};
		bool begin(bool writer){
			this->writer = writer;
			/// versions are orderable in time and globally unique
			this->version = Poco::UUIDGenerator::defaultGenerator().create();
			return true;
		};
		bool commit(){
			size_t ok = 0;
			size_t involved = 0;
			for(_InfoNodes::iterator i = connections.begin(); i!= connections.end(); ++i){					
				if((*i).is_started()){
					++involved;
					if((*i).commit()) ++ok;
				}
			}
			return ok > 0;
		};
		bool rollback(){
			size_t ok = 0;
			size_t involved = 0;
			for(_InfoNodes::iterator i = connections.begin(); i!= connections.end(); ++i){					
				if((*i).is_started()){
					++involved;
					if((*i).rollback()) ++ok;
				}
			}
			return ok > 0;
		};		
		bool unlock_all(){
			size_t ok = 0;
			size_t involved = 0;
			for(_InfoNodes::iterator i = connections.begin(); i!= connections.end(); ++i){					
				(*i).begin(this->version,this->writer);

				if((*i).unlock_all()) ++ok;
				
			}
			return ok > 0;
		};		
		bool close(){
			size_t ok = 0;
			size_t involved = 0;
			for(_InfoNodes::iterator i = connections.begin(); i!= connections.end(); ++i){					
				
				if((*i).close()) ++ok;
				
			}
			return ok > 0;
		};		
		address_type max_block_address() {
			address_type r = 0;
			size_t ok = 0;
			size_t involved = 0;
			for(_InfoNodes::iterator i = connections.begin(); i!= connections.end(); ++i){				
				(*i).begin(this->version,this->writer);
				++involved;
				r = std::max<address_type>(r,(*i).max_block_address());				
			}
			return r;
		}
		/// must be thread safe
		bool is_open() const {
			return !connections.empty();
		};
		
	};
	client_allocator* create_allocator(){
		return new client_allocator_paxos_impl();
	}
	void destroy_allocator(client_allocator* alloc){
		delete dynamic_cast<client_allocator_paxos_impl*>(alloc);
	}
	class red_server_worker;
	class red_block_server{
	public:
		red_block_server(StreamSocket& socket, SocketReactor& reactor);
		
      
		~red_block_server()
		{
          
			red_println("[SERVER] leaving red node listener");
		
			_reactor.removeEventHandler(_socket, NObserver<red_block_server, ReadableNotification>(*this, &red_block_server::onReadable));
			_reactor.removeEventHandler(_socket, NObserver<red_block_server, ShutdownNotification>(*this, &red_block_server::onShutdown));         
		}

		void onReadable(const AutoPtr<ReadableNotification>& pNf);
      
		void onShutdown(const AutoPtr<ShutdownNotification>& pNf)
		{
			
			delete this;
		}
		void onWorkerExit(){
			--workersActive;
		}
		StreamSocket& getSocket(){
			return _socket;
		}
		_Versions &get_versions(){
			return versions;
		}
		const _Versions &get_versions() const {
			return versions;
		}
	private:
      
      
		StreamSocket			_socket;
		SocketReactor&			_reactor;
		std::atomic<nst::u32>	workersActive;
		//rednodes				_nodes;
		nst::u64				wid;
		_Versions				versions;
	};

	class red_server_state{
	public:
		typedef std::shared_ptr<block_type> block_type_ptr;
		typedef rabbit::unordered_map<address_type, block_type_ptr> _BufferMap;
	private:		
		size_t				invalid_versions;
		version_type		writer_version;
		_BufferMap			buffers;
		red_block_server	*server;
		block_type			_buffer;
		_RedAlloc			*alloc;
		nst::u64			id;
		bool				transacted;
	public:
		_Versions& get_versions(){
			return server->get_versions();
		}
		const _Versions& get_versions() const {
			return server->get_versions();
		}
		version_type get_version(address_type address) const {
			if(has_version(address)){
				return get_versions().at(address);
			}
			return version_type();
		}
		bool has_version(address_type address){
			return get_versions().count(address) != 0;
		}
		bool has_version(address_type address) const {
			return get_versions().count(address) != 0;
		}
		void replace_version(address_type address, version_type version){
			get_versions()[address] = version;
		}
		void erase_version(address_type address){
			get_versions().erase(address);
		}
		void store_version(address_type address, version_type version){
			if(!has_version(address)){
				get_versions()[address] = version;
			}
		}
		void unlock(address_type address){
			if(has_version(address)){
				if(!alloc->unlock_version(address,get_version(address))){
					red_error("[SERVER] version could not be unlocked");
				}
				erase_version(address);
			}
		}
		void unlock_all(){
			std::vector<address_type> addresses;
			for(auto b = get_versions().begin(); b != get_versions().end(); ++b){
				addresses.push_back(b->first);				
			}
			red_println("[SERVER] unlocking %lld addresses",addresses.size());
			for(auto a = addresses.begin(); a != addresses.end(); ++a){
				unlock(*a);
			}
			
		}
	public:
		bool has_buffer(address_type address) const {
			return buffers.count(address) != 0;
		}
		block_type& get_buffer(address_type address){
			block_type_ptr& bp = buffers[address];
			if(bp == nullptr){
				bp = std::make_shared<block_type>();
			}
			return *(bp.get());
		}
		void clear(){
		
		}
		
		bool process(){
			bool result = true;
			symbol_stream<StreamSocket> symbols(server->getSocket());			
			symbols.start_buffering();
			/// do one operation and exit 
			nst::u32 cmd = 0;
			nst::u32 r = 0;
			try	{
				
				if(!symbols.read(cmd)){
					red_error("[SERVER] [%lld]  invalid command",this->id);
					return false;
				}
				if(cmd == cmd_close){
					red_println("[SERVER] [%lld] closing red store",this->id);
					result = false;
				}
				if(cmd == cmd_open){
					std::string name;
					symbols.read(name);
					red_println("[SERVER] [%lld] opening red store %s",this->id,name.c_str());
					alloc = get_red_store(name);
				}
				if(alloc==nullptr){
					red_error("[SERVER] [%lld] invalid command sequence (not opened)",this->id);
					return false;
				}
				if(cmd == cmd_get){
					red_println("[SERVER] [%lld] received get",this->id);
					nst::u64 address = 0;
					version_type version = version_type();
					if(!symbols.read(address)) {
						red_error("[SERVER] [%lld] invalid address",this->id);
						return false;
					}
					if(!symbols.read(version)) {
						red_error("[SERVER] [%lld] invalid version",this->id);
						return false;
					}
					if(has_buffer(address)){						
						symbols.write(get_buffer(address));
					}else{
						_buffer.clear();
						version_type test = has_version(address) ? get_version(address) : alloc->get(address);
						if(alloc->lock_version(address, test)){						
							version_type test = alloc->get(address, _buffer);
							if(test!=version_type()){
								store_version(address,test);							
							}
						}else{
							red_println("[SERVER] [%lld] could not read lock %lld",this->id,address);
							++(this->invalid_versions);
							r = err_version_locked;
						}
					
						symbols.write(_buffer);						
						
					}
					symbols.write(get_version(address));
					red_println("[SERVER] [%lld] got %lld",this->id,address);
				}else if(cmd == cmd_set){
					red_println("[SERVER] [%lld] received set",this->id);
					nst::u32 ml = 0;
					nst::u64 address = 0;
					version_type version = version_type();
					if(!symbols.read(address)){
						red_error("[SERVER] [%lld] invalid address",this->id);
						return false;
				
					}
					if(!symbols.read(version)) {
						red_error("[SERVER] [%lld] invalid version",this->id);
						return false;
				
					}
					if(!symbols.read(_buffer)) {
						red_error("[SERVER] [%lld] invalid buffer",this->id);
						return false;				
					}
					ml = (stx::storage::u32) _buffer.size();
					version_type result_version = this->writer_version;
					if(this->writer_version != version){
						red_println("[SERVER] [%lld] version mismatch %s!=%s",this->id,this->writer_version.toString().c_str(),version.toString().c_str());
						
						r = err_version_mismatch;
					}else{
						if(alloc->lock_version(address, this->writer_version, get_version(address))){
							replace_version(address, this->writer_version);												
							get_buffer(address) = _buffer;
						}else{
							++(this->invalid_versions);
							result_version = alloc->get(address);
							red_println("[SERVER] [%lld] writer address locked %lld",this->id,(address_type)address);
							r = err_version_locked;
						}
					}
					//alloc->set(address, version, _buffer);
					symbols.write(result_version);
					red_println("[SERVER] [%lld] received [%ld] bytes",this->id,ml);
				}else if(cmd == cmd_version){
					address_type address = 0;
					symbols.read(address);
					version_type version = has_version(address) ? get_version(address) : alloc->get(address);					
					symbols.write(version);
					r = version == version_type() ? err_address_does_not_exist : 0;
				}else if(cmd == cmd_contains){
					address_type address = 0;
					symbols.read(address);
					if(alloc->contains(address)){
						r = 0;
					}else{
						r = err_address_does_not_exist;
					}					
				}else if(cmd == cmd_begin){
					red_println("[SERVER] [%lld] received begin",this->id);
					bool writer = false;
					version_type version;
					symbols.read(writer);
					symbols.read(version);
					this->writer_version = version;
					this->invalid_versions = 0;
					alloc->begin();
					transacted = true;
				}else if(cmd == cmd_commit){
					red_println("[SERVER] [%lld] received commit",this->id);
					//synchronized context(alloc->get_lock());
					if(this->invalid_versions == 0){
						alloc->begin();
						for(auto b = buffers.begin(); b != buffers.end(); ++b){
							alloc->set(b->first,this->writer_version,*(b->second.get()));
						}
						red_println("[SERVER] [%lld] start commit",this->id);
						alloc->commit();
						red_println("[SERVER] [%lld] complete commit",this->id);
				
					}else{				
						red_println("[SERVER] [%lld] received invalid versions ->rollback",this->id);
					
						r = err_versions;
					}
			
					buffers.clear();
				
				}else if(cmd == cmd_checkpoint){
					red_println("[SERVER] [%lld] received check point",this->id);
					//synchronized context(alloc->get_lock());
					if(this->invalid_versions == 0){
						for(auto b = buffers.begin(); b != buffers.end(); ++b){
							alloc->set(b->first,this->writer_version,*(b->second.get()));
						}					
					}else{				
						red_println("[SERVER] [%lld] received invalid versions ->rollback",this->id);
					
						r = err_versions;
					}
			
					buffers.clear();				
				}else if(cmd == cmd_rollback){
					red_println("[SERVER] [%lld] received rollback",this->id);
					buffers.clear();			
					transacted = false;
				}else if(cmd == cmd_evict){
					red_println("[SERVER] [%lld] received evict ",this->id);
				}else if(cmd == cmd_unlock_all){
					/// if transacted dont do notin inf fact rollback afterwards
					red_println("[SERVER] [%lld] received unlock all",this->id);
					unlock_all();
					r = 0;				
				}else if(cmd == cmd_max_block_address){
					red_println("[SERVER] [%lld] received cmd max block address",this->id);
					address_type max_address = alloc->max_block_address();
					symbols.write(max_address);
				}
				red_println("[SERVER] [%lld] there is %lld lock(s) ",this->id,(address_type)get_versions().size());
						
				symbols.write(r);
				symbols.flush_buffer();

			}catch (Poco::Exception& e)	{
				red_error("[SERVER] Could not process storage instruction - %s",e.name());
			}
			
			
			return result;
		}
		
		red_server_state(red_block_server* server, nst::u64 id)
		:	server(server)
		,	id(id)
		,	transacted(false){
			
			invalid_versions = 0;		
		}
	};
	typedef std::shared_ptr<red_server_state> red_server_state_ptr;

	class red_server_worker : public asynchronous::AbstractWorker{
	protected:
	protected:
		typedef rabbit::unordered_map<nst::u64,red_server_state_ptr> _States;
	protected:
		red_block_server	*server;
		_States				states;
	public:
		red_server_worker(red_block_server* server) : server(server){
			
		}
		virtual void work(){
			symbol_stream<Poco::Net::StreamSocket> symbols(server->getSocket());
			nst::u64 id = 0;
			red_server_state_ptr processor;
			while(true){
				if(!symbols.read(id)){
					red_error("[SERVER] invalid id");
					break;
				}
				if(!states.get(id, processor)){
					processor = std::make_shared<red_server_state>(server,id);	
					states[id] = processor;
				}
				if(!processor->process()){
					states.erase(id);
				}
				processor = nullptr;

			}
			
			server->onWorkerExit();
			
			///red_error("[SERVER] operation could not be completed");
			
			//}
		}
		virtual ~red_server_worker(){			
		}
	};

	red_block_server::red_block_server(StreamSocket& socket, SocketReactor& reactor)
	:	_socket(socket)
	,	_reactor(reactor)
	{
		red_println("[SERVER]  starting red node listener");
		wid = ++connection_ids; /// assigns all comms to this server on one thread
		std::string proto;
			
		symbol_stream<Poco::Net::StreamSocket> symbols(_socket);
		symbols.read(proto,PROTO_NAME.size());
		if(PROTO_NAME==proto){
			++workersActive;	
				
			red_workers::get_threads((*this).wid).add(new red_server_worker(this));
		}else{
			red_error("[SERVER] disconnected invalid client");
			//delete this;
		}
		//_reactor.addEventHandler(_socket, NObserver<red_block_server, ReadableNotification>(*this, &red_block_server::onReadable));
		_reactor.addEventHandler(_socket, NObserver<red_block_server, ShutdownNotification>(*this, &red_block_server::onShutdown));
	}
	void red_block_server::onReadable(const AutoPtr<ReadableNotification>& pNf){
		
		if(workersActive==0){
			
			
		}
		///printf("received idle chatter\n");
        
	///delete this;
		  
	}
class node_block : public Poco::Runnable{
	public:
	void run(){		
		typedef std::vector<std::string> _Nodes;
		unsigned short port = 8989; //(unsigned short) config().getInt("red.port", 9977);
		
		

		rednodes resources;
		resources.open();
		
        // set-up a server socket
        ServerSocket server(port);
        // set-up a SocketReactor...
        SocketReactor reactor;
        // ... and a SocketAcceptor
        SocketAcceptor<red_block_server> acceptor(server, reactor);
        // run the reactor in its own thread so that we can wait for 
        // a termination request
        Thread thread;
        thread.start(reactor);
		// start the node in the dht
		if(resources.has_node(treestore_red_address)){		
			resources.start_node(treestore_red_address);
			resources.set_node_space(treestore_red_address);
		}else{
			resources.add_node(treestore_red_address);
		}
		resources.set_node_status(treestore_red_address, "started");
        // wait for CTRL-C or kill
        while(Poco::Thread::current()->isRunning()){
			Poco::Thread::sleep(100);
			
		}
        // Stop the SocketReactor
        reactor.stop();
        thread.join();
	}
		
};
void red_tests(){
	int start ;
	std::cout << "enter a value" << std::endl;
	std::cin >> start;
	
	block_type data;					
		
	client_allocator_paxos_impl red,red1;
	red.open("mydata");
	red1.open("mydata");
	std::cout<< "start test " << std::endl;
	for(nst::u32 t = 0; t  < 2000;++t){
			
		red_println("Test Iteration %ld started",t);
		red.begin(true);			
		red1.begin(true);
		red.get(15,data);
		red.store(15,data);		
		red1.get(15,data);
		red1.store(15,data);		
		red1.commit();
		red.commit();			
		red.begin(false);
		red1.begin(false);
		red1.unlock_all();
		red.unlock_all();
		if(t %100 == 0){
			printf("Test Iteration %ld finished\n",t);
		}
	}
			
	red_println("Test Iterations complete ");
	std::cout<< "end test " << std::endl;
}
class node_chat : public Poco::Runnable{
public:
	void run(){	
		typedef std::vector<std::string> _Nodes;
		red_println("[SERVER] waiting for server");
		
		rednodes resources;
		resources.open();

		_Nodes nodes = resources.get_nodes();
		red_println("[SERVER] found %lld nodes - local [%s]",nodes.size(),treestore_red_address);
		//red_tests();
		while(Poco::Thread::current()->isRunning()){			
			::Sleep(100);
		}

	}	
};

static node_block block;
static node_chat chat;
static Poco::Thread server_thread("red:store_thread");
static Poco::Thread chat_thread("red:chat_thread");

static void start(){
	try{
		//red_println("Starting red storage thread... ");
		//server_thread.start(block);
		//chat_thread.start(chat);
	}catch(Poco::Exception &e){
		red_error("Could not start red storage thread : %s",e.name());
	}
}

};

void start_red(const nst::u64& id){

	red::start();
	//node_block::start();
	//node_chat::start(id);
}
