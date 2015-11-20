namespace NS_STORAGE = stx::storage;
#include <cassandra.h>
#include <string>
namespace blanca{
	class cass_env{
	private:
		CassCluster*		cluster ;
		CassSession*		session ;
	protected:
		
		std::string			contact_points;
		CassError connect_session() {
			CassError rc = CASS_OK;
			cass_cluster_set_contact_points(cluster, contact_points.c_str());
			//cass_cluster_set_port(cluster, 9160);
			CassFuture* future = cass_session_connect(session, cluster);

			cass_future_wait(future);
			rc = cass_future_error_code(future);
			if (rc != CASS_OK) {
				print_error(future);
			}
			cass_future_free(future);

			return rc;
		}
		CassSession* get_session(){
			return this->session;
		}
		const CassSession* get_session() const {
			return this->session;
		}
		std::string get_string(const CassRow* cas_row, int col){
			const char* str;
			size_t str_size = 0;				
			std::string result;
			CassError rc = cass_value_get_string(cass_row_get_column(cas_row, col),&str,&str_size);
			if(rc==CASS_OK){
				result.reserve(str_size);
				result = str;
				
			}
			return result;
		}
		template<typename _VectorType>
		_VectorType get_vector(const CassRow* cass_row, int col){
			const cass_byte_t* bytes;
			size_t bytes_size = 0;				
			_VectorType result;
			CassError rc = cass_value_get_bytes(cass_row_get_column(cass_row, col),&bytes,&bytes_size);
			if(rc==CASS_OK){
				result.reserve(bytes_size);
				for(size_t t = 0; t < bytes_size;++t){
					result.push_back((typename _VectorType::value_type)(bytes[t]));
				}								
			}
			return result;
		}
		double get_double(const CassRow* cas_row, int col){
			double r = 0.0;
			cass_double_t cas_val;
			CassError rc = cass_value_get_double(cass_row_get_column(cas_row, col), &cas_val);
			if(rc == CASS_OK){
				r = cas_val;
			}else{
				printf("[TS] [DHT] get value failed\n");
			}
			return r;
		}
		nst::u64 get_u64(const CassRow* cass_row, int col){
			nst::u64 r = 0;
			cass_int64_t cass_val;
			CassError rc = cass_value_get_int64(cass_row_get_column(cass_row, col), &cass_val);
			if(rc == CASS_OK){
				r = cass_val;
			}else{
				printf("[TS] [DHT] get int 64 value failed\n");
			}
			return r;
		}
		nst::u32 get_u32(const CassRow* cass_row, int col){
			nst::u32 r = 0;
			cass_int32_t cass_val;
			CassError rc = cass_value_get_int32(cass_row_get_column(cass_row, col), &cass_val);
			if(rc == CASS_OK){
				r = cass_val;
			}else{
				printf("[TS] [DHT] get int 32 value failed\n");
			}
			return r;
		}

	public:
		cass_env() : cluster(NULL),session(NULL){
		}
		void close(){
					
			if(session!=NULL)
				cass_session_free(session);
			if(cluster!=NULL)
				cass_cluster_free(cluster);
			cluster = NULL;
			session = NULL;
			contact_points.clear();
		}

		bool is_open() const {
			return session != NULL;
		}
		
		bool open(std::string contact_points){
			cluster = cass_cluster_new();
			session = cass_session_new();
			this->contact_points = contact_points;
			if (connect_session() != CASS_OK) {
				close();
			}
			return is_open();
		}

		void print_error(CassFuture* future) {
			const char* message;
			size_t message_length;
			cass_future_error_message(future, &message, &message_length);
			fprintf(stderr, "[TS] [DHT] [ERROR] %.*s\n", (int)message_length, message);
		}
				
		
		CassError prepare_statement(std::string query, const CassPrepared** prepared) {
			CassError rc = CASS_OK;
			CassFuture* future = NULL;
	
			future = cass_session_prepare(session, query.c_str());
			cass_future_wait(future);

			rc = cass_future_error_code(future);
			if (rc != CASS_OK) {
				print_error(future);
			} else {
				*prepared = cass_future_get_prepared(future);
			}

			cass_future_free(future);

			return rc;
		}
		CassError execute_query(const std::string &query) {
			return execute_query(query.c_str());
		}
		/// excute query on the current key space and session
		CassError execute_query(const char* query) {
			CassError rc = CASS_OK;
			CassFuture* future = NULL;
			CassStatement* statement = cass_statement_new(query, 0);		
			future = cass_session_execute(session, statement);
			cass_future_wait(future);

			rc = cass_future_error_code(future);
			if (rc != CASS_OK) {
				print_error(future);
			}

			cass_future_free(future);
			cass_statement_free(statement);

			return rc;
		}
	};
	class dht_prepared{
	private:
		std::string query;
		CassSession* session ;
		const CassPrepared* prepared;
		CassFuture* result_future ;
		CassStatement* statement;
		
		void print_error(CassFuture* future) {
			const char* message;
			size_t message_length;
			cass_future_error_message(future, &message, &message_length);
			fprintf(stderr, "[TS] [DHT] [ERROR] [%s] [%.*s]\n", query.c_str(), (int)message_length, message);
		}

		CassError prepare_statement(const CassPrepared** prepared) {
			
			CassError rc = CASS_OK;
			if(session==NULL) {
				printf("[TS] [DHT] [ERROR] session not available\n");
				return rc;
			}

			CassFuture* future = NULL;
			
			future = cass_session_prepare(session, this->query.c_str());
			cass_future_wait(future);

			rc = cass_future_error_code(future);
			if (rc != CASS_OK) {
				print_error(future);
			} else {
				*prepared = cass_future_get_prepared(future);
			}

			cass_future_free(future);

			return rc;
		}
		void bind_statement(){
			if(statement == NULL){
				if(this->prepared != NULL){
					statement = cass_prepared_bind(this->prepared);
				}else{
					printf("[TS] [ERROR] [DHT] statement not prepared\n");
				}
			}
		}
		void prepare(std::string query){
			this->query = query;
			
			if(CASS_OK != prepare_statement(&(this->prepared))){
				close();
				return;
			}
		}
	public:
		// is it prepared
		bool is_prepared(){
			return this->prepared != NULL;
		}
		std::string get_query(){
			return query;
		}
		
		dht_prepared() : session(NULL), statement(NULL), prepared(NULL), result_future(NULL){
		}
		
		~dht_prepared(){
			close();
		}

		void prepare(CassSession*session, std::string query){
			this->set_session(session);
			prepare(query);
		}
	
		void set_session(CassSession*session){
			this->session = session;
			
		}
		void close(){
			if(this->statement)
				cass_statement_free(this->statement);
			this->statement = NULL;
			if(this->prepared)
				cass_prepared_free(this->prepared);
			this->prepared = NULL;
		}
		
		void bind_i32( size_t index, cass_int32_t value){
			bind_statement();
			if(statement!=NULL)
				cass_statement_bind_int32(statement, index, value);
			else
				printf("[TS] [ERROR] [DHT] storage not available\n");
		}
		
		void bind_i64( size_t index, cass_int64_t value){
			bind_statement();
			if(statement!=NULL)
				cass_statement_bind_int64(statement, index, value);
			else
				printf("[TS] [ERROR] [DHT] storage not available\n");
		}

		void bind( size_t index, const cass_byte_t* value, size_t value_size){
			bind_statement();
			if(statement!=NULL)
				cass_statement_bind_bytes(statement, index, value, value_size);				
			else
				printf("[TS] [ERROR] [DHT] storage not available\n");
		}
		void bind( size_t index, std::string value){
			bind_statement();
			if(statement!=NULL)
				cass_statement_bind_string(statement, index, value.c_str());
			else
				printf("[TS] [ERROR] [DHT] storage not available\n");

		}
		CassError execute(){
			bind_statement();
			CassError rc = CASS_OK;			
			if(statement==NULL) return CASS_ERROR_SERVER_UNPREPARED;
						
			result_future = cass_session_execute(session, statement);
			if(result_future != NULL){
				cass_future_wait(result_future);
				rc = cass_future_error_code(result_future);
				if (rc != CASS_OK) {
					print_error(result_future);
				}
				
			}else{
				printf("[TS] [ERROR] [DHT] not set future \n");
			}				
			
			return rc;
		}
		
		CassFuture* get_result_future () {
			return this->result_future;
		}
		CassStatement* get_statement(){
			return this->statement;
		}
		const CassResult* get_result(){
			if(this->result_future)
				return cass_future_get_result(this->result_future);
			return NULL;
		}
		
		void complete(){
			
			if(this->result_future)
				cass_future_free(this->result_future);
			if(this->statement)
				cass_statement_free(this->statement);
			this->statement = NULL;
			this->result_future = NULL;
		}
	};
};
template<class block_type>
class cassandras{
public:
	

	class dht_prepared{
	private:
		std::string query;
		CassSession* session ;
		const CassPrepared* prepared;
		CassFuture* result_future ;
		CassStatement* statement;
		
		void print_error(CassFuture* future) {
			const char* message;
			size_t message_length;
			cass_future_error_message(future, &message, &message_length);
			fprintf(stderr, "[TS] [DHT] [ERROR] %.*s\n", (int)message_length, message);
		}

		CassError prepare_statement(const CassPrepared** prepared) {
			CassError rc = CASS_OK;
			CassFuture* future = NULL;
			
			future = cass_session_prepare(session, this->query.c_str());
			cass_future_wait(future);

			rc = cass_future_error_code(future);
			if (rc != CASS_OK) {
				print_error(future);
			} else {
				*prepared = cass_future_get_prepared(future);
			}

			cass_future_free(future);

			return rc;
		}
		void bind_statement(){
			if(statement == NULL){
				if(this->prepared != NULL){
					statement = cass_prepared_bind(this->prepared);
				}else{
					printf("[TS] [ERROR] [DHT] statement not prepared\n");
				}
			}
		}
	public:
		
		std::string get_query(){
			return query;
		}
		
		dht_prepared() : session(NULL), statement(NULL), prepared(NULL), result_future(NULL){
		}
		
		~dht_prepared(){
			close();
		}

		void prepare(CassSession*session, std::string query){
			this->set_session(session);
			prepare(query);
		}
		void prepare(std::string query){
			this->query = query;
			
			if(CASS_OK != prepare_statement(&(this->prepared))){
				close();
				return;
			}
		}
		void set_session(CassSession*session){
			this->session = session;
			
		}
		void close(){
			if(this->statement)
				cass_statement_free(this->statement);
			this->statement = NULL;
			if(this->prepared)
				cass_prepared_free(this->prepared);
			this->prepared = NULL;
		}
		
		void bind_i32( size_t index, cass_int32_t value){
			bind_statement();
			if(statement!=NULL)
				cass_statement_bind_int32(statement, index, value);
			else
				printf("[TS] [ERROR] [DHT] storage not available\n");
		}
		
		void bind_i64( size_t index, cass_int64_t value){
			bind_statement();
			if(statement!=NULL)
				cass_statement_bind_int64(statement, index, value);
			else
				printf("[TS] [ERROR] [DHT] storage not available\n");
		}

		void bind( size_t index, const cass_byte_t* value, size_t value_size){
			bind_statement();
			if(statement!=NULL)
				cass_statement_bind_bytes(statement, index, value, value_size);				
			else
				printf("[TS] [ERROR] [DHT] storage not available\n");
		}
		void bind( size_t index, std::string value){
			bind_statement();
			if(statement!=NULL)
				cass_statement_bind_string(statement, index, value.c_str());
			else
				printf("[TS] [ERROR] [DHT] storage not available\n");

		}
		CassError execute(){
			bind_statement();
			CassError rc = CASS_OK;			
			if(statement==NULL) return CASS_ERROR_SERVER_UNPREPARED;
						
			result_future = cass_session_execute(session, statement);
			if(result_future != NULL){
				cass_future_wait(result_future);
				rc = cass_future_error_code(result_future);
				if (rc != CASS_OK) {
					print_error(result_future);
				}
				
			}else{
				printf("[TS] [ERROR] [DHT] not set future \n");
			}				
			
			return rc;
		}
		
		CassFuture* get_result_future () {
			return this->result_future;
		}
		CassStatement* get_statement(){
			return this->statement;
		}
		const CassResult* get_result(){
			if(this->result_future)
				return cass_future_get_result(this->result_future);
			return NULL;
		}
		
		void complete(){
			
			if(this->result_future)
				cass_future_free(this->result_future);
			if(this->statement)
				cass_statement_free(this->statement);
			this->statement = NULL;
			this->result_future = NULL;
		}
	};
	typedef nst::u64 stream_address;	
	
	CassCluster*		cluster ;
	CassSession*		session ;
	std::string			cas_keyspace;
	std::string			cas_table_name;
	std::string			insert_query;
	std::string			retrieve_query;
	std::string			retrieve_version_query;
	std::string			update_query;
	std::string			contains_query;
	std::string			update_attribute;
	std::string			insert_attribute;
	std::string			contains_attribute;
	std::string			retrieve_attribute;
	
	stream_address		max_block;
	
	dht_prepared		update_statement;
	dht_prepared		add_statement;
	dht_prepared		get_statement;	
	dht_prepared		get_version_statement;
	dht_prepared		contains_statement;
	dht_prepared		update_attr_statement;
	dht_prepared		add_attr_statement;
	dht_prepared		get_attr_statement;
	dht_prepared		contains_attr_statement;

	CassBatch*			transaction;
	size_t				writes;
private:
	typedef std::unordered_set<std::string> _KeySpaces;
	Poco::Mutex & get_ks_lock(){
		static Poco::Mutex l;
		return l;
	}
	_KeySpaces& get_spaces(){
		static _KeySpaces spaces;
		return spaces;
	}
	bool has_space(std::string name){
		synchronized l(get_ks_lock());
		return (get_spaces().count(name) > 0);
	}
	void add_space(std::string name){
		synchronized l(get_ks_lock());
		get_spaces().insert(name);
	}
	struct My_Data_Row {
		My_Data_Row(): a1(0ll),dsize(0l),version(0ll),data(NULL),data_length(0){
		}
		cass_int64_t a1;
		cass_int32_t dsize;
		cass_int64_t version;
		const cass_byte_t* data;		
		size_t data_length;
	};

	struct Attribute_Row {		
		Attribute_Row() : name_length(0), value(0ll){
		}
		const char* name;		
		size_t name_length;
		cass_int64_t value;
		
	};
	void print_error(CassFuture* future) {
		const char* message;
		size_t message_length;
		cass_future_error_message(future, &message, &message_length);
		fprintf(stderr, "[TS] [DHT] [ERROR] %.*s\n", (int)message_length, message);
	}
				
	CassError connect_session() {
		CassError rc = CASS_OK;
		cass_cluster_set_contact_points(cluster, treestore_contact_points);
		//cass_cluster_set_port(cluster, 9160);
		CassFuture* future = cass_session_connect(session, cluster);

		cass_future_wait(future);
		rc = cass_future_error_code(future);
		if (rc != CASS_OK) {
			print_error(future);
		}
		cass_future_free(future);

		return rc;
	}
	CassError prepare_statement(std::string query, const CassPrepared** prepared) {
		CassError rc = CASS_OK;
		CassFuture* future = NULL;
	
		future = cass_session_prepare(session, query.c_str());
		cass_future_wait(future);

		rc = cass_future_error_code(future);
		if (rc != CASS_OK) {
			print_error(future);
		} else {
			*prepared = cass_future_get_prepared(future);
		}

		cass_future_free(future);

		return rc;
	}
	/// excute query on the current key space and session
	CassError execute_query(const char* query) {
		CassError rc = CASS_OK;
		CassFuture* future = NULL;
		CassStatement* statement = cass_statement_new(query, 0);		
		future = cass_session_execute(session, statement);
		cass_future_wait(future);

		rc = cass_future_error_code(future);
		if (rc != CASS_OK) {
			print_error(future);
		}

		cass_future_free(future);
		cass_statement_free(statement);

		return rc;
	}
	std::string get_max_block_name() const {
		return "__max_blk";
	}
public:
	cassandras(){
		
		this->cluster = NULL;
		this->session = NULL;
		
		this->transaction = NULL;
		this->writes = 0;
	}
	~cassandras(){
		close();
	}
	bool opened() const {
		return !cas_table_name.empty();
	}
	bool closed() const {
		return cas_table_name.empty();
	}
	stream_address max_block_address() const {
		return max_block;
	}
	void open(std::string unfiltered_name){
		if(cluster==NULL){
			
			if(strlen(treestore_contact_points) == 0){
				close();
				return;
			}
			std::string name = unfiltered_name;
			for(auto n = name.begin(); n != name.end(); ++n){
				switch((*n)){
					case '#':
					case '-':
						(*n) = 'p';
						break;
					default:
						break;
				};
			}
			cluster = cass_cluster_new();
			session = cass_session_new();
			
			if (connect_session() == CASS_OK) {
				/// check table exists or create
				Poco::StringTokenizer components(name, "\\/.");
				if(components.count() < 3){
					close();
					printf("[TS] [ERROR] could not create cassandra table %s.%s\n", cas_keyspace.c_str(), name.c_str());
					return;
				}
				
				size_t e = (components.count()-1);
				cas_table_name = components[e];
				cas_keyspace = "ts_";
				for(size_t s = 0; s < e; ++s){
					cas_keyspace += components[s] ;
					if (s < e-1)
						cas_keyspace += "_";
				}				
				std::string canon =  cas_keyspace + "." + cas_table_name ;
				std::string attributes = canon+"_attrs" ;
				printf("[TS] [INFO] creating cassandra address [%s]\n", canon.c_str());
				
				std::string qlk = "CREATE KEYSPACE " + cas_keyspace ;
				qlk += " WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' };";
				
				std::string qlt = "CREATE TABLE " + canon + " (";
				qlt += "a1 bigint , ";
				qlt += "dsize int, ";
				qlt += "version bigint, ";
				qlt += "data BLOB, PRIMARY KEY (a1));";
				std::string alt = "CREATE TABLE " + attributes + " (name text, value bigint, primary key(name));";
				if(!has_space(cas_keyspace)){
					execute_query(qlk.c_str());
					add_space(cas_keyspace);
				}
				execute_query(qlt.c_str());
				execute_query(alt.c_str());
				insert_query = "INSERT INTO  " + canon + " (a1, dsize, version, data) VALUES (?, ?, ?, ?);";
				update_query = "UPDATE "+ canon + " SET dsize = ?, data = ? WHERE a1 = ?;";
				retrieve_query = "SELECT a1,dsize,data FROM "+ canon + " WHERE a1 = ?;";
				retrieve_version_query = "SELECT a1,dsize,data FROM "+ canon + " WHERE a1 = ? AND version = ?;";
				contains_query = "SELECT count(*) FROM "+ canon + " WHERE a1 = ?;";
				update_attribute = "UPDATE " + attributes + " SET value = ? WHERE name = ?;";
				insert_attribute = "INSERT INTO  " + attributes + " (name, value) VALUES (?, ?);";
				contains_attribute = "SELECT count(*) FROM "+ attributes + " WHERE name = ?;";
				retrieve_attribute = "SELECT value FROM "+ attributes + " WHERE name = ?;";
				
				//const char* query = contains ? update_query.c_str() : insert_query.c_str();//(a1, dsize, data) VALUES (?, ?, ?)
				this->update_statement.prepare(session, update_query);
				this->add_statement.prepare(session, insert_query);					
				this->get_statement.prepare(session, retrieve_query);
				this->get_version_statement.prepare(session, retrieve_version_query);
				this->contains_statement.prepare(session, contains_query);					
				this->update_attr_statement.prepare(session, update_attribute);
				this->add_attr_statement.prepare(session, insert_attribute);					
				this->get_attr_statement.prepare(session, retrieve_attribute);					
				this->contains_attr_statement.prepare(session, contains_attribute);					
				max_block = get_attribute(get_max_block_name(),0);
			}else{				
				close();
			}
		}		

	}
	void close(){
		rollback();
		this->update_statement.close();
		this->add_statement.close();					
		this->get_statement.close();
		this->get_version_statement.close();
		this->contains_statement.close();					
		this->update_attr_statement.close();
		this->add_attr_statement.close();					
		this->get_attr_statement.close();					
		this->contains_attr_statement.close();					
		if(cluster!=NULL)
			cass_cluster_free(cluster);
		if(session!=NULL)
			cass_session_free(session);
		cas_table_name.clear();
		cas_keyspace.clear();
		cluster = NULL;
		session = NULL;
	}
	bool has_attribute(std::string name){
		CassError rc = CASS_OK;
		bool has_result = false;
		Attribute_Row attr_row;
		
		this->contains_attr_statement.bind(0, name);
		rc = this->contains_attr_statement.execute();
		
		if (rc == CASS_OK) {
			
			const CassResult* result = cass_future_get_result(this->contains_attr_statement.get_result_future());
			CassIterator* iterator = cass_iterator_from_result(result);

			if (cass_iterator_next(iterator)) {
				attr_row.value = 0;
				const CassRow* row = cass_iterator_get_row(iterator);
				cass_value_get_int64(cass_row_get_column(row, 0), &attr_row.value);
				has_result = attr_row.value > 0;				
			}

			cass_result_free(result);
			cass_iterator_free(iterator);
		}

		this->contains_attr_statement.complete();
		return has_result;
	}
	void set_attribute(std::string name, stream_address value){
		CassError rc = CASS_OK;
		Attribute_Row row;
		bool contains = has_attribute(name);
		dht_prepared * prepared = contains ? &(this->update_attr_statement) : &(this->add_attr_statement);
		
		//statement = cass_statement_new(query, 2 );
		row.name = &name[0];
		row.name_length = name.size();
		row.value = value;
		int c = 0;
		if(contains){
			prepared->bind_i64(0, row.value);
			prepared->bind(1, name);			
		}else{
			prepared->bind(0, name);
			prepared->bind_i64(1, row.value);
		}
		rc = prepared->execute();		
		prepared->complete();
	}

	stream_address get_attribute(std::string name, stream_address def){
		CassError rc = CASS_OK;
		stream_address get_result = def;
		Attribute_Row attr_row;
		
		this->get_attr_statement.bind(0, name);
		rc = this->get_attr_statement.execute();
		if (rc == CASS_OK) {
			const CassResult* result = cass_future_get_result(this->get_attr_statement.get_result_future());
			CassIterator* iterator = cass_iterator_from_result(result);

			if (cass_iterator_next(iterator)) {
				attr_row.value = 0;
				const CassRow* row = cass_iterator_get_row(iterator);
				cass_value_get_int64(cass_row_get_column(row, 0), &attr_row.value);
				get_result = attr_row.value;				
			}

			cass_result_free(result);
			cass_iterator_free(iterator);
		}		
		this->get_attr_statement.complete();
		return get_result;
	}

	void set(stream_address a, const block_type &data){
		CassError rc = CASS_OK;
		My_Data_Row row;
		bool contains = has(a);
		dht_prepared * prepared = contains ? &(this->update_statement) : &(this->add_statement);
		row.a1 = a;
		row.dsize = data.size();
		row.data = &data[0];
		row.version = 1;
		row.data_length = data.size();
		if(contains){			
			prepared->bind_i32(0, row.dsize);
			prepared->bind(1, row.data, row.data_length);
			prepared->bind_i64(2, row.a1);
		}else{
			prepared->bind_i64(0, row.a1);
			prepared->bind_i32(1, row.dsize);
			prepared->bind_i64(2, row.version);
			prepared->bind(3, row.data, row.data_length);
		}
		if(transaction){
			rc = cass_batch_add_statement(this->transaction, prepared->get_statement());
			if(rc != CASS_OK){
				printf("[TS] [ERROR] [DHT] cass not batch\n");
			}
		}else{
			rc = prepared->execute();		
		}
		this->writes++;
		prepared->complete();
		if(rc == CASS_OK && a > max_block){
			max_block = a;			
		}	
		if(this->writes % 10 == 0){
			flush();
		}
		if(this->writes % 1000 == 0){
			printf("[TS] [DHT] [INFO] completed %lld writes\n",writes);
			
		}
	}
	
	bool has(stream_address a) const {
		return const_cast<cassandras*>(this)->has(a);
	}

	bool has(stream_address a) {
		CassError rc = CASS_OK;
		bool has_result = false;
		//CassStatement* statement = NULL;
		CassFuture* future = NULL;
		
		//const char* query = contains_query.c_str();
		//statement = cass_prepared_bind(this->contains_statement);
		//statement = cass_statement_new(query, 1);

		this->contains_statement.bind_i64(0, a);

		rc = this->contains_statement.execute();
		if (rc == CASS_OK) {
			
			const CassResult* result = this->contains_statement.get_result();
			CassIterator* iterator = cass_iterator_from_result(result);

			if (cass_iterator_next(iterator)) {
				My_Data_Row row; row;
				
				const CassRow* cas_row = cass_iterator_get_row(iterator);
				row.a1 = 0;
				cass_value_get_int64(cass_row_get_column(cas_row, 0), &row.a1);

				has_result = row.a1 > 0;
				
			}

			cass_result_free(result);
			cass_iterator_free(iterator);
		}
		this->contains_statement.complete();
		
		return has_result;
	}
	bool get(block_type& data, stream_address a) const{
		return const_cast<cassandras*>(this)->get(data, a);
	}
	bool get(block_type& data, stream_address a) {
		if(a > max_block) return false;
		CassError rc = CASS_OK;
		bool get_result = false;		
		this->get_statement.bind_i64(0, a);

		rc = this->get_statement.execute();
		if (rc == CASS_OK) {

			const CassResult* result = cass_future_get_result(this->get_statement.get_result_future());
			CassIterator* iterator = cass_iterator_from_result(result);

			if (cass_iterator_next(iterator)) {
				My_Data_Row row;
				const CassRow* cas_row = cass_iterator_get_row(iterator);
				CassValueType t1 = cass_value_type(cass_row_get_column(cas_row, 0));
				CassValueType t2 = cass_value_type(cass_row_get_column(cas_row, 1));
				CassValueType t3 = cass_value_type(cass_row_get_column(cas_row, 2));
				rc = cass_value_get_int64(cass_row_get_column(cas_row, 0), &row.a1);
				if(rc != CASS_OK){
					
				}
				cass_value_get_int32(cass_row_get_column(cas_row, 1), &row.dsize);
				if(rc != CASS_OK){
				}
				cass_value_get_bytes(cass_row_get_column(cas_row, 2), (const cass_byte_t**)&(row.data), &row.data_length);
				if(rc != CASS_OK){
				}
				get_result = (row.a1==a);
				if(get_result){
					data.resize(row.data_length);
					memcpy(&data[0], row.data, row.data_length);
				}
			}

			cass_result_free(result);
			cass_iterator_free(iterator);
		}

		this->get_statement.complete();

		return get_result;
	}
	size_t size() const {
		return 0;
	}
	void flush(){
		if(transaction==NULL) return;
		CassFuture* future = NULL;					
		future = cass_session_execute_batch(session, transaction);
		cass_future_wait(future);
		CassError rc = cass_future_error_code(future);
		if (rc != CASS_OK) {
			print_error(future);
		}
		cass_future_free(future);
	}
	void begin(){
		 if(transaction){
			 return;
		 }
		 //transaction = cass_batch_new(CASS_BATCH_TYPE_LOGGED);

	}
	void commit(){
		set_attribute(get_max_block_name(),max_block);
		if(transaction == NULL) return;
		printf("[TS] [INFO] cass commit\n");		
		flush();
		cass_batch_free(transaction);
		transaction = NULL;

	}
	void rollback(){
		if(transaction == NULL){
			return;
		}
		cass_batch_free(transaction);
		transaction = NULL;
	}
};
