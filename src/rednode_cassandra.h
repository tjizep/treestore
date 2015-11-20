#ifndef _RED_NODE_CASSANDRA_H_CEP_20151004_
#define _RED_NODE_CASSANDRA_H_CEP_20151004_
#include "red_includes.h"
#include "Poco/Net/SocketReactor.h"
#include "Poco/Net/SocketAcceptor.h"
#include "Poco/Net/SocketNotification.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/NObserver.h"
#include "Poco/Exception.h"
#include "Poco/Thread.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"

#include <iostream>
#include "cassandras.h"
#include <boost/filesystem.hpp>

using Poco::Net::SocketReactor;
using Poco::Net::SocketAcceptor;
using Poco::Net::ReadableNotification;
using Poco::Net::ShutdownNotification;
using Poco::Net::ServerSocket;
using Poco::Net::StreamSocket;
using Poco::NObserver;
using Poco::AutoPtr;
using Poco::Thread;
using Poco::Util::Option;
using Poco::Util::OptionSet;
extern char * treestore_contact_points;
namespace red{
	typedef Poco::ScopedLockWithUnlock<Poco::Mutex> synchronized;

	struct dist_info{
		std::string name;
		nst::u64 start_row;
		nst::u64 end_row;
		std::string node;
		nst::u64 size;
	};
	class rednodes : public blanca::cass_env{
	protected:
		blanca::dht_prepared	insert_node;
		blanca::dht_prepared	insert_col;
		blanca::dht_prepared	select_nodes;
		blanca::dht_prepared	select_node;
		blanca::dht_prepared	col_by_node;
		blanca::dht_prepared	col_2_node;		
		blanca::dht_prepared	node_started;
		blanca::dht_prepared	update_node_status;
		blanca::dht_prepared	update_node_space;
		blanca::dht_prepared	cols_all;
		
	public:
		rednodes(){
		}
		~rednodes(){
			close();
		}
		void close(){
			insert_node.close();
			insert_col.close();
			select_nodes.close();
			select_node.close();
			col_by_node.close();
			col_2_node.close();
			node_started.close();
			select_node.close();
			update_node_status.close();
			update_node_space.close();
			cols_all.close();
			blanca::cass_env::close();
			
		}
		void open(){			
			if(blanca::cass_env::open(treestore_contact_points)){
				std::string cas_table_name = "nodes";
				std::string cas_cols_table_name = "cols";
				std::string cas_keyspace = "ts_red_node_dist_info";
				std::string canon =  cas_keyspace + "." + cas_table_name ;
				std::string canon_cols =  cas_keyspace + "." + cas_cols_table_name ;
				std::string create_key_space = "CREATE KEYSPACE " + cas_keyspace ;
				create_key_space += " WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' };";
				
				std::string create_nodes = "CREATE TABLE " + canon + " (";
				create_nodes += "name text , ";
				create_nodes += "max_row bigint, ";
				create_nodes += "available bigint, ";
				create_nodes += "capacity bigint, ";
				create_nodes += "created timestamp, ";
				create_nodes += "started timestamp, ";
				create_nodes += "cols bigint, ";
				create_nodes += "base text, ";
				create_nodes += "status text , ";
				create_nodes += "PRIMARY KEY (name));";

				std::string create_cols = "CREATE TABLE " + canon_cols + " (";
				create_cols += "name text , ";				
				create_cols += "node text, ";
				create_cols += "inserted timestamp, ";
				create_cols += "startrow bigint, ";
				create_cols += "endrow bigint, ";
				create_cols += "size bigint, ";
				create_cols += "PRIMARY KEY (name,startrow,endrow));";

				this->execute_query(create_key_space);
				this->execute_query(create_cols);
				this->execute_query(create_nodes);

				std::string insert_cols_query = "INSERT INTO  " + canon_cols + " (name, node, startrow, endrow, size) VALUES (?, ?, ?, ?, dateof(now()));";
				std::string insert_query = "INSERT INTO  " + canon + " (name, max_row, available, capacity, cols, base, status, started, created) VALUES (?, ?, ?, ?, ?, ?, 'created', dateof(now()), dateof(now()));";
				std::string update_node_started = "UPDATE " + canon + " SET started = dateof(now()) WHERE name = ?";
				std::string update_node_status_query = "UPDATE " + canon + " SET status = ? WHERE name = ?";
				std::string update_node_space_query = "UPDATE " + canon + " SET available = ?, capacity = ? WHERE name = ?";


				std::string select_query = "SELECT name, max_row, available, capacity, base, started, created FROM " + canon + ";";
				std::string select_node_query = "SELECT name, max_row, base, started, created FROM " + canon + " WHERE name = ?;";
				std::string select_col_query = "SELECT name, node, startrow, endrow, size FROM " + canon_cols + " WHERE name = ?;";
				std::string select_col_all_query = "SELECT name, node, startrow, endrow, size FROM " + canon_cols + " ;";

				std::string select_cols_2_node = "SELECT node FROM " + canon_cols + " WHERE name = ?;";

				this->update_node_status.prepare(get_session(), update_node_status_query);
				this->insert_node.prepare(get_session(), insert_query);
				this->select_nodes.prepare(get_session(), select_query);		
				this->select_node.prepare(get_session(), select_node_query);
				this->insert_col.prepare(get_session(), insert_cols_query);
				this->col_by_node.prepare(get_session(), select_col_query);
				this->col_2_node.prepare(get_session(), select_cols_2_node);
				this->node_started.prepare(get_session(), update_node_started);
				this->update_node_space.prepare(get_session(), update_node_space_query);
				this->cols_all.prepare(get_session(), select_col_all_query);
			}
		}

		std::vector<std::string> get_nodes(std::string col){
			std::vector<std::string> r;
			return r;
		}
		
		std::vector<dist_info> get_colinfos(){
			std::vector<dist_info> result;
			if(!cols_all.is_prepared()) return result;
			this->cols_all.execute();
			const CassResult* cass_result = cass_future_get_result(this->cols_all.get_result_future());
			CassIterator* iterator = cass_iterator_from_result(cass_result);
			dist_info inf;
			while (cass_iterator_next(iterator)) {
				const CassRow* cass_row = cass_iterator_get_row(iterator);
				/// name, node, startrow, endrow, size
				inf.name = get_string(cass_row, 0);
				inf.node = get_string(cass_row, 1);
				inf.start_row = get_u64(cass_row, 2);
				inf.end_row = get_u64(cass_row, 3);		
				inf.size = get_u64(cass_row, 4);			
				result.push_back(inf);				
			}
			return result;
		}
		/// return a sorted set of nodes
		std::vector<std::string> get_nodes(){			
			if(!select_nodes.is_prepared()) return std::vector<std::string>();
			CassError rc = CASS_OK;		
			std::vector<std::string> nodes;
			this->select_nodes.execute();
			const CassResult* result = cass_future_get_result(this->select_nodes.get_result_future());
			CassIterator* iterator = cass_iterator_from_result(result);

			while (cass_iterator_next(iterator)) {
				const CassRow* cas_row = cass_iterator_get_row(iterator);
				nodes.push_back(get_string(cas_row,0));				
			}
			cass_result_free(result);
			this->select_nodes.complete();
			return nodes;
		}
		void set_node_space(std::string name){
			using namespace boost;
			using namespace std;
			filesystem::space_info inf = filesystem::space(filesystem::current_path());
			cout << "available: " << filesystem::current_path() << " " << inf.available << " " << inf.capacity << endl;
			set_node_space(name, inf.available, inf.capacity);			
		}
		void set_node_space(std::string name, nst::i64 available, nst::i64 capacity){
			if(!update_node_space.is_prepared()) return;
			CassError rc = CASS_OK;
			this->update_node_space.bind_i64(0, available);
			this->update_node_space.bind_i64(1, capacity);
			this->update_node_space.bind(2, name);
			rc = this->update_node_space.execute();
			this->update_node_space.complete();
		}
		void set_node_status(std::string name, std::string status){
			
			if(!update_node_status.is_prepared()) return;
			CassError rc = CASS_OK;
			this->update_node_status.bind(0, status);
			this->update_node_status.bind(1, name);
			rc = this->update_node_status.execute();
			this->update_node_status.complete();
		}
		void start_node(std::string name){
			if(!node_started.is_prepared()) return;
			CassError rc = CASS_OK;
			this->node_started.bind(0, name);
			rc = this->node_started.execute();
			this->node_started.complete();
		}
		bool has_node(std::string name){
			bool r = false;
			if(!select_node.is_prepared()) return r;

			CassError rc = CASS_OK;		
			this->select_node.bind(0, name);
			this->select_node.execute();
			const CassResult* result = cass_future_get_result(this->select_node.get_result_future());
			CassIterator* iterator = cass_iterator_from_result(result);

			if (cass_iterator_next(iterator)) {
				
				std::string name;
				const CassRow* cas_row = cass_iterator_get_row(iterator);

				const char* str;
				size_t str_size = 0;
				rc = cass_value_get_string(cass_row_get_column(cas_row, 0),&str,&str_size);
				r = (rc==CASS_OK);				
				if(r){
					printf("[TS] [INFO] found the node %.*s\n",(int)str_size,str);
				}
			}
			cass_result_free(result);
			this->select_node.complete();
			return r;
		}
		bool has_col(std::string col_name){
			return false;
		}
		void add_coll(std::string col_name, std::string node){
			 //(name, node, startrow, endrow, size) VALUES (?, ?, ?, ?, dateof(now()));
			
			if(!insert_col.is_prepared()) return;
			CassError rc = CASS_OK;			
			this->insert_col.bind(0, col_name);
			this->insert_col.bind(1, node);
			this->insert_col.bind_i64(2, 0);			
			this->insert_col.bind_i64(3, 0);
			this->insert_col.bind_i64(4, 0);
			
			rc = this->insert_col.execute();
			this->insert_col.complete();

		}
		void add_node(std::string name){
			if(!this->insert_node.is_prepared()) return;
			using namespace boost;
			using namespace std;
			filesystem::space_info inf = filesystem::space(filesystem::current_path());
			cout << "[TS] [DHT]  [INFO] " << "available: " << filesystem::current_path() << " " << inf.available << " " << inf.capacity << endl;
			CassError rc = CASS_OK;			
			this->insert_node.bind(0, name);
			this->insert_node.bind_i64(1, 0);
			this->insert_node.bind_i64(2, 0);
			this->insert_col.bind_i64(3, inf.available); /// available space
			this->insert_col.bind_i64(4, inf.capacity); /// space total
			this->insert_node.bind(5, "");
			rc = this->insert_node.execute();
			this->insert_node.complete();
		}
	};
}; ///red namespace
#endif