#pragma once
#ifndef _RED_NODE_FILE_H_CEP_20151004_
#define _RED_NODE_FILE_H_CEP_20151004_
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
namespace nst = ::stx::storage;
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
namespace red {
	typedef Poco::ScopedLockWithUnlock<Poco::Mutex> synchronized;

	struct dist_info {
		std::string name;
		nst::u64 start_row;
		nst::u64 end_row;
		std::string node;
		nst::u64 size;
	};
	class rednodes  {
	protected:
		

	public:
		rednodes() {
		}
		~rednodes() {
		
		}
		void close() {
		
		}
		void open() {					
		}

		std::vector<std::string> get_nodes(std::string col) {
			std::vector<std::string> r;
			return r;
		}

		std::vector<dist_info> get_colinfos() {
			std::vector<dist_info> result;
		
			return result;
		}
		/// return a sorted set of nodes
		std::vector<std::string> get_nodes() {
			std::vector<std::string> nodes;
			return nodes;
		}
		void set_node_space(std::string name) {
			
		}
		void set_node_space(std::string name, nst::i64 available, nst::i64 capacity) {
			
		}
		void set_node_status(std::string name, std::string status) {

			
		}
		void start_node(std::string name) {
			
		}
		bool has_node(std::string name) {
			bool r = false;
			
			return r;
		}
		bool has_col(std::string col_name) {
			return false;
		}
		void add_coll(std::string col_name, std::string node) {
			//(name, node, startrow, endrow, size) VALUES (?, ?, ?, ?, dateof(now()));

			

		}
		void add_node(std::string name) {
			
		}
	};
}; ///red namespace
#endif