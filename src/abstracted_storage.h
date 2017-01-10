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
#ifndef _ABSTRACTED_STORAGE_H_CEP2013_
#define _ABSTRACTED_STORAGE_H_CEP2013_

#include "stx/storage/basic_storage.h"
#include "fields.h"
#include "MurmurHash3.h"
#include <map>
#include <vector>
#include <rabbit/unordered_map>
#ifdef _MSC_VER
#include <conio.h>
#endif
namespace NS_STORAGE = stx::storage;
namespace nst = stx::storage;
typedef std::set<std::string> _LockList;
extern _LockList& get_locklist();
namespace stored{
	
	typedef NS_STORAGE::sqlite_allocator<NS_STORAGE::stream_address, NS_STORAGE::buffer_type> _BaseAllocator;
	typedef NS_STORAGE::mvcc_coordinator<_BaseAllocator> _Allocations;

	typedef _Allocations::version_storage_type _Transaction;
	extern _Allocations* get_abstracted_storage(std::string name);
	extern bool erase_abstracted_storage(std::string name);
	extern void reduce_all();
	class AbstractStored{
	public:

		virtual NS_STORAGE::u32 stored() const = 0;
		virtual NS_STORAGE::buffer_type::iterator store(NS_STORAGE::buffer_type::iterator writer) const = 0;
		virtual NS_STORAGE::buffer_type::iterator read(NS_STORAGE::buffer_type::iterator reader) = 0;
	};
	class NullPointerException : public std::exception{
		public: /// The storage action required is inconsistent with the address provided (according to contract)
		NullPointerException() throw() {
		}
	};
	class TransactionNotStartedException : public std::exception{
	public: /// the transaction was not started and the transaction would be by calling this function
		TransactionNotStartedException() throw(){
		}
	};
	class abstracted_storage : public NS_STORAGE::basic_storage{
	public:

	private:

		std::string name;
		_Allocations *_allocations;
		_Transaction *_transaction;
		nst::u64 order;
		bool has_allocations() const {
			return (_allocations != NULL);
		}
		_Allocations& get_allocations(){
			if(_allocations == NULL){
				_allocations = get_abstracted_storage(   (*this).name  );
			}
			return *_allocations;
		}
		const _Allocations& get_allocations() const {
			if(_allocations == NULL){
				throw NullPointerException();
			}
			return *_allocations;
		}
		_Transaction& get_transaction() const {
			if(_transaction == NULL){
				throw NullPointerException();
			}
			return *_transaction;
		}
		_Transaction& get_transaction(bool writer){
			if(_transaction == NULL){
				if(_allocations == NULL){
					begin(writer);
				}else
					_transaction = get_allocations().begin(writer);/// resource aquisition on initialization
				if(_transaction == NULL){
					throw NullPointerException();
				}
			}
			return *_transaction;
		}
		_Transaction& get_transaction(){
			return get_transaction((*this).writer);
		}
        bool writer;
		NS_STORAGE::stream_address boot;
	public:

		std::string get_name() const {
			return name;
		}

		void reduce(){
			get_allocations().reduce();
		}

		abstracted_storage(std::string name)
		:	name(name)
		,	_allocations( NULL)
		,	_transaction(NULL)
		,	order (0)
		,	writer (false)
		,	boot(1)
		{

		}

		~abstracted_storage() {
			try{
				/// TODO: for test move it to a specific 'close' ? function or remove completely
				//get_allocations().rollback();
				close();
			}catch(const std::exception& ){
				/// nothing todo in destructor
				printf("error closing transaction\n");
			}
		}

		nst::u64 get_storage_size() {
			return get_allocations().get_storage_size();
		}

		void load_all(){
			get_allocations().load_all();
		}
		void set_read_cache(bool read_cache){
			get_allocations().set_read_cache(read_cache);
		}
		bool is_readahead() const {
			if(has_allocations()){
				return get_allocations().is_readahead();
			}
			return false;
		}
		void set_readahead(bool is_readahead){
			if(has_allocations()){
				return get_allocations().set_readahead(is_readahead);
			}
		}
		bool get_boot_value(NS_STORAGE::i64 &r){
			r = 0;
			const NS_STORAGE::buffer_type &ba = get_transaction().allocate((*this).boot, NS_STORAGE::read); /// read it
			if(!ba.empty()){
				/// the b+tree/x map needs loading
				NS_STORAGE::buffer_type::const_iterator reader = ba.begin();
				r = NS_STORAGE::leb128::read_signed64(reader,ba.end());
			}
			get_transaction().complete();
			return !ba.empty();
		}

		bool get_boot_value(NS_STORAGE::i64 &r, NS_STORAGE::stream_address boot){
			r = 0;
			const NS_STORAGE::buffer_type &ba = get_transaction().allocate(boot, NS_STORAGE::read); /// read it
			if(!ba.empty()){
				/// the b+tree/x map needs loading
				NS_STORAGE::buffer_type::const_iterator reader = ba.begin();

				r = NS_STORAGE::leb128::read_signed64(reader,ba.end());
				//printf("[BOOT VAL] %s [%lld] version %lld\n",get_name().c_str(), r, get_transaction().get_allocated_version());
			}
			get_transaction().complete();
			return !ba.empty();
		}
		bool get_boot_value(NS_STORAGE::buffer_type& buffer, NS_STORAGE::stream_address boot){
			const NS_STORAGE::buffer_type &ba = get_transaction().allocate(boot, NS_STORAGE::read); /// read it
			buffer = ba;
			get_transaction().complete();
			return !ba.empty();
		}
		void set_boot_value(NS_STORAGE::i64 r){

			NS_STORAGE::buffer_type &buffer = get_transaction().allocate((*this).boot, NS_STORAGE::create); /// write it
			buffer.resize(NS_STORAGE::leb128::signed_size(r));
			NS_STORAGE::buffer_type::iterator writer = buffer.begin();

			NS_STORAGE::leb128::write_signed(writer, r);
			get_transaction().complete();

		}
		void set_boot_value(NS_STORAGE::i64 r, NS_STORAGE::stream_address boot){

			NS_STORAGE::buffer_type &buffer = get_transaction().allocate(boot, NS_STORAGE::create); /// write it
			buffer.resize(NS_STORAGE::leb128::signed_size(r));
			NS_STORAGE::buffer_type::iterator writer = buffer.begin();

			NS_STORAGE::leb128::write_signed(writer, r);
			get_transaction().complete();
		}
		void set_boot_value(const NS_STORAGE::buffer_type& buffer, NS_STORAGE::stream_address boot){

			NS_STORAGE::buffer_type &written = get_transaction().allocate(boot, NS_STORAGE::create); /// write it
			written = buffer ;
			get_transaction().complete();
		}
		/// return true if the storage is local
		bool is_local() const {
			return get_allocations().is_local();
		}
		/// begin a transaction at this very moment
		void begin(bool writer){
			rollback();
			get_allocations();
			(*this).writer = writer;
			get_transaction(writer);
			order = get_allocations().get_order();

		}

		NS_STORAGE::u64 current_transaction_order() const{
			return order;
		}

		bool stale() const {
			if(_transaction==nullptr) return true;
			return (get_transaction().get_order() != get_allocations().get_order());
		}

		bool is_transacted() const {
			return (_transaction!=nullptr);
		}


		bool is_readonly() const {
			if(_transaction == NULL) return true;
			return get_transaction().is_readonly();
		}

		/// merge the changed data to the underlying storage
		/// the transaction is not destroyed and may continue writing
		
		bool merge(){
			bool r = false;
			if(_transaction != NULL){
				r = get_allocations().merge(_transaction);				
			}			
			return r;
		}
		/// a kind of auto commit - by starting the transaction immediately after initialization

		bool commit(){
			bool r = false;
			if(_transaction != NULL){
				if((*this).writer){
					r = get_allocations().merge(_transaction);
					(*this).order = get_allocations().get_order();
				}else{
					r = get_allocations().commit(_transaction);
					_transaction = NULL;
					(*this).order = 0;
				}
				
			}else{
				(*this).writer = false;
			}
			
			return r;
		}

		/// return the version of the current transaction

		NS_STORAGE::version_type get_version(){
			return get_transaction((*this).writer).get_version();
		}

		/// releases whatever version locks may be used

		void rollback(){
			if(_transaction != NULL){
				get_allocations().discard(_transaction);
				_transaction = NULL;
				(*this).writer = false;
				(*this).order = 0;
			}
		}

		/// get versions

		nst::u64 get_greater_version_diff(nst::_VersionRequests& request){
			nst::u64 response = 0;
			if(_transaction != NULL){
				response += get_transaction().get_greater_version_diff(request);
			}
			return response;
		}
		/// close the named storage so that files may be removed

		void close(){
			if(_transaction != NULL){
				get_allocations().discard(_transaction);
				_transaction = NULL;
			}
			if(_allocations!=NULL){
				//get_transaction().();
				get_allocations().release();
				_allocations = NULL;
			}
			(*this).writer = false;
		}

		/// returns true if the buffer passed marks the end of storage
		///	i.e. invalid read or write allocation of non existent address

		bool is_end(const NS_STORAGE::buffer_type& buffer) const {
			return get_transaction().is_end(buffer);
		}

		/// Storage functions called by persisted data structure
		/// allocate a new or existing buffer, new denoted by what == 0 else an existing
		/// buffer with the specfied stream address is returned - if the non nil address
		/// does not exist an exception is thrown'

		NS_STORAGE::buffer_type& allocate(NS_STORAGE::stream_address &what,NS_STORAGE::storage_action how){

			return get_transaction().allocate(what,how);
		}

		void complete(){
			get_transaction().complete();
		}

		/// get allocated version

		NS_STORAGE::version_type get_allocated_version() const {
			return get_transaction().get_allocated_version();
		}

		/// function returning the stored size in bytes of a value

		template<typename _Stored>
		NS_STORAGE::u32 store_size(const _Stored& k) const {
			return  k.stored();
		}


		/// writes a key to a vector::iterator like writer

		template<typename _Iterator,typename _Stored>
		void store(_Iterator& writer, const _Stored& stored) const {
			writer = stored.store(writer);
		}


		/// reads a key from a vector::iterator like reader

		template<typename _Stored>
		void retrieve(const nst::buffer_type& buffer, typename nst::buffer_type::const_iterator& reader, _Stored &value) const {
			reader = value.read(buffer, reader);
		}

	};
	/// returns true if the map should reload
	template<typename _Storage>
	bool abstracted_tx_begin_1(bool read, _Storage& storage){
		bool write = !read;
		if(write){
			/// only start the writing transaction when reading or idle
			bool r = false;
			if(!storage.is_transacted() || storage.is_readonly()){
				
				storage.begin(write);
				r = true;
			}

			return r;
		}
		else
		{

			if(storage.stale()){
				storage.rollback();
				storage.begin(write);
				return true;
			}
		}
		return false;
	}
	template<typename _Storage, typename _Map>
	void abstracted_tx_begin(bool read,bool shared, _Storage& storage, _Map& map){
		////bool write = !read;
		bool reload = abstracted_tx_begin_1(read,storage);
		//if(write || !shared){
			map.unshare();
		//}else
		//	map.share();
		if(reload)
			map.reload();
	}
	/// definitions for registry functions
	typedef rabbit::unordered_map<std::string, _Allocations*> _AlocationsMap;

	extern _Allocations* _get_abstracted_storage(std::string name);
};
#endif
