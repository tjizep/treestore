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
#ifndef _COLLUM_DEF_CEP_20130801_
#define _COLLUM_DEF_CEP_20130801_
#include <stdlib.h>
#include <string>
#include "fields.h"
#include "Poco/Mutex.h"
#include "Poco/Thread.h"
#include "Poco/ThreadPool.h"
#include "Poco/TaskManager.h"
#include "Poco/Task.h"
#include "NotificationQueueWorker.h"
#include "conversions.h"
#include "iterator.h"
#include <rabbit/unordered_map>
#include <stx/storage/pool.h>
namespace nst = NS_STORAGE;
extern Poco::Mutex data_loading_lock;
extern double		treestore_column_cache_factor;
namespace collums{
	template<typename _Key, typename _Value, typename _Storage, typename _ValueEncoder>
	class paged_vector{
	public:
		/// size type
		typedef size_t							size_type;
		/// version type
		typedef nst::version_type				version_type;
		/// key address size type
		typedef _Key							key_type;
		/// the type of values stored
		typedef _Value							data_type;
		
		/// the value encoder
		typedef _ValueEncoder					value_encoder_type;
		/// the storage type
		typedef _Storage						storage_type;
		/// the persistent buffer type for encoding pages
		typedef nst::buffer_type				buffer_type;
		/// the key value pair for iterators
		typedef std::pair<key_type, data_type>	value_type;
		
	protected:
		enum{
			page_size = 128,//192,348
			use_pool_allocator = 1
		};
		struct stored_page{
		private:
			mutable bool modified;
			size_type size; 	
			size_type address;
			version_type version;
			nst::u8	exists[page_size/8];
			data_type values[page_size];
			std::atomic<nst::u64> references;
		private:
			void set_bit(nst::u8& w, nst::u8 index, bool f){
				
				#ifdef _MSC_VER
				#pragma warning(disable:4804)
				#endif				
				nst::u8 m = (nst::u8)1ul << index;// the bit mask				
				w ^= (-f ^ w) & m;				
			}
		
			
		public:
			void reset_references(){
				this->references = 0;
			}
			nst::u64 get_references() const {
				return this->references;
			}
			void ref(){
				++references;
			}
			void unref(){
				--references;
			}
			stored_page(){
				references = 0;
				modified = false;
				size = page_size;
				address = 0;
				memset(&exists[0], 0, sizeof(exists)); /// empty
				//if(use_pool_allocator!=1)
				//	nst::add_buffer_use(sizeof(*this));
			}
			//~stored_page(){
			//	if(use_pool_allocator!=1)
			//		nst::remove_buffer_use(sizeof(*this));
			//}
			void set_exists(size_type which, bool val){
				size_type b = which & (page_size-1);
				nst::u8& v = exists[b>>3];// 8 == 1<<3,/8 >>3
				nst::u8 bit = b & 7;
				set_bit(v, bit, val);
			}
			/// this function takes an page untranslated address - which
			bool is_exists(size_type which) const {
				size_type b = which & (page_size-1);
				nst::u8 v = exists[b>>3];// 8 == 1<<3,/8 >>3
				nst::u8 bit = b & 7;
				return ((v >> bit) & (nst::u8)1ul);
			}
			/// erases an entry
			void erase(size_type which){
				set_exists(which,false);
			}
			/// this function takes an page untranslated address - which
			bool is_exists_row(size_type row) const {
				size_type b = row;
				nst::u8 v = exists[b>>3];// 8 == 1<<3,/8 >>3
				nst::u8 bit = b & 7;
				return ((v >> bit) & (nst::u8)1ul);
			}
			data_type& get(size_type which) {				
				return values[which % page_size];
			}
			data_type* pget(size_type which) {				
				if(is_exists(which))
					return &values[which % page_size];
				return nullptr;
			}
			const data_type& get(size_type which) const {
				return values[which % page_size];
			}
			
			void set(size_type which, const data_type& value){				
				modified = true;
				set_exists(which, true);
				get(which) = value;				
			}
			
			void set_address(size_type address){
				(*this).address = address;
			}
			
			size_type get_address() const {
				return (*this).address;
			}
			
			void set_version(version_type version){
				this->version = version;
			}

			version_type get_version() const {
				return (*this).version;
			}
			
			void load
			(	value_encoder_type encoder
			,	storage_type & storage
			,	buffer_type& buffer
			,	size_t bsize
			){
				if(!bsize) return;
				 using namespace stx::storage;
				(*this).address = address;
				buffer_type::const_iterator reader = buffer.begin();
				nst::i32 encoded_value_size = leb128::read_signed(reader);
				for(size_type b = 0; b < page_size/8; ++b){
					exists[b] = (*reader);
					++reader;
				}
				if(encoded_value_size > 0){
					encoder.decode_values_row(buffer, reader, (*this).values, page_size);
				}else{
					for(size_type v = 0; v < page_size; ++v){
						if(is_exists_row(v))
							storage.retrieve(buffer, reader, values[v]);
					}
				}

				size_t d = reader - buffer.begin();

				if(d != bsize){					
					err_print("page has invalid size");
				}
			}
			
			void save(value_encoder_type encoder, storage_type &storage, buffer_type& buffer) const {
				if(!modified) return;
					
				using namespace stx::storage;
				size_type storage_use = 0;
				nst::i32 encoded_value_size = encoder.encoded_values_size((*this).values, page_size);
				storage_use += leb128::signed_size(encoded_value_size);				
				storage_use += page_size/8;
				if(encoded_value_size > 0){
					storage_use += encoded_value_size;
				}else{
					encoded_value_size = 0;
					for(size_type v = 0; v < page_size; ++v){
						if(is_exists_row(v))
							storage_use += storage.store_size(values[v]);
					}
				}
				buffer.resize(storage_use);
				if(buffer.size() != storage_use){
					err_print("resize failed");
				}
				buffer_type::iterator writer = buffer.begin();				
				writer = leb128::write_signed(writer, encoded_value_size);			
				for(size_type b = 0; b < page_size/8; ++b){
					(*writer) = exists[b];
					++writer;
				}
					
				if(encoded_value_size > 0 ){
					encoder.encode_values_row(buffer, writer, (*this).values, page_size);
				}else{					
					for(size_type v = 0; v < page_size; ++v){
						if(is_exists_row(v))
							storage.store(writer, values[v]);
					}
				}
				size_type d = writer - buffer.begin();
				if(d > storage_use){
					err_print("array page encoding failed");
				}
				
				modified = false;
			}
				
			bool is_modified() const {
				return this->modified;
			}
			//typedef std::shared_ptr<stored_page> ptr;
			typedef stored_page* ptr;
		};

		typedef typename stored_page::ptr stored_page_ptr;
		class shared_context{
		public:
			typedef std::pair<size_type,version_type> address_version;
			
			struct hash_address_version{
				unsigned long operator() (const address_version& k) const {
					return (k.first << 7) ^ std::hash<version_type>()(k.second);
				}
			};
			typedef rabbit::unordered_map<address_version, stored_page_ptr,hash_address_version> _Pages;
		protected:
			mutable Poco::Mutex lock;
			_Pages versioned_pages;
			mutable nst::u64 reduced;
			
		public:
			shared_context(){
				reduced = 0;
			}
			~shared_context(){
			}			
			
			bool has(stored_page_ptr page){
				//
				nst::synchronized l(this->lock);
				address_version location = std::make_pair(page->get_address(), page->get_version());
				if(versioned_pages.count(location)){
					return true;
				}
				return false;
			}
			bool release(stored_page_ptr page){
				
				nst::synchronized l(this->lock);
				address_version location = std::make_pair(page->get_address(), page->get_version());
				stored_page_ptr r = nullptr;
				if(versioned_pages.get(location,r)){
					if(r == page){
						page->unref();
						versioned_pages.erase(location);
						if(page->get_references()==0){
							if(use_pool_allocator==1){
								allocation_pool.free<stored_page>(page);
							}else{
								delete page;
							}
						}
						this->reduced = 0;
						return true;
					}
				}
				
				return false;
			}
			void reduce(){
				//auto now = os::millis() ;
				if(reduced!=0) return; /// nothing happened since last reduce
				if(versioned_pages.empty()) return;
				nst::synchronized l(this->lock);
				if(!stx::memory_mark_state){
					return;
				}
				
				
				//if(now - this->reduced < treestore_cleanup_time/4){
				//	return;
				//}
				
				//printf("[TS] [INFO] reduce pages\n");
				typedef std::vector<address_version> _Released;
				
				if(versioned_pages.empty()){
				}else{
					_Released released;
					for(_Pages::iterator p = versioned_pages.begin();p!=versioned_pages.end();++p){
						stored_page_ptr page = p->second;
						if(page->get_references() == 0){
							err_print("inconsistent memory previously visited page or page invalid");
						}
						if(page->get_references() == 1 && page->is_modified()){
							err_print("inconsistent memory page state");
						}
						if(page->get_references() == 1 && !page->is_modified()){
							page->reset_references();
							if(use_pool_allocator == 1){
								allocation_pool.free<stored_page>(page);
							}else{
								delete page;
							}
						
							released.push_back(p->first);
						}
						if(released.size() > versioned_pages.size() / 8){
							break;
						}
					}
					size_t old = versioned_pages.size();
					for(_Released::iterator r = released.begin(); r != released.end(); ++r){
						versioned_pages.erase((*r));
					}
					if(versioned_pages.size() != old-released.size()){
						err_print("inconsistent memory");
					}
					
				}
				this->reduced = 1 ;
			}
			/// add the page to version control
			bool add(stored_page_ptr p){
				if(p->get_references() < 1){
					err_print("page has invalid reference count");
				}
				address_version location = std::make_pair(p->get_address(), p->get_version());				
				nst::synchronized l(this->lock);
				if(versioned_pages.count(location)==0){					
					p->ref();/// reference for pages collection			
					if(p->is_modified()){
						err_print("page added as modified");
					}
					versioned_pages[location] = p;			
					this->reduced = 0;
					return true;
				}				
				return false;
			}
			/// notify that something has changed inside
			/// which means the reduce function may continue
			void notify_modify(){
				this->reduced = 0;
			}
			/// get the page of specified version
			stored_page_ptr check_out(size_type address, version_type version){
				stored_page_ptr r = nullptr;
				nst::synchronized l(this->lock);
				address_version query = std::make_pair(address, version);
				versioned_pages.get(query,r);				
				if(r != nullptr){
					address_version location = std::make_pair(r->get_address(), r->get_version());	
					if(location != query){
						err_print("the retrieved version does not match the query");
					}
					r->ref(); 
					this->reduced = 0;
				}
				
				return r;
			}
		
		};

		class shared_pages{
		private:
			typedef rabbit::unordered_map<std::string, shared_context*> _Contexts;
			_Contexts contexts;
			mutable Poco::Mutex lock;
		public:
			shared_pages(){
			}
			~shared_pages(){
				for(_Contexts::iterator c = contexts.begin(); c!=contexts.end(); ++c){
					delete (*c).second;
				}
			}
			shared_context * get_context(std::string name){
				nst::synchronized l(this->lock);
				shared_context * r = nullptr;
				if(!contexts.get(name,r)){
					r = new shared_context();
					contexts[name] = r;
				}
				return r;
			}
		};

		/// the page map
		typedef rabbit::unordered_map<size_type, stored_page_ptr, rabbit::rabbit_hash<size_type>, std::equal_to<size_type>, sta::pool_alloc_tracker<size_type>> page_map_type;
		//typedef std::unordered_map<size_type, stored_page_ptr, std::hash<size_type>, std::equal_to<size_type>, sta::pool_alloc_tracker<size_type>> page_map_type;
	private:
		/// keeps cached pages
		mutable page_map_type pages;
		mutable page_map_type modified_pages;
		/// storage from where pages are loaded
		mutable storage_type* storage;
		/// the largest key added
		stx::storage::i64 largest;
		/// the key count 
		stx::storage::i64 _size;
		/// buffer type temporary compress destination
		mutable buffer_type temp_encode;
		mutable buffer_type temp_compress;
		mutable buffer_type temp_decompress;
		/// the encoder/decoder for arrays of values
		value_encoder_type encoder;
		shared_context * version_control;
		mutable stored_page_ptr last_loaded;
		mutable stored_page_ptr local_page;
	private:
		stored_page_ptr allocate_page() const {
			stored_page_ptr page = nullptr;
			//page = std::make_shared<stored_page>();
			if(use_pool_allocator==1){
				page = allocation_pool.allocate<stored_page>();
			}else{
				page = new stored_page(); 
			}
			return page;
		}

		void free_page(stored_page_ptr page) const {
			if(use_pool_allocator==1){
				allocation_pool.free<stored_page>(page);
			}else{
				delete page;
			}
		}

		shared_pages& get_shared(){
			synchronized sl(data_loading_lock);
			static shared_pages shared;
			return shared;
		}

		storage_type& get_storage() const {
			return *storage;
		}
		/// this function expects all reference counts to be updated
		void store_page(stored_page_ptr page){
			if(!page->is_modified()){
				return;
			}
			if(page->get_address()==0){
				err_print("array page has no address");
				return;
			}
			using namespace stx::storage;
			//if(version_control->has(page)){
			//	if(page->get_references() < 2){
			//		err_print("array page is under referenced");
			//	}								
			//}
			version_control->release(page);
			stream_address w = page->get_address();
			//temp_encode.clear();
			page->save(encoder, get_storage(), temp_encode);		
			buffer_type swapped;
			compress_lz4_fast(swapped,temp_encode);				
			buffer_type &buffer = get_storage().allocate(w, stx::storage::create);
			buffer.swap(swapped);
			page->set_version(get_storage().get_allocated_version());
								
			get_storage().complete();

			version_control->add(page);
		}
		void clear_cache() const {
			const_cast<paged_vector*>(this)->clear_cache();
		}
		void clear_cache(){
			if(last_loaded)
				last_loaded->unref();
			last_loaded = nullptr;
			if(pages.empty()) return;
			for(page_map_type::iterator p = pages.begin(); p != pages.end();++p){
				stored_page_ptr page = (*p).second;
				if(page == local_page){
					if(page->is_modified()){
						err_print("local page not saved");
					}
					page->set_address(0);
				}else{
					page->unref();
				}
			}
			pages.clear();
			local_page->set_address(0);
			version_control->notify_modify();
		}
		nst::_VersionRequests version_req;
		/// get version of page relative to current transaction
		/// this is an expensive function - relatively speaking
		version_type get_page_version(size_type address) {
			version_req.clear();			
			version_req.push_back(std::make_pair(address, version_type()));
			if(get_storage().get_greater_version_diff(version_req)){
				if(!version_req.empty() && version_req[0].first == address){
					/// version_req[0].second != 0
					return version_req[0].second;
				}
			}
			return version_type();
		}
		/// get version of page relative to current transaction (const)
		version_type get_page_version(size_type address) const {
			return ((paged_vector*)this)->get_page_version(address);
		}
		/// load a page from an address 
		stored_page_ptr load_page(size_type address) const {
			bool mem_low = false;//(treestore_column_cache != 0 && !stx::memory_mark_state) && get_storage().is_readonly();
			
			stored_page_ptr page = nullptr;
			if(true){ ///get_storage().is_local()
				version_type current_version = get_page_version(address);			
				page = version_control->check_out(address, current_version);				
				if(page){	/// check out refs the page			
					return page;
				}
			}
			/// storage operation started
			nst::stream_address w = address;
			/// the dangling buffer is not copied
			buffer_type& dangling_buffer = get_storage().allocate(w, stx::storage::read);
			if(get_storage().is_end(dangling_buffer) || dangling_buffer.size() == 0){
				
			}
			/// TODO: if there is shortage of ram first try getting a page from version control
			if(mem_low ){				
				//clear_cache();
				
				//version_control->reduce();
			}
			if(mem_low){
				page = local_page;				
			}else {
				page = allocate_page(); 
			}			
			page->set_address(address);
			nst::version_type version = get_storage().get_allocated_version();
			//if(current_version != version){	
			//	current_version = get_page_version(address);
			//	printf("[TS] [ERROR] the current version is invalid - does not match allocated version\n");
			//}
			size_t load_size = nst::r_decompress_lz4(temp_decompress, dangling_buffer );
			get_storage().complete(); /// storage operation complete
			page->load(encoder, get_storage(), temp_decompress, load_size);
			page->set_version(version);
			if(!mem_low){
				page->ref();/// for this
				if(version_control->add(page)){				
				
				}else{
					///printf("[TS] [ERROR] the current page already exists in version control or could not be added\n");
				}
			}
			return page;
		}
		
		stored_page_ptr get_page(size_type which) const {
			size_type address = (which / page_size) + 128;
			if(last_loaded && last_loaded->get_address()==address){
				return last_loaded;
			}
			if(last_loaded)
				last_loaded->unref();
			last_loaded = nullptr;
			if(local_page->get_address() == address){
				return local_page;
			}
			stored_page::ptr page;
			auto i = pages.find(address);
			//if(!pages.get(address,page)){
			if(i != pages.end()){
				page = (*i).second;
			}else{
				page = load_page(address);
				if(page!=local_page){
					pages[address] = page;
				}
			}
			last_loaded = page;
			last_loaded->ref();
			return page;
		}

	public:
		data_type& resolve(size_type key){
			size_type h = key;
			return get_page(h)->get(h);
		}
		data_type* presolve(size_type key){
			size_type h = key;
			return get_page(h)->pget(h);
		}
		bool is_exists(size_type key) const {
			size_type h = key;
			return get_page(h)->is_exists(h);
		}
		const data_type& resolve(size_type key) const {
			size_type h = key;
			return get_page(h)->get(h);
		}

		struct iterator{
			
		private:
			mutable paged_vector* h;			
			size_type pos;
			mutable value_type t;
			mutable key_type k;
			
		public:
			iterator() : h(nullptr), pos(0), t(value_type()), k(key_type()){
				
			}
			iterator(const paged_vector* h, size_type pos)
			: h((paged_vector*)h),pos(pos), t(value_type()), k(key_type()){
				
			}
			iterator(const iterator& r){
				(*this) = r;
			}
			iterator& operator=(const iterator& r){
				h = r.h;
				pos = r.pos;
				return (*this);
			}
			iterator& operator++(){
				/// TODO: not skipping erased values
				++pos;
				return (*this);
			}
			iterator operator++(int){
				iterator t = (*this);
				++(*this);
				return t;
			}
			/// --Prefix backstep the iterator to the last slot
			inline iterator& operator--()
			{

				--pos;

				return *this;
			}

			/// Postfix-- backstep the iterator to the last slot
			inline iterator operator--(int)
			{

				iterator t = (*this);
				--(*this);
				return t;
				
			}
			inline data_type& value(){
				return h->resolve(pos);
			}
			/// returns nullptr when value does not exist
			inline data_type* ref_value(){
				return h->presolve(pos);
			}

			inline const data_type& value() const {
				if(h==nullptr){
					err_print("h is null");
				}
				return h->resolve(pos);
			}

			inline data_type& data(){
				if(h==nullptr){
					err_print("h is null");
				}
				return h->resolve(pos);
			}
			inline bool is_exists(){
				return h->is_exists(pos);
			}

			inline const data_type& data() const {
				if(h==nullptr){
					err_print("h is null");
				}
				return h->resolve(pos);
			}

			inline key_type& key() {
				k = (key_type::value_type)pos;
				return k;
			}
			/// TODO:
			/// this might be safe but if the iterator itself is temporary its not
			/// the key should be safe while the map type is still alive
			inline const key_type& key() const {
				k = (key_type::value_type)pos;
				return k;
			}
			/// TODO:
			/// this might be safe but if the iterator itself is temporary its not
			/// the key should be safe while the map type is still alive
			const value_type& operator*() const {
				t = std::make_pair(key(), value());
				return t;
			}
			/// TODO:
			/// this might be safe but if the iterator itself is temporary its not
			/// the key should be safe while the map type is still alive
			inline value_type& operator*() {
				t = std::make_pair(key(), value());
				return t;
			}
			/// TODO:
			/// this might be safe but if the iterator itself is temporary its not
			/// the key should be safe while the map type is still alive
			inline const value_type* operator->() {
				t = std::make_pair(key(), value());
				return &t;
			}

			inline bool operator==(const iterator& r) const {
				return (pos == r.pos);
			}
			bool operator!=(const iterator& r) const {
				return (pos != r.pos);
			}
		};
		typedef iterator const_iterator;
		paged_vector(storage_type& storage) 
		:	storage(&storage)
		,	largest(0)
		,	_size(0)
		{
			//pages.set_logarithmic(1);
			local_page = allocate_page();
			last_loaded = nullptr;
			version_control = get_shared().get_context(storage.get_name());
			get_storage().get_boot_value(largest,2) ;
			get_storage().get_boot_value(_size,3);
		}
		
		~paged_vector(){
			clear_cache();
			free_page(local_page);
		}
		std::pair<iterator, bool> erase(const key_type&  key){
			size_t h = key.get_hash();
			if(h <= largest){
				if(h > 0)
					largest = std::min<size_type>(largest, h-1);
				if(get_storage().is_readonly()){
					get_page(h);
				}else{
					stored_page::ptr page = get_page(h);
					if(page->is_exists(h)){
						--_size;
					}					
					page->erase(h);
					modified_pages[h] = page;
					return std::make_pair(iterator(this, key.get_hash()), true);
				}
					
			}
			std::pair<iterator, bool> r = std::make_pair(iterator(this, key.get_hash()), false);
			return r;
		}
		std::pair<iterator, bool> insert(const key_type&  key, const data_type& value){
			//if(stx::memory_low_state){
			//	flush();
			//	clear_cache();
			//}
			size_t h = key.get_hash();
			largest = std::max<size_type>(largest, h+1);
			if(get_storage().is_readonly()){
				get_page(h);
			}else{
				if(!get_page(h)->is_exists(h)){
					++_size;
				}
				stored_page::ptr page = get_page(h);
				page->set(h, value);
				modified_pages[h] = page;

			}
			std::pair<iterator, bool> r = std::make_pair(iterator(this, key.get_hash()), false);
			return r;
		}
		/// Returns a reference to the object that is associated with a particular
		/// key. If the map does not already contain such an object, operator[]
		/// inserts the default object data_type().
		inline data_type& operator[](const key_type& key)
		{
			iterator i = insert( key, data_type() ).first;
			return i.data();
		}

		/// Returns a const reference to the object that is associated with a particular
		/// key. If the map does not already contain such an object, operator[] const
		/// returns the object at the end.
		inline const data_type& operator[](const key_type& key) const
		{
			const_iterator i = find( key );
			return i.data();
		}
		iterator find(const key_type& key) const{
			return iterator(this, key.get_hash());
		}
		iterator lower_bound(const key_type& key) const{
			return iterator(this, key.get_hash());
		}
		iterator end() const {
			return iterator(this, largest);
		}
		iterator begin() const {
			return iterator(this, 0);
		}
		bool count(size_type key) {
			return false;
		}
		void reduce_use(){			
			//if(stx::memory_low_state){
				
				flush();
				//if(pages.size()*sizeof(stored_page) < 2048*1024) return;
				clear_cache();
				version_control->reduce();
			//}			
		}
		
		void flush(){
			flush_buffers();
			if(stx::memory_mark_state){
				clear_cache();
				version_control->reduce();
			}
		}

		void flush_buffers(){
			if(!get_storage().is_readonly()){
				get_storage().set_boot_value(largest, 2);
				get_storage().set_boot_value(_size, 3);
				for(auto p = modified_pages.begin(); p != modified_pages.end();++p){
					store_page((*p).second);
				}		
				modified_pages.clear();
			}
		}

		void reload(){
			//if(!get_storage().is_readonly()){
				clear_cache();
				get_storage().get_boot_value(largest,2) ;
				get_storage().get_boot_value(_size,3);
			//}
		}
		void unshare(){
		}
		size_type size() const {
			return _size;
		}
		bool empty() const {
			return size() == 0;
		}
	};
	template<class _StoredType>
	struct col_policy_type{
		bool load(size_t max_size) const{
			return max_size < 512;
		}
	};
	template<>
	struct col_policy_type<stored::UShortStored>{
		bool load(size_t) const{
			return true;
		}
	};
	
	template<>
	struct col_policy_type<stored::CharStored>{
		bool load(size_t) const{
			return true;
		}
	};
	template<>
	struct col_policy_type<stored::UCharStored>{
		bool load(size_t) const{
			return true;
		}
	};
	template<>
	struct col_policy_type<stored::IntStored>{
		bool load(size_t) const{
			return true;
		}
	};
	template<>
	struct col_policy_type<stored::UIntStored>{
		bool load(size_t) const{
			return true;
		}
	};
	template<>
	struct col_policy_type<stored::LongIntStored>{
		bool load(size_t) const{
			return true;
		}
	};
	template<>
	struct col_policy_type<stored::ULongIntStored>{
		bool load(size_t) const{
			return true;
		}
	};
	
	using namespace iterator;
	typedef stored::_Rid _Rid;

	extern void set_loading_data(const std::string& name, int loading);
	extern int get_loading_data(const std::string& name);
	extern long long col_page_use;
	template<typename _Stored>
	class collumn {
	public:
		typedef typename stored::IntTypeStored<_Rid> _StoredRowId;
		stored::abstracted_storage storage;

		///typedef stx::btree_map<_StoredRowId, _Stored, stored::abstracted_storage,std::less<_StoredRowId>, stored::int_terpolator<_StoredRowId,_Stored>> _ColMap;
		typedef paged_vector<_StoredRowId, _Stored, stored::abstracted_storage, stored::int_terpolator<_StoredRowId,_Stored> > _ColMap;
	private:
		static const nst::u16 F_NOT_NULL = 1;
		static const nst::u16 F_CHANGED = 2;
		static const nst::u16 F_INVALID = 4;



		
		
	private:
		_ColMap col;
		typedef typename _ColMap::iterator _ColIter;
		typename _ColMap::iterator cend;
		typename _ColMap::iterator ival;
		_Stored empty;
		_Rid max_row_id;
		_Rid rows_per_key;
		bool modified;
		bool lazy;		
		size_t max_size;
		col_policy_type<_Stored> col_policy;
		inline bool has_cache() const {
			return pages != nullptr ;
		}

		void load_cache(){
		}
		
		void reset_cache_locals(){

			ival = col.end();
		}

		_Rid get_v_row_count(){
			_Rid r = 0;
			typename _ColMap::iterator e = col.end();
			typename _ColMap::iterator c = e;
			if(!col.empty()){
				--c;
				r = c.key().get_value() + 1;
			}
			return r;
		}
		void check_page_cache(){
			max_row_id = get_v_row_count(); //(_Rid)col.size();
			load_cache();
		}
		
		void uncheck_page_cache(){
		}
		/// no assignments please
		collumn& operator=(const collumn&){
		}
	public:
		void set_lazy(bool dl){
			(*this).lazy= dl;
		}
		_Rid get_rows() const {
			return  (_Rid)col.size();
		}


		typedef ImplIterator<_ColMap> iterator_type;
		collumn(std::string name, size_t max_size, bool load = false)
		:	storage(name)
		,	col(storage)
		,	empty(_Stored())
		,	max_row_id(0)
		,	rows_per_key(0)
		,	modified(false)
		,	lazy(load)		
		,	max_size(max_size)
		{
			storage.load_all();
			
#ifdef _DEBUG

			//int_terpolator t;
			//t.test_encode();
#endif
		}

		~collumn(){

		}

		void set_data_max_size(size_t max_size){
			this->max_size = max_size;
		}
		
		void initialize(bool by_tree){

			cend = col.end();
			ival = cend;
			if(!modified){

			}
		}
		/// returns a sampled rows per key statistic for the collumn
		_Rid get_rows_per_key(){
			typedef std::set<_Stored> _Uniques;
			if(rows_per_key == 0){

				_Uniques unique;
				const nst::u32 SAMPLE = std::min<nst::u32>((nst::u32)col.size(), 100000);
				for(nst::u32 u = 0; u < SAMPLE; ++u){
					unique.insert(seek_by_cache(u));
				}
				rows_per_key = SAMPLE/std::max<nst::u32>((nst::u32)unique.size(),1); //returns a value >= 1
				if(rows_per_key > 1){
					rows_per_key /= 2;
				}


			}
			return rows_per_key;
		}

		bool is_null(const _Stored&v){
			return (&v == &empty);
		}
		
		_Stored& seek_by_tree(_Rid row) {

			if(ival != cend){
				++ival;				
				if(ival != cend && ival.key().get_value() == row){
					_Stored* val = ival.ref_value();
					if(val!=nullptr){
						return *val;
					}else 
						return empty;
				}
			}
			//if(allocation_pool.is_near_depleted())
			///	col.reduce_use();
			ival = col.find(row);
			

			if(ival == cend || ival.key().get_value() != row)
			{
				return empty;
			}
			_Stored* val = ival.ref_value();
			if(val!=nullptr){
				return *val;
			}else
				return empty;
		}

		inline _Stored& seek_by_cache(_Rid row)  {
			
			

			return seek_by_tree(row);

		}

		void flush(){
			if(modified)
			{
				inf_print("column flushing %s", storage.get_name().c_str());
				col.flush_buffers();
				//col.reduce_use();
				storage.commit();
				initialize(true);
			}
			modified = false;
		}

		void flush2(){
			if(modified){
				col.flush_buffers();
				initialize(true);
			}
		}

		void commit1(){
			if(modified){
				col.flush_buffers();
				initialize(true);
			}

			reset_cache_locals();
		}

		void commit2(){
			uncheck_page_cache();

			if(modified){

				if(!storage.commit()){
					rollback();
				}
			}
			modified = false;

		}

		void tx_begin(bool read,bool shared= true){
			stored::abstracted_tx_begin(read,shared, storage, col);
			storage.set_readahead(false);
			check_page_cache();
		}
		_Rid get_max_row_id() const {
			return max_row_id;
		}
		void rollback(){
			
			if(modified){
				col.flush();
			}
			uncheck_page_cache();
			///col.reduce_use();
			modified = false;

			storage.rollback();
		}

		void reduce_cache_use(){
			bool tx = storage.is_transacted();
			if(!tx)
				storage.rollback();
		}

		void reduce_tree_use(){
			bool tx = storage.is_transacted();
			col.reduce_use();
			if(!tx)
				storage.rollback();
		}
		void flush_tree(){
			bool tx = storage.is_transacted();
			col.flush_buffers();
			if(!tx)
				storage.rollback();
			initialize(true);
		}
		ImplIterator<_ColMap> find(_Rid rid){
			return ImplIterator<_ColMap> (col, col.find(rid));
		}

		ImplIterator<_ColMap> begin(){
			return ImplIterator<_ColMap> (col, col.begin());
		}

		void erase(_Rid row){
			col.erase(row);
		
		}

		void add(_Rid row, const _Stored& s){
			max_row_id = std::max<_Rid>(row+1, max_row_id);
		
			col[row] = s;
			modified = true;
		}
	};



	static void test_ints(){
		typedef stored::StandardDynamicKey _Sk;
		typedef std::map<_Sk, int> _TestMap;
		int mv = 1000000;
		_TestMap tm;
		_Sk k1, k2, k3 , k4 , k5;
		k1.add4(19930101);
		k2.add4(19930201);
		k3.add4(19930202);
		k4.add4(19940202);
		k5.add4(199402020);
		if(k1 < k2){
			printf("ok 1\n");
		}
		if(k2 < k3){
			printf("ok 2\n");
		}
		if(k3 < k4){
			printf("ok 3\n");
		}
		if(k4 < k5){
			printf("ok 4\n");
		}
		for(int i=1; i < mv; ++i){
			_Sk k;
			k.add4(i);
			tm[k] = i;

		}
		for(int i=1; i < mv; ++i){
			_Sk k;
			k.add4(i);
			_TestMap::iterator t = tm.find(k);
			if((*t).second != i){
				printf("cry boohoo\n");
			}

		}
		printf("passed the test\n");
	}


};
#endif
