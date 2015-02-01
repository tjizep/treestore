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
#include "fields.h"
#include "Poco/Mutex.h"
#include "Poco/Thread.h"
#include "Poco/ThreadPool.h"
#include "Poco/TaskManager.h"
#include "Poco/Task.h"
#include "NotificationQueueWorker.h"
#include "conversions.h"
#include "iterator.h"
namespace nst = NS_STORAGE;
namespace collums{
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


		struct int_terpolator{

			typedef typename _Stored::list_encoder value_encoder;
			value_encoder encoder; /// delegate encoder/decoder
			/// function assumes a list of increasing integer values
			inline bool encoded(bool multi) const
			{
				return !multi; // only encodes uniques;
			}
			inline bool encoded_values(bool multi) const
			{
				return encoder.encoded(multi); // only encodes uniques;
			}
			typedef nst::u16 count_t;
			/// delegate value decoder
			void decode_values(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader,_Stored* values, count_t occupants) {
				
				encoder.decode(buffer, reader, values, occupants);
			}
			/// delegate value encoder
			void encode_values(nst::buffer_type& buffer, nst::buffer_type::iterator& writer,const _Stored* values, count_t occupants) const {
				
				encoder.encode(buffer, writer, values, occupants);
			}
			/// delegate value encoder
			size_t encoded_values_size(const _Stored* values, count_t occupants) {
				
				return encoder.encoded_size(values, occupants);
			}

			 
			/// decode mixed run length and delta for simple sequences
			void decode(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader,_StoredRowId* keys, count_t occupants) const {
				count_t k = 0, m = occupants - 1;
				nst::i32 rl = 0;
				reader = keys[k++].read(buffer, reader);
				for(; k < occupants; ){
					rl = nst::leb128::read_signed(reader);
					if(!rl){
						rl = nst::leb128::read_signed(reader);
						count_t e = std::min<count_t>(k+rl,occupants);
						count_t j = k;
						_Rid prev = keys[j-1].get_value();
						for(; j < e; ++j){
							keys[j].set_value(prev + 1);
							prev = keys[j].get_value();
						}
						k = j;
					}else{
						keys[k].set_value(rl + keys[k-1].value);
						++k;

					}
				}
			}

			/// encoding mixes run length and delta encoding for simple sequences
			int encoded_size(const _StoredRowId* keys, count_t occupants) const {
				count_t k = 0, m = occupants - 1;
				nst::i32 rl = 0;
				int size = keys[k++].stored();

				for(; k < occupants; ++k){
					// works with keys[k] < 0 too
					if(keys[k].value - keys[k-1].value == 1 &&  k < m){
						// halts when k == m
						++rl;
					}else if(rl > 3){
						++rl;
						size += nst::leb128::signed_size((nst::i64)0);
						size += nst::leb128::signed_size((nst::i64)rl);
						rl = 0;
					}else{
						for(count_t j = k - rl; j <= k; ++j){ // k never < 1
							size += nst::leb128::signed_size((nst::i64)keys[j].value - keys[j-1].value);
						}
					}
				}
				return size;
			}

			void encode(nst::buffer_type::iterator& writer,const _StoredRowId* keys, count_t occupants) const {
				count_t k = 0, m = occupants - 1;
				nst::i32 rl = 0;
				writer = keys[k++].store(writer);

				for(; k < occupants; ++k){
					// works with keys[k] < 0 too
					if(keys[k].value - keys[k-1].value == 1 &&  k < m){
						// halts when k == m
						++rl;
					}else if(rl > 3){
						++rl;
						writer = nst::leb128::write_signed(writer, (nst::i64)0);
						writer = nst::leb128::write_signed(writer, (nst::i64)rl);
						rl = 0;
					}else{
						for(count_t j = k - rl; j <= k; ++j){ // k never < 1
							writer = nst::leb128::write_signed(writer, keys[j].value - keys[j-1].value);
						}
					}
				}
			}
			inline bool can(const _StoredRowId &first , const _StoredRowId &last, int size) const
			{				
				return ::abs((long long)size - (long long)::abs( (long long)last.get_value() - (long long)first.get_value() )) > 0 ;
			}
			inline unsigned int interpolate(const _StoredRowId &k, const _StoredRowId &first , const _StoredRowId &last, int size) const
			{
				if(last.get_value()>first.get_value())
					return (size*(k.get_value()-first.get_value()))/(last.get_value()-first.get_value());
				return 0;
			}

			void test_sequence(_StoredRowId *sequence,_StoredRowId *decoded,int _TestSize){
				nst::buffer_type output;
				output.resize(encoded_size(sequence, _TestSize));
				encode(output.begin(), sequence, _TestSize);
				decode(output.begin(), decoded, _TestSize);
				if(memcmp(sequence,decoded,sizeof(decoded))!=0){
					printf("Test failed\n");
				}
			}
			void test_encode(){
				if(1){
					const int _TestSize = 10;
					_StoredRowId sequence[_TestSize] = {0,1,2,3,4,5,6,7,8,9};
					_StoredRowId decoded[_TestSize];
					test_sequence(sequence, decoded, _TestSize);
				}
				if(2){
					const int _TestSize = 10;
					_StoredRowId sequence[_TestSize] = {0,1,2,3,5,6,7,8,9,10};
					_StoredRowId decoded[_TestSize];
					test_sequence(sequence, decoded, _TestSize);
				}
				if(3){
					const int _TestSize = 15;
					_StoredRowId sequence[_TestSize] = {0,2,4,8,10,11,12,13,14,15,17,18,20,21,22};
					_StoredRowId decoded[_TestSize];
					test_sequence(sequence, decoded, _TestSize);
					for(int i = 0 ; i < _TestSize; ++i){
						printf("%ld\n",decoded[i].get_value());
					}

				}
			}
		};
		typedef stx::btree_map<_StoredRowId, _Stored, stored::abstracted_storage,std::less<_StoredRowId>, int_terpolator> _ColMap;
	private:
		static const nst::u16 F_NOT_NULL = 1;
		static const nst::u16 F_CHANGED = 2;
		static const nst::u16 F_INVALID = 4;



		struct _StoredEntry{

			_StoredEntry(const _Stored& key):key(key){};//,flags(F_INVALID)

			_StoredEntry(){};//:flags(0)

			_StoredEntry &operator=(const _Stored& key){
				(*this).key = key;
				return *this;
			}

			inline operator _Stored&() {
				return key;
			}

			inline operator const _Stored&() const {
				return key;
			}

			void nullify(nst::u8& flags){

				flags &= (~F_NOT_NULL);
			}

			bool null(const nst::u8& flags) const {

				return ( ( flags & F_NOT_NULL ) == 0);
			}

			bool invalid(const nst::u8& flags) const {

				return ( ( flags & F_INVALID ) != 0);
			}

			bool valid(const nst::u8& flags) const {

				return ( ( flags & F_INVALID ) == 0);
			}

			void invalidate(nst::u8& flags){
				
				flags |= (F_NOT_NULL|F_INVALID);
			}

			void _D_validate(nst::u8& flags){
				flags &= (~F_INVALID);
			}
			_Stored key;
			//nst::u16 flags;


		};
		typedef std::vector<_StoredEntry, sta::col_tracker<_StoredEntry> > _Cache;

		typedef std::vector<nst::u8, sta::col_tracker<nst::u8> > _Flags;
		struct _CachePage{
		public:
			typedef stored::standard_entropy_coder<_Stored> _Encoded;
			_CachePage() : available(true), loaded(false),_data(nullptr),rows_cached(0),rows_start(0),density(1),users(0),flagged(false),loading(false){} ;//
			~_CachePage(){
				
				rows_cached = 0;
				rows_start = 0;
			}
		
			
			_CachePage(const _CachePage& right) throw() : available(true), loaded(false),_data(nullptr),rows_cached(0),rows_start(0),density(1),users(0),flagged(false),loading(false),data_size(0){				

				(*this) = right;
			}
			_CachePage& operator=(const _CachePage& right){
				printf(" _CachePage ASSIGN \n");
				encoded = right.encoded;
				data = right.data;
				_temp = right._temp;
				rows_cached = right.rows_cached;
				rows_start = right.rows_start;
				density = right.density;
				users = right.users;
				available = right.available;
				loaded = right.loaded;
				flags = right.flags;
				flagged = right.flagged;
				modified = right.modified;
				_data = data.empty()? nullptr : & data[0];
				access = right.access;
				modified = right.modified;
				loading = right.loading;
				(*this).data_size = data.size();
				return (*this);
			}
		public:
			bool available;
			bool loaded;
			bool loading;
			Poco::Mutex lock;
			Poco::AtomicCounter modified;
			Poco::AtomicCounter access;
			_Flags flags;

		private:
			
			_Encoded encoded;
			_Cache data;
			_StoredEntry * _data;
			_StoredEntry _temp;
			_Rid rows_cached;
			_Rid rows_start;			
			bool flagged;
			_Rid data_size;
		public:
            
			_Rid density;
			nst::i64 users;

			void resize(nst::i64 size){
				
				
				data.resize(size);
				flags.resize(MAX_PAGE_SIZE);
				rows_cached = (stored::_Rid)size;
				_data = data.empty()? nullptr : & data[0];
				(*this).data_size = data.size();
				
			}
			void invalidate(_Rid row){
				++modified;
				nst::u8 & flags = (*this).flags[row-rows_start];
				_temp.invalidate(flags);
			}
			void nullify(_Rid row){
				if(row < rows_start){
					printf("ERROR: invalid row\n");
					return;
				}
				nst::u8 & flags = (*this).flags[row-rows_start];
				_temp.nullify(flags);
			}
			void clear(){
				_Flags f;
                _Cache c;
				data.swap(c);
				flags.swap(f);
				encoded.clear();
				rows_start = 0;
				rows_cached = 0;
				_data = nullptr;
				available = true;
				/// loaded = false;				
				flagged = false;
				modified = 0;
				access = 0;
				
			}
			void encode(_Rid row){
				encoded.set(row-rows_start, data[row]);
			}

			_Rid size() const {
				return rows_cached;
			}
			bool included(_Rid row) const {
				_Rid at = row - rows_start;
				return (at < data_size);
			}
			_Stored& get(_Rid row)  {				
				_Rid at = row - rows_start;
				if(data_size <= at){
					printf("ERROR invalid data size or row\n");
				}
				if(encoded.good()){

					return encoded.get(at);;
				}else
					return _data[at].key;
			}
			const _Stored& get(_Rid row) const {
				
				_Rid at = row - rows_start;
				if(data_size <= at){
					printf("ERROR invalid data size or row\n");
				}
				if(encoded.good()){

					return encoded.get(at);
				}else
					return data[at].key;
			}

			void set_data(_Rid row, const _StoredEntry& d){
				if(row-rows_start < rows_cached){
					if(!data.empty())
						data[row-rows_start] = d;
				}
			}
			/// subscript operator
			const _Stored& operator[](_Rid at){ //  const {
				return get(at);
			}
			bool empty() const {
				return (encoded.empty() && data.empty());
			}
			_Rid get_v_row_count(_ColMap&col){
				_Rid r = 0;
				typename _ColMap::iterator e = col.end();
				typename _ColMap::iterator c = e;
				if(!col.empty()){
					--c;
					r = c.key().get_value() + 1;

				}
				return r;
			}
			void load_data(_ColMap &col,_Rid start, _Rid p_end){
				const _Rid CKECK = 1000000;
				_Rid end = std::min<_Rid>(p_end, get_v_row_count(col));
				if(start>=end){					
					return;
				}
				typename _ColMap::iterator e = col.lower_bound(end);
				typename _ColMap::iterator c = e;
				_Rid kv = 0;
				_Rid ctr = 0;
				_Rid prev = start;
				_Rid last = p_end - start;
				nst::u64 nulls = 0;
				nst::u64 use_begin = calc_use();
				
				for(c = col.lower_bound(start); c != e; ++c){
					if(ctr >= last){
						break;
					}
					kv = c.key().get_value();
					if(kv == 0){
						kv = c.key().get_value();
						c = col.lower_bound(start);
						kv = c.key().get_value();
					}
					/// nullify the gaps					
					if(end > kv){
						if(kv > 0){
							for(_Rid n = prev; n < kv-1; ++n){
								(*this).nullify(n);
								++nulls;
							}
						}

						(*this).set_data(kv, c.data());
						if(kv <start){
							printf("ERROR: kv invalid\n");
						}
						prev = kv;
					}
					ctr++;

				}
				if(kv > 0){
					for(_Rid n = prev; n < kv-1; ++n){
						(*this).nullify(n);
						++nulls;
					}
				}
				
				if(nulls == 0){
                    _Flags f;					
					(*this).flags.swap(f);
					
				}
				col.reduce_use();
				
				available = true;
				
			}
			void finish_decoded(_ColMap &col,const std::string &name,_Rid start, _Rid p_end){
				const bool treestore_only_encoded = false;
				/// printf("page: did not reduce %s from %.4g MB col pu: %.4g MB\n", name.c_str(), (double)calc_use() / units::MB, (double)col_page_use / units::MB);						
				if(treestore_only_encoded){
					encoded.clear();					
					clear();
					available = false;
				}else{
					resize(rows_cached);
					load_data(col,start,p_end);
					col_page_use += calc_use() ;
					
					//if(treestore_column_encoded)
					//	printf("page: could not reduce %s from %.4g MB\n", name.c_str(), (double)calc_use() / units::MB);
					available = true;
				};
				
			}
			void finish(_ColMap &col,const std::string &name,_Rid start, _Rid p_end){
				
				(*this).available = false;
				const _Rid CKECK = 1000000;
				_Rid end = std::min<_Rid>(p_end, get_v_row_count(col)+1);
				if(start>=end){					
					return;
				}
				(*this).rows_start = start;
				(*this).rows_cached = end-start;
				nst::i64 use_before = rows_cached * sizeof(_StoredEntry);
				nst::u64 used_by_encoding = 0;
				
				typename _ColMap::iterator e = col.lower_bound(end);
				typename _ColMap::iterator c = e;
				_Rid ctr = 0;
				nst::u64 use_begin = calc_use();
				if(treestore_column_encoded && encoded.applicable()){
					
					bool ok = true;
					for(c = col.lower_bound(start); c != e; ++c){
						if(ctr >= rows_cached){
							break;
						}
						/// _Rid r = c.key().get_value();
						(*this).encoded.sample(c.data());
						
						++ctr;

						if(used_by_encoding + encoded.total_bytes_allocated() > treestore_max_mem_use/2){							
							unload();
							return;
						}
					
					}
					col.reduce_use();
					if(ok){					
						(*this).encoded.finish(rows_cached);
					}
				}
				
				if(treestore_column_encoded && encoded.good()){
					_Rid test = 0;
					_Rid last = p_end - start;
					_Rid ctr = 0;
					
					for(c = col.lower_bound(start); c != e; ++c){
						if(ctr >= last){
							break;
						}
						_Rid r = c.key().get_value();
						if(r <= test){
							printf("WARNING: bad order in table detected\n");
						}
						test = r;
						(*this).encoded.set(r-start, c.data());
						++ctr;
					}
					
					(*this).encoded.optimize();

					if(calc_use() < use_before ){
						
						(*this).available = true;
						//printf("page: reduced %s from %.4g to %.4g MB col pu: %.4g MB\n", name.c_str(), (double)use_before / units::MB,  (double)calc_use()/ units::MB, (double)col_page_use / units::MB);
						col_page_use += calc_use() ;
					}else{
						finish_decoded(col,name,start,p_end);
					}

				}else{	
					finish_decoded(col,name,start,p_end);
				}				
				
			}
			nst::i64 calc_use(){
				if(encoded.good()){
					return encoded.capacity() + flags.capacity();
				}else{
					return data.capacity()* sizeof(_StoredEntry) + flags.capacity();
				}
			}

			void make_flags(){
				if(!flagged){
					NS_STORAGE::synchronized ll(lock);
					if(!flagged){
						flags.resize(MAX_PAGE_SIZE);
						flagged = true;
					}
				}
				
			}
			void unload(){
				clear();
				available = false;
                
			}
			inline bool has_flags() const {
				return flagged;
			}
			nst::u8 get_flags(_Rid row) const {
				if(flagged){
					if(!flags.empty())
						return flags[row - rows_start];
				}
				return 0;
			}
		};

		
		typedef std::vector<char, sta::col_tracker<char> >    _Nulls;
		static const _Rid MAX_PAGE_SIZE = 64*1024;///32768*32;
		typedef std::vector<_CachePage, sta::col_tracker<_CachePage> > _CachePages;

		struct _CachePagesUser{
			_CachePagesUser() : users(0){}
			_Rid users;
			_CachePages pages;
		};
		
		typedef std::map<std::string, std::shared_ptr<_CachePagesUser>, std::less<std::string>, sta::col_tracker<_CachePage> > _Caches; ///
		
		Poco::Mutex &get_mutex(){
			static Poco::Mutex m;
			return m;
		}

		_Caches & get_g_cache(){
			static _Caches _g_cache;
			return _g_cache;
		}
		void unload_cache(std::string name, _Rid last_logical_row){
				{
				_Rid p = last_logical_row/MAX_PAGE_SIZE+1;
				NS_STORAGE::synchronized ll(get_mutex());
			
				if(get_g_cache().count(name)!=0){

					std::shared_ptr<_CachePagesUser> user = get_g_cache()[name];
					typedef std::pair<_Rid, _CachePage*> _EvictionPair;
					typedef std::vector<_EvictionPair> _Evicted;
					_Evicted evicted;
					user->users--;	
					if(false && !user->users){
						if(user->pages.size()!=p){
							printf("resizing col '%s' from %lld to %lld\n",name.c_str(),(long long)user->pages.size(),(long long)p);
							user->pages.resize(p);
						}
						_Rid loaded = 0;
						for(_CachePages::iterator p = user->pages.begin(); p != user->pages.end(); ++p){
							
							if((*p).modified > MAX_PAGE_SIZE/8 || !((*p).available)){
								(*p).clear();
							}
							if((*p).loaded){
								++loaded;
								//evicted.push_back(std::make_pair((*p).access, &(*p)));
							}
						}
						std::sort(evicted.begin(),evicted.end());
						/// evict 5 percent least used or least recently used depending how access is specified
						_Rid remaining = evicted.size() / 20;
						for(_Evicted::iterator e = evicted.begin() ; e != evicted.end() && remaining > 0; ++e,--remaining){
							(*e).second->clear();
						}

					}
					

				}
				
				
			}
		}
		
		_CachePages* load_cache(std::string name, _Rid last_logical_row){
			_CachePages * result = nullptr;
				
			{
				NS_STORAGE::synchronized ll(get_mutex());
			
				if(get_g_cache().count(name)==0){

					std::shared_ptr<_CachePagesUser> user = std::make_shared<_CachePagesUser>();
					_Rid p = last_logical_row/MAX_PAGE_SIZE+1;
					user->pages.resize(p);
					get_g_cache()[name] = user;

				}
				std::shared_ptr<_CachePagesUser> user = get_g_cache()[name];
				user->users++;
				result = &(user->pages);
			}
		
			return result;
		}
	private:
		_ColMap col;
		typedef typename _ColMap::iterator _ColIter;
		typename _ColMap::iterator cend;
		typename _ColMap::iterator ival;
				
		_CachePages *pages;		
		_StoredEntry user;
		_Stored empty;
		_Rid cache_size;
		_Rid rows;
		_Rid rows_per_key;
		bool modified;
		bool lazy;
		nst::u32 sampler;
		inline bool has_cache() const {
			return pages != nullptr ;
		}

		void load_cache(){
			if(treestore_column_cache==FALSE || lazy) return;
			if(pages==nullptr && rows > 0){
				using namespace stored;
				pages = load_cache(storage.get_name(),rows); ///= new _CachePages(); ///
				//_Rid p = rows/MAX_PAGE_SIZE+1;
				//pages->resize(p);
				
			}
		}
		inline _CachePages& get_cache()  {

			return *pages;
		}

		const _CachePages& get_cache() const {

			return *pages;
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
			rows = get_v_row_count(); //(_Rid)col.size();
			load_cache();			
		}
		void uncheck_page_cache(){
			if(pages!=nullptr){
				//delete pages;				
				unload_cache(storage.get_name(),rows);
				pages = nullptr;
			}
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
		collumn(std::string name, bool load = false)
		:	storage(name)
		,	col(storage)
		,	pages(nullptr)
		,	rows(0)
		,	rows_per_key(0)
		,	modified(false)
		,	lazy(load)
		,	sampler(0)
		{
						
#ifdef _DEBUG

			//int_terpolator t;
			//t.test_encode();
#endif
		}

		~collumn(){
			
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
		_CachePage* load_page(_Rid requested, bool no_load = false){			
			_Rid i = (requested % MAX_PAGE_SIZE);
			_Rid l = requested - i;
			_Rid p = l/MAX_PAGE_SIZE;
			_CachePage * page = nullptr;
			++sampler;
			if( has_cache() && p < (*pages).size()){
				page = &((*pages)[p]);
				if(page->loaded){					
					if(page->included(requested))
						return page;
					return nullptr;
				}
				if(no_load){
					return nullptr;
				}
				if(page->loading){
					return nullptr;
				}

				NS_STORAGE::synchronizing synch(page->lock);
				if(!synch.isLocked()) return nullptr;
				if(page->loaded){					
					return nullptr;
				}				
				if(page->available){					
					page->loading = true;
					nst::u64 bytes_used = MAX_PAGE_SIZE * (sizeof(_StoredEntry) + 1);
					if(	(	
							nst::col_use + bytes_used > treestore_max_mem_use/2 
						)
						||	
						(
							treestore_current_mem_use + bytes_used > treestore_max_mem_use
						)
						){
						page->unload();							
						page->loading = false;					
						
					}else{
						page->finish(col,storage.get_name(),l,l+MAX_PAGE_SIZE);
						page->loaded = true;
						page->loading = false;					
					}
					return nullptr;
					
				}else{
					page->loading = false;
				}
				
			}
			return nullptr;
		}
		_Stored& seek_by_tree(_Rid row) {

			if(ival != cend){
				++ival;
				if(ival != cend && ival.key().get_value() == row){
					return ival.data();
				}
			}
			ival = col.find(row);


			if(ival == cend || ival.key().get_value() != row)
			{
				return empty;
			}
			return ival.data();
		}

		inline _Stored& seek_by_cache(_Rid row)  {
			
			if(treestore_column_cache && !lazy){
				_CachePage* page = load_page(row);
				if(page != nullptr){
					
					_Stored & se = page->get(row);
					//if( page->has_flags() ){
					//	nst::u8 flags = page->get_flags(row);
					//	if(user.valid(flags))
					//		return se;
//
//						if(user.null(flags))
//							return empty;
//					}else{
						return se;
//					}					
				}
			}
			
			return seek_by_tree(row);

		}

		void flush(){
			if(modified)
			{
				printf("flushing %s\n", storage.get_name().c_str());
				col.flush_buffers();
				//col.reduce_use();
				storage.commit();
			}
			modified = false;
		}

		void flush2(){
			if(modified){
				col.flush_buffers();

			}
		}

		void commit1(){
			
			if(modified){
				col.flush_buffers();

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
			check_page_cache();
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
		}
		ImplIterator<_ColMap> find(_Rid rid){
			return ImplIterator<_ColMap> (col, col.find(rid));
		}

		ImplIterator<_ColMap> begin(){
			return ImplIterator<_ColMap> (col, col.begin());
		}

		void erase(_Rid row){
			col.erase(row);
			_CachePage * page = load_page(row);
			if(page != nullptr){

				NS_STORAGE::synchronized synch(page->lock);
				
				page->make_flags();

				page->invalidate(row);
				
			}
		}

		void add(_Rid row, const _Stored& s){
			rows = std::max<_Rid>(row+1, rows);
			_CachePage * page = load_page(row,true);
			if(page != nullptr){

				NS_STORAGE::synchronized synch(page->lock);
				
				page->make_flags();

				page->invalidate(row);
			}
			col[row] = s;
			modified = true;
		}
	};



	static void test_ints(){
		typedef stored::DynamicKey _Sk;
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
