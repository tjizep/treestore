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
	
	typedef std::vector<nst::u8> _RowData;

	struct _LockedRowData{
		std::vector<_RowData> rows;
		Poco::Mutex lock;
	};

	struct _RowDataCache{
		typedef std::shared_ptr<_LockedRowData> _SharedRowData;
		typedef std::unordered_map<std::string,_SharedRowData> _NamedRowData;

		_NamedRowData cache;
		Poco::Mutex lock;

		_LockedRowData* get_for_table(const std::string& name){
			nst::synchronized sync(lock);
			_SharedRowData result;
			_NamedRowData::iterator n = cache.find(name);
			if(n != cache.end()){
				result = (*n).second;
			}else{
				result = std::make_shared<_LockedRowData>();
				cache[name] = result;
			}
			return result.get();
		}
		void remove(const std::string& name){
			nst::synchronized sync(lock);
			_LockedRowData* rows = get_for_table(name);
			nst::synchronized sync_rows(rows->lock);
			rows->rows.clear();
		}
	};

	extern _LockedRowData* get_locked_rows(const std::string& name);

	extern void set_loading_data(const std::string& name, int loading);
	extern int get_loading_data(const std::string& name);

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

				flags |= F_INVALID;
			}

			void _D_validate(nst::u8& flags){
				flags &= (~F_INVALID);
			}
			_Stored key;
			//nst::u16 flags;


		};
		typedef std::vector<_StoredEntry> _Cache;

		typedef std::vector<nst::u8> _Flags;

		struct _CacheEntry{
			typedef stored::standard_entropy_coder<_Stored> _Encoded;
			_CacheEntry() : available(false), loaded(false),_data(nullptr),rows_cached(0),density(1),users(0){};//
			~_CacheEntry(){
				NS_STORAGE::remove_col_use(calc_use());
				rows_cached = 0;
			}
			bool available;
			bool loaded;

			_Flags flags;
		private:
			_Encoded encoded;
			_Cache data;
			_StoredEntry * _data;
			_StoredEntry _temp;
			_Rid rows_cached;
		public:
            Poco::Mutex lock;
			_Rid density;
			nst::i64 users;

			void resize(nst::i64 size){
				nst::u64 use_begin = calc_use();
				
				data.resize(size);
				flags.resize(size);
				rows_cached = (stored::_Rid)size;
				_data = & data[0];
				NS_STORAGE::remove_col_use(use_begin);
				NS_STORAGE::add_col_use(calc_use());
			}
			void invalidate(_Rid row){
				nst::u8 & flags = (*this).flags[row];
				_temp.invalidate(flags);
			}
			void nullify(_Rid row){
				nst::u8 & flags = (*this).flags[row];
				_temp.nullify(flags);
			}
			void clear(){
				nst::u64 use_begin = calc_use();
                _Flags f;
                _Cache c;
				data.swap(c);
				flags.swap(f);
				encoded.clear();
				nst::remove_col_use(use_begin);
				NS_STORAGE::add_col_use(calc_use());
			}
			void encode(_Rid row){
				encoded.set(row, data[row]);
			}

			_Rid size() const {
				return rows_cached;
			}

			_Stored& get(_Rid row)  {
				if(encoded.good()){

					return encoded.get(row);;
				}else
					return _data[row].key;
			}
			const _Stored& get(_Rid row) const {
				if(encoded.good()){

					return encoded.get(row);;
				}else
					return _data[row].key;
			}

			void set_data(_Rid row, const _StoredEntry& d){
				if(row < rows_cached){
					if(_data!=nullptr)
						_data[row] = d;
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
			void load_data(_ColMap &col){
				const _Rid CKECK = 1000000;
				typename _ColMap::iterator e = col.end();
				typename _ColMap::iterator c = e;
				_Rid kv = 0;
				_Rid ctr = 0;
				_Rid prev = 0;

				nst::u64 nulls = 0;
				nst::u64 use_begin = calc_use();
				for(c = col.begin(); c != e; ++c){
					kv = c.key().get_value();

					/// nullify the gaps					
					if((*this).size() > kv){
						if(kv > 0){
							for(_Rid n = prev; n < kv-1; ++n){
								(*this).nullify(n);
								++nulls;
							}
						}

						(*this).set_data(kv, c.data());
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
				nst::remove_col_use(use_begin);
				nst::add_col_use(calc_use());
			}
			void finish(_ColMap &col,const std::string &name){
				const _Rid CKECK = 1000000;
				
				rows_cached = get_v_row_count(col);
				nst::i64 use_before = rows_cached * sizeof(_StoredEntry);
				nst::u64 used_by_encoding = 0;
				typename _ColMap::iterator e = col.end();
				typename _ColMap::iterator c = e;
				_Rid ctr = 0;
				nst::u64 use_begin = calc_use();
				if(treestore_column_encoded){
					
					bool ok = true;
					for(c = col.begin(); c != e; ++c){
						_Rid r = c.key().get_value();
						encoded.sample(c.data());
						encoded.total_bytes_allocated();
						++ctr;

						if(used_by_encoding + encoded.total_bytes_allocated() > treestore_max_mem_use/2){							
							unload();
							return;
						}
						
					}
					col.reduce_use();
					if(ok){					
						encoded.finish(rows_cached);
					}
				}
				if(treestore_column_encoded && encoded.good()){

					for(c = col.begin(); c != e; ++c){
						_Rid r = c.key().get_value();
						encoded.set(r, c.data());
						
					}

					if(calc_use() < use_before ){
						
						encoded.optimize();
						NS_STORAGE::remove_col_use(use_begin);
						NS_STORAGE::add_col_use(calc_use());
						printf("reduced %s from %.4g to %.4g MB\n", name.c_str(), (double)use_before / units::MB,  (double)calc_use()/ units::MB);
					}else{
						printf("did not reduce %s from %.4g MB\n", name.c_str(), (double)use_before / units::MB);						
						encoded.clear();
						resize(rows_cached);
						load_data(col);
					}

				}else{					
					encoded.clear();					
					resize(rows_cached);
					load_data(col);
					if(treestore_column_encoded)
						printf("could not reduce %s from %.4g MB\n", name.c_str(), (double)use_before / units::MB);
					else
						printf("column use %s from %.4g MB\n", name.c_str(), (double)use_before / units::MB);
					
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
				if(flags.size() != rows_cached){
					NS_STORAGE::remove_col_use(flags.capacity());
					flags.resize(rows_cached);
					NS_STORAGE::add_col_use(flags.capacity());
				}
			}
			void unload(){
				nst::synchronized _l(lock);
				//printf("unloading col cache\n");
				nst::u64 use_begin = calc_use();
				
				loaded = false;
				available = false;
                _Cache c;
                _Flags f;
				data.swap(c);
				flags.swap(f);
				NS_STORAGE::remove_col_use(use_begin);
			}
		};

		class Density{
		public:
			Density(){
			}
			~Density(){
			}
			_Rid density;
			template<typename _DataType>
			void measure(_DataType& data){
				/// get a 5% sample
				typedef std::set<_Stored> _Uniques;
				_Uniques uniques;
				_Rid SAMPLE = (_Rid)data.size()/10;
				//printf("calc %ld density sample\n",(long)SAMPLE);
				for(_Rid r = 0;r<SAMPLE ; ++r){
					_Rid sample = (std::rand()*std::rand()) % data.size();
					uniques.insert(data[sample]);
				}
				density = SAMPLE/std::max<_Rid>(1,(_Rid)uniques.size());
				if(density >= 2){
					density /= 2;
				}

			}
		};

		class ColLoader : public  asynchronous::AbstractWorker{
		protected:
			std::string name;

			_CacheEntry* cache;
			size_t col_size;
			bool lazy;
			void _calc_density(){
				/// get a 5% sample
				Density d;
				d.measure((*cache));
				(*cache).density = d.density;
				//printf("measured density sample: %lld\n", (nst::lld)(*cache).density);
			}
		protected:

			bool load_into_cache(size_t col_size){

				stored::abstracted_storage storage(name);
				storage.begin();
				storage.set_reader();
				_ColMap col(storage);
				

				typename _ColMap::iterator e = col.end();
				typename _ColMap::iterator c = e;
				nst::u64 cached = 0;
				if(!col.empty()){
					--c;

					cached = std::max<size_t>(col_size ,c.key().get_value()+1);
					nst::u64 bytes_used = cached * (sizeof(_StoredEntry) + 1);
					
					(*cache).clear();
					if(calc_total_use() + bytes_used > treestore_max_mem_use){
						//printf("ignoring col cache for %s\n", storage.get_name().c_str());
						(*cache).unload();
						return false;
					}

				}
				//printf("load %s start system use %.4g MB\n", storage.get_name().c_str(), (double)calc_total_use()/ units::MB);

				(*cache).finish(col,storage.get_name());
				/// _calc_density();
				storage.rollback();

				col.reduce_use();
				storage.reduce();
				cache->available = true;
				//printf("load %s end system use %.4g MB\n", storage.get_name().c_str(), (double)calc_total_use()/ units::MB);

				return true;
			}
		public:

			ColLoader(std::string name,_CacheEntry * cache, bool lax, size_t col_size)
			:	name(name)
			,	cache(cache)
			,	col_size(col_size)
			,	lazy(false)
			{
				cache->available = false;
				cache->loaded = true;
				set_loading_data(name, 1);
			}

			virtual void work(){

				try{
					(*this).load_into_cache(col_size);
				}catch(std::exception&){
					printf("Error during col cache loading/decoding\n");
				}
				set_loading_data(name, 0);


			}
			virtual ~ColLoader(){

			}

		};
		typedef std::vector<char>    _Nulls;

		typedef std::map<std::string, std::shared_ptr<_CacheEntry> > _Caches;

		Poco::Mutex &get_mutex(){
			static Poco::Mutex m;
			return m;
		}

		_Caches & get_g_cache(){
			static _Caches _g_cache;
			return _g_cache;
		}

		void unload_cache(std::string name){
			_CacheEntry * entry= 0;


			{
				NS_STORAGE::synchronized ll(get_mutex());
				if(get_g_cache().count(name)==0){

					return;

				}
				entry = get_g_cache()[name].get() ;
				NS_STORAGE::synchronized slock(entry->lock);
				if(entry != nullptr && entry->available)
				{
					if(entry->users == 0){
						printf("releasing col cache %s\n", storage.get_name().c_str());
						entry->unload();
					};
				}
			}

		}
		_CacheEntry* load_cache(std::string name, bool lazy, size_t col_size){
			_CacheEntry * result = 0;
				
			{
				NS_STORAGE::synchronized ll(get_mutex());
				if(calc_total_use() + col_size*sizeof(_Stored) > treestore_max_mem_use){
					return result;
				}
				if(get_g_cache().count(name)==0){

					std::shared_ptr<_CacheEntry> cache = std::make_shared<_CacheEntry>();

					get_g_cache()[name] = cache;

				}
				result = get_g_cache()[name].get() ;
			}

			NS_STORAGE::synchronized slock(result->lock);
			if(!result->loaded){

				result->loaded = true;
				//ColLoader l(name, result, lazy, col.size());
				//l.doTask();

				using namespace storage_workers;

				get_threads( get_next_counter() ).add(new ColLoader(name, result, lazy, col.size()));

			}

			return result;
		}
	private:
		_ColMap col;
		typedef typename _ColMap::iterator _ColIter;
		typename _ColMap::iterator cend;
		typename _ColMap::iterator ival;
		_CacheEntry * _cache;
		_Nulls * _nulls;

		nst::u8 * cache_f;
		_StoredEntry user;
		_Stored empty;
		_Rid cache_size;
		_Rid rows;
		nst::u32 rows_per_key;
		bool modified;
		bool lazy;

		inline bool has_cache() const {
			return _cache != nullptr && _cache->available;
		}

		void load_cache(){
			if(treestore_column_cache==FALSE) return;

			if(lazy) return;
			if(_cache==nullptr || !_cache->loaded){
				using namespace stored;
				if((calc_total_use()+col.size()*sizeof(_StoredEntry)) < treestore_max_mem_use){

					_cache = load_cache(storage.get_name(),lazy,col.size());
				}
			}
		}
		inline _CacheEntry& get_cache()  {

			return *_cache;
		}

		const _CacheEntry& get_cache() const {

			return *_cache;
		}
		void reset_cache_locals(){
			cache_size = 0;
			cache_f = nullptr;
			_cache = nullptr;
			ival = col.end();
		}
		void engage_cache(){
			if(_cache != nullptr)
			{
				NS_STORAGE::synchronized slock(_cache->lock);
				if(_cache != nullptr && _cache->available)
				{
					if(!get_cache().empty()){
						_cache->users++;
						return;
					}
				}
			}
			reset_cache_locals();
		}

		void release_cache(){
			if(_cache != nullptr)
			{
				NS_STORAGE::synchronized crit(_cache->lock);
				if(_cache != nullptr && _cache->available)
				{
					_cache->users--;
				}
			}

		}
		void unload_cache(){
			if(_cache != nullptr)
			{
				NS_STORAGE::synchronized slock(_cache->lock);
				if(_cache != nullptr && _cache->available)
				{
					if(_cache->users==0){
						printf("releasing col cache %s\n", storage.get_name().c_str());
						_cache->unload();
					};
				}
			}else{
				unload_cache(storage.get_name());
			}
		}
		void check_cache(){
			if(has_cache()){
				NS_STORAGE::synchronized slock(_cache->lock);
				if(_cache->available)
				{

					cache_size = (_Rid)get_cache().size();

					if(cache_size){
						if(get_cache().flags.empty()){
							cache_f = nullptr;
						}else{
							cache_f = &(get_cache().flags[0]);
						}
					}
					if(cache_size != col.size())
					{

						//_cache->unload();
						//get_loader().add(new ColLoader(storage.get_name(), _cache, col.size()));
						//reset_cache_locals();
					}
				}
			}
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

		,	_cache(nullptr)
		,	_nulls(nullptr)
		,	rows_per_key(0)
		,	modified(false)
		,	lazy(load)
		{
			rows = (_Rid)col.size();

#ifdef _DEBUG

			int_terpolator t;
			t.test_encode();
#endif
		}

		~collumn(){
		}

		void initialize(bool by_tree){

			cend = col.end();
			ival = cend;
			if(!modified){
				load_cache();
				check_cache();
				engage_cache();
			}

			//check_cache();


		}
		/// returns a sampled rows per key statistic for the collumn
		nst::u32 get_rows_per_key(){
			typedef std::set<_Stored> _Uniques;
			if(has_cache()){
				//rows_per_key = get_cache().density;
			}
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

		const _Stored& seek_by_tree(_Rid row) {

			return seek_by_cache(row);
		}

		inline _Stored& seek_by_cache(_Rid row)  {


			if( has_cache() && cache_size > row )
			{
				_Stored & se = _cache->get(row);

				if(nullptr != cache_f ){
					nst::u8 flags = cache_f[row];
					if(user.valid(flags))
						return se;

					if(user.null(flags))
						return empty;
				}else{
					return se;
				}

			}
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
				col.reduce_use();

			}
			release_cache();
			reset_cache_locals();
		}

		void commit2(){
			if(modified){

				storage.commit();
			}
			modified = false;
		}

		void tx_begin(bool read,bool shared= true){
			stored::abstracted_tx_begin(read,shared, storage, col);

		}

		void rollback(){
			if(modified){
				col.flush();
			}

			col.reduce_use();
			modified = false;			
			release_cache();
			reset_cache_locals();
			storage.rollback();
		}

		void reduce_cache_use(){
			bool tx = storage.is_transacted();
			release_cache();
			unload_cache();
			reset_cache_locals();
			if(!tx)
				storage.rollback();
		}

		void reduce_tree_use(){
			bool tx = storage.is_transacted();
			col.reduce_use();
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
			if((*this).rows > 0) --(*this).rows;

			if(has_cache()){

				NS_STORAGE::synchronized synch(get_cache().lock);
				if(has_cache() && get_cache().size() > row){
					get_cache().make_flags();

					get_cache().invalidate(row);
				}
			}
		}

		void add(_Rid row, const _Stored& s){
			rows = std::max<_Rid>(row+1, rows);
			if(has_cache()){

				NS_STORAGE::synchronized synch(get_cache().lock);
				if(has_cache() && get_cache().size() > row){
					get_cache().make_flags();
					get_cache().invalidate(row);
				}
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
