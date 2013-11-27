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
#include "fields.h"
#include "Poco/Mutex.h"
#include "Poco/Thread.h"
#include "Poco/ThreadPool.h"
#include "Poco/TaskManager.h"
#include "Poco/Task.h"
#include "NotificationQueueWorker.h"
namespace nst = NS_STORAGE;
namespace collums{

	typedef NS_STORAGE::u32 _Rid;

	class AbstractedIterator{
	public:
		AbstractedIterator(){
		}
		virtual ~AbstractedIterator(){
		}
		virtual bool valid(){
			return true;
		};
		virtual bool invalid(){
			return true;
		};
		virtual void next(){
		}
	};

	template <typename _MapType>
	class ImplIterator {
	private:
		typename _MapType::iterator i;
		typename _MapType::iterator iend;
		_MapType * map;

		_MapType &get_map(){
			return *map;
		}
	public:
		void check() const {
#ifdef _DEBUG
			if(map != i.context()){
				printf("context mismatch()\n");
			}
			if(map != iend.context()){
				printf("context mismatch()\n");
			}
#endif
		}

		typedef _MapType map_type;
		typedef typename _MapType::iterator::initializer_pair initializer_pair;
		typedef typename _MapType::key_type key_type;
		typedef typename _MapType::data_type data_type;
		typedef typename _MapType::iterator iterator_type;

		ImplIterator()
		:	map(NULL)
		{
		}

		ImplIterator(_MapType& map, typename _MapType::iterator i){
			(*this).map = &map;
			(*this).i = i;
			iend = get_map().end();
			check();
		}

		ImplIterator(const ImplIterator& right){
			*this = right;
			check();
		}

		ImplIterator& operator=(const ImplIterator& right){
			map = right.map;
			iend.clear();
			i.clear();
			iend = right.iend;
			i = right.i;
			check();
			return *this;
		}

		virtual ~ImplIterator(){
		}
		iterator_type& get_i(){
			return i;
		}
		void set_end(const ImplIterator& the_end){
			iend.clear();
			iend = the_end.i;
			check();
		}
		void seek(const typename _MapType::iterator& the_i){
			i = the_i;
		}
		void set(const typename _MapType::iterator& the_i,const typename _MapType::iterator& the_end, _MapType& amap){
			i.clear();
			iend.clear();
			i = the_i;
			iend = the_end;
			(*this).map = &amap;
			check();
		}

		void lower(const key_type &k){
			i = get_map().lower_bound(k);
			check();
		}
		void find(const key_type &k){
			check();
			i = get_map().find(k);
			iend = get_map().end();
			check();
		}
		void first(){
			check();
			i = get_map().begin();
			iend = get_map().end();
			check();
		}

		_Rid count(const ImplIterator &last){
			_Rid r = (_Rid)i.count(last.i);
			check();
			return r;
		}

		const key_type& get_key() const{
			return i.key();
		}

		data_type& get_value(){
			return i.data();
		}

		const data_type& get_value() const{
			return i.data();
		}

		bool valid(){
			check();
			return (i!=iend);
		};

		bool invalid(){
			check();
			return (i==iend);
		};

		initializer_pair get_initializer(){
			check();
			return i.construction();
		}

		void from_initializer(_MapType& amap, const initializer_pair& init){

			if(map != &amap){
				map = &amap;
				iend.clear();
				iend = get_map().end();
			}
			get_map().restore_iterator(i,init);
			check();
		}

		void next(){
			check();
			++i;
		}
		void previous(){
			--i;
		}
	};


	template<typename _Stored>
	class collumn {
	public:
		typedef typename stored::IntTypeStored<_Rid> _StoredRowId;
		stored::abstracted_storage storage;

		struct int_terpolator{

			/// function assumes a list of monotonously ascending integer values
			inline bool can(const _StoredRowId& first, const _StoredRowId& last, int size) const
			{
				return false; //size > 0;
			}

			inline bool asc() const
			{
				return true;
			}

			inline _StoredRowId diff(const _StoredRowId &larger, const _StoredRowId &smaller) const
			{
				_StoredRowId r = larger.get_value() - smaller.get_value();
				return r;
			}

			inline _StoredRowId add(const _StoredRowId &larger, const _StoredRowId &smaller) const
			{
				_StoredRowId r = larger.get_value() + smaller.get_value();
				return r;
			}

			inline unsigned int interpolate(const _StoredRowId &k, const _StoredRowId &first , const _StoredRowId &last, int size) const
			{
				if(last.get_value()>first.get_value())
					return (size*(k.get_value()-first.get_value()))/(last.get_value()-first.get_value());
				return 0;
			}
		};
		typedef stx::btree_map<_StoredRowId, _Stored, stored::abstracted_storage,std::less<_StoredRowId>, int_terpolator> _ColMap;
	private:
		static const nst::u16 F_NOT_NULL = 1;
		static const nst::u16 F_CHANGED = 2;
		static const nst::u16 F_INVALID = 4;
		struct _StoredEntry{

			_StoredEntry(const _Stored& key):key(key),flags(F_INVALID){};

			_StoredEntry():flags(0){};

			_StoredEntry &operator=(const _Stored& key){
				(*this).key = key;
				flags = F_NOT_NULL;
				return *this;
			}

			inline operator _Stored&() {
				return key;
			}

			inline operator const _Stored&() const {
				return key;
			}
			void nullify(){
				flags &= (~F_NOT_NULL);
			}
			bool null(){
				return ( ( flags & F_NOT_NULL ) == 0);
			}
			bool invalid() const {
				return ( ( flags & F_INVALID ) != 0);
			}
			bool valid() const {
				return ( ( flags & F_INVALID ) == 0);
			}
			void invalidate(){
				flags |= F_INVALID;
			}
			void validate(){
				flags &= (~F_INVALID);
			}
			_Stored key;
			nst::u16 flags;


		};
		typedef std::vector<_StoredEntry> _Cache;

		struct _CacheEntry{
			_CacheEntry() : available(false), loaded(false),density(1),users(0){};
			~_CacheEntry(){
				NS_STORAGE::remove_total_use(data.capacity()* sizeof(_Stored));
				
			}
			bool available;
			bool loaded;
			_Cache data;
			Poco::Mutex lock;
			_Rid density;
			nst::i64 users;
			void unload(){
				nst::synchronized _l(lock);
				//printf("unloading col cache\n");
				NS_STORAGE::remove_total_use(data.capacity()* sizeof(_Stored));
				loaded = false;
				available = false;
				data.clear();
				data.~_Cache();
				new (&data) _Cache();
				
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
				_Rid SAMPLE = (_Rid)data.size()/20;
				//printf("calc %ld density sample\n",(long)SAMPLE);
				for(_Rid r = 0;r<SAMPLE ; ++r){
					_Rid sample = (std::rand()*std::rand()) % data.size();
					uniques.insert(data[sample].key);
				}
				density = SAMPLE/std::max<_Rid>(1,(_Rid)uniques.size());
				if(density >= 2){
					density /= 2;
				}

			}
		};
		class ColLoader : public Poco::Notification{
		protected:
			std::string name;

			_CacheEntry* cache;
			size_t col_size;
			void calc_density(){
				/// get a 5% sample
				Density d;
				d.measure((*cache).data);
				(*cache).density = d.density;
				//printf("measured density sample: %lld\n", (nst::lld)(*cache).density);
			}
		protected:
			bool load_into_cache(size_t col_size){
				
				stored::abstracted_storage storage(name);
				storage.begin();
				storage.set_transaction_r(true);
				_ColMap col(storage);


				col.share(storage.get_name());
				//col.reload();
				_Rid ctr = 0;
				
				typename _ColMap::iterator e = col.end();
				typename _ColMap::iterator c = e;
				nst::u64 cached = 0;
				if(!col.empty()){
					--c;
					cached = std::max<size_t>(col_size ,c.key().get_value()+1);
					nst::u64 bytes_used = cached * sizeof(_Stored) ;
					NS_STORAGE::remove_total_use((*cache).data.capacity()* sizeof(_Stored));
					(*cache).data.clear();
					if(calc_total_use() + bytes_used > treestore_max_mem_use){
						//printf("ignoring col cache for %s\n", storage.get_name().c_str());
						(*cache).unload();
						return false;
					}
					
					(*cache).data.resize(cached);
					NS_STORAGE::add_total_use((*cache).data.capacity()* sizeof(_Stored));
				}
				const _Rid FACTOR = 50;
				_Rid prev = 0;
				for(c = col.begin(); c != e; ++c){
					_Rid kv = c.key().get_value();
					if(e != col.end()){
						e = col.end();
						
						printf("tx bug 1 iterator should stop %s\n",storage.get_name().c_str());
						break;
					}
					if(kv < ctr){
						printf("tx bug 2 iterator should stop  %s\n",storage.get_name().c_str());
						break;
					}
					if(cached <= kv){
						e = col.end();
						printf("tx bug 3 iterator should stop %s\n",storage.get_name().c_str());
						break;
					}
					/// nullify the gaps
					if((*cache).data.size() > kv){
						for(_Rid n = prev; n < kv; ++n){
							(*cache).data[n].nullify();
						}
						(*cache).data[kv] = c.data();
						prev = kv;
					}
					ctr++;

					if(cached > FACTOR){
						if(ctr % ( cached/ FACTOR ) ==0){
							col.reduce_use();
							//double MB = 1024.0*1024.0;
							//printf("mem use loading col %.4g MB\n", (double)(btree_totl_used + nst::total_use )/MB);
						}
					}
				}
				
				col.reduce_use();
				calc_density();
				storage.rollback();
				cache->available = true;


				return true;
			}
		public:
			ColLoader(std::string name,_CacheEntry * cache, size_t col_size)
			:	name(name)
			,	cache(cache)
			,	col_size(col_size)
			{
				cache->available = false;
				cache->loaded = true;

			}

			virtual void doTask(){
				(*this).load_into_cache(col_size);

			}
			virtual ~ColLoader(){

			}
		public:
			typedef Poco::AutoPtr<ColLoader> Ptr;
		};
		typedef std::vector<char>    _Nulls;

		typedef std::map<std::string, std::shared_ptr<_CacheEntry> > _Caches;
		typedef asynchronous::QueueManager<ColLoader> ColLoaderManager;
		static const int MAX_THREADS = 1;

		ColLoaderManager &get_loader(){
			static ColLoaderManager loader(MAX_THREADS);
			return loader;
		}
		Poco::Mutex &get_mutex(){
			static Poco::Mutex m;
			return m;
		}
		_Caches & get_g_cache(){
			static _Caches _g_cache;
			return _g_cache;
		}
		//static _Nulls _g_nulls;

		_CacheEntry* load_cache(std::string name, size_t col_size){
			_CacheEntry * result = 0;


			{
				NS_STORAGE::synchronized ll(get_mutex());
				if(calc_total_use()+col_size*sizeof(_Stored) > treestore_max_mem_use){
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
				get_loader().add(new ColLoader(name, result, col.size()));
			}

			return result;
		}
	private:
		_ColMap col;
		typename _ColMap::iterator cend;
		typename _ColMap::iterator ival;
		
		_CacheEntry * _cache;
		_Nulls * _nulls;
		_StoredEntry * cache_r;
		_Stored empty;
		_Rid cache_size;
		_Rid rows;
		nst::u32 rows_per_key;
		bool modified;
		inline bool has_cache() const {
			return _cache != nullptr && _cache->available;
		}
		void load_cache(){
			if(_cache==nullptr || !_cache->loaded){
				using namespace stored;
				if((calc_total_use()+col.size()*sizeof(_Stored)) < treestore_max_mem_use){
					
					_cache = load_cache(storage.get_name(),col.size());
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
			cache_r = nullptr;
			_cache = nullptr;
		}
		void engage_cache(){
			if(_cache != nullptr)
			{
				NS_STORAGE::synchronized slock(_cache->lock);
				if(_cache != nullptr && _cache->available)
				{
					if(!get_cache().data.empty()){
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
				NS_STORAGE::synchronized slock(_cache->lock);
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
			}
		}
		void check_cache(){
			if(has_cache()){
				NS_STORAGE::synchronized slock(_cache->lock);
				if(_cache->available)
				{
					
					cache_size = (_Rid)get_cache().data.size();

					if(cache_size)
						cache_r = &(get_cache().data[0]);

					if(cache_size != col.size())
					{
						
						_cache->unload();
						get_loader().add(new ColLoader(storage.get_name(), _cache, col.size()));
						reset_cache_locals();
					}
				}
			}
		}
	public:
		_Rid get_rows() const {
			return  (_Rid)col.size();
		}


		typedef ImplIterator<_ColMap> iterator_type;
		collumn(std::string name)
		:	storage(name)
		,	col(storage)
		
		,	_cache(nullptr)
		,	_nulls(nullptr)
		,	rows_per_key(0)
		,	modified(false)
		{
			rows = (_Rid)col.size();
			

		}

		~collumn(){
		}
		void initialize(bool by_tree){
			
			cend = col.end();
			ival = cend;
			
			
			//check_cache();


		}
		/// returns a sampled rows per key statistic for the collumn
		nst::u32 get_rows_per_key(){
			typedef std::set<_Stored> _Uniques;
			if(has_cache()){
				rows_per_key = get_cache().density;
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
		const _Stored& seek_by_cache(_Rid row)  {


			if( has_cache() && cache_size > row )
			{
				_StoredEntry & se = cache_r[row];
				if(se.valid())
					return se;
				
				if(se.null())
					return empty;

			}
			

			ival = col.find(row);

			if(ival != cend)
			{
				return ival.data();
			}

			return empty;
		}
		void flush(){
			if(modified)
			{
				printf("flushing %s\n", storage.get_name().c_str());
				col.flush();
				//col.reduce_use();
				storage.commit();
			}
			modified = false;
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

		void tx_begin(bool read){
			stored::abstracted_tx_begin(read, storage, col);	
			if(read){
				load_cache();
				check_cache();
				engage_cache();
			}
		}

		void rollback(){
			if(modified){
				//printf("releasing %s\n", storage.get_name().c_str());
				col.flush();

			}
			col.reduce_use();
			modified = false;
			storage.rollback();
			release_cache();
			reset_cache_locals();
		}

		void reduce_use(){
			
			//if(calc_total_use() > treestore_max_mem_use){
				col.reduce_use();
				release_cache();
				unload_cache();
				reset_cache_locals();
			//}
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
				if(has_cache() && get_cache().data.size() > row){

					get_cache().data[row].invalidate();
				}
				
			}
		}
		void add(_Rid row, const _Stored& s){
			rows = std::max<_Rid>(row+1, rows);
			if(has_cache()){
			
				NS_STORAGE::synchronized synch(get_cache().lock);
				if(has_cache() && get_cache().data.size() > row){

					get_cache().data[row].invalidate();
				}	
			}
			col[row] = s;
			modified = true;
		}
	};
	typedef stored::FTypeStored<float> FloatStored ;
	typedef stored::FTypeStored<double> DoubleStored ;
	typedef stored::IntTypeStored<short> ShortStored;
	typedef stored::IntTypeStored<NS_STORAGE::u16> UShortStored;
	typedef stored::IntTypeStored<NS_STORAGE::i8> CharStored;
	typedef stored::IntTypeStored<NS_STORAGE::u8> UCharStored;
	typedef stored::IntTypeStored<NS_STORAGE::i32> IntStored;
	typedef stored::IntTypeStored<NS_STORAGE::u32> UIntStored;
	typedef stored::IntTypeStored<NS_STORAGE::i64> LongIntStored;
	typedef stored::IntTypeStored<NS_STORAGE::u64> ULongIntStored;
	typedef stored::Blobule<false, 14> BlobStored;
	typedef stored::Blobule<true, 14> VarCharStored;


	class DynamicKey{
	public:

		enum StoredType{
			I1 = 1,
			I2 ,
			I4 ,
			I8 ,
			R4 ,
			R8 ,
			S 

		};

		typedef unsigned char	uchar;	/* Short for unsigned char */
		typedef signed char int8;       /* Signed integer >= 8  bits */
		typedef unsigned char uint8;    /* Unsigned integer >= 8  bits */
		typedef short int16;
		typedef unsigned short uint16;
		typedef int int32;
		typedef unsigned int uint32;
		typedef unsigned long	ulong;		  /* Short for unsigned long */
		typedef unsigned long long  ulonglong; /* ulong or unsigned long long */
		typedef long long longlong;
		typedef longlong int64;
		typedef ulonglong uint64;

		static const size_t MAX_BYTES = 2048;
		
		typedef _Rid row_type ;

		typedef NS_STORAGE::u16 _size_type;
		typedef NS_STORAGE::u16 _BufferSize;

	protected:
		std::string buf;
		_BufferSize size;// bytes used

		inline NS_STORAGE::u8* data(){

			return (NS_STORAGE::u8*)&buf[0];
		}
		inline const NS_STORAGE::u8* data() const{

			return (const NS_STORAGE::u8*)&buf[0];
		}
		inline NS_STORAGE::u8* _resize_buffer(_BufferSize nbytes){

			using namespace NS_STORAGE;
			if(buf.capacity() < nbytes){
				std::string empty;
				nst::i64 d = buf.capacity();
				if(d == empty.capacity())	
					d = 0;
				add_btree_totl_used (buf.capacity()-d);
			}
			buf.resize(nbytes);				
			return data();
		}
		NS_STORAGE::u8* _append( _BufferSize count ){
			
			NS_STORAGE::u8* r = _resize_buffer( size + count ) + size;
			size += count ;
			return r;
		}

		/// 1st level
		void addDynInt(nst::i64 v){
			*_append(1) = DynamicKey::I8;
			*(nst::i64*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::u64 v){
			*_append(1) = DynamicKey::I8;
			*(nst::u64*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::i32 v){
			*_append(1) = DynamicKey::I4;
			*(nst::i32*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::u32 v){
			*_append(1) = DynamicKey::I4;
			*(nst::u32*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::i16 v){
			*_append(1) = DynamicKey::I2;
			*(nst::i16*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::u16 v){
			*_append(1) = DynamicKey::I2;
			*(nst::u16*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::i8 v){
			*_append(1) = DynamicKey::I1;
			*(nst::i8*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::u8 v){
			*_append(1) = DynamicKey::I1;
			*(nst::u8*)_append(sizeof(v)) =  v;
		}
		void addDynReal(double v){
			*_append(1) = DynamicKey::R8;
			*(double*)_append(sizeof(v)) = v;
		}
		void addDynReal(float v){
			*_append(1) = DynamicKey::R4;
			*(float*)_append(sizeof(v)) = v;
		}
	public:	
		void add(const char* k, size_t s){
			size_t sad = s;
			*_append(1) = DynamicKey::S;
			( *(nst::u16*)_append(2)) = (nst::u16)sad; 
			if(sad > 0){
				memcpy(_append(sad), k, sad);
			}
		}
	public:	
		row_type row;
	public:
		/// 2nd level
		void addf8(double v){

			addDynReal(v);
		}

		void add1(char c){
			addDynInt(c);
			
		}

		void addu1(unsigned char c){

			addDynInt(c);
			
		}

		void addf4(float v){

			addDynReal(v);
		}

		void add8(long long v){
			addDynInt(v);
			
		}

		void addu8(unsigned long long v){

			addDynInt(v);
			
		}

		void add4(long v){

			addDynInt(v);
			
		}

		void addu4(unsigned int v){

			addDynInt(v);
			
		}

		void add2(short v){

			addDynInt(v);
		}

		void addu2(unsigned short v){

			addDynInt(v);
		}
		/// 3d level
		void add(const FloatStored & v){

			addf4(v.get_value());
		}

		void add(const DoubleStored & v){

			addf8(v.get_value());
		}
		void add(const ShortStored& v){

			add2(v.get_value());
		}

		void add(const UShortStored& v){

			addu2(v.get_value());
		}

		void add(const CharStored& v){

			add1(v.get_value());
		}

		void add(const UCharStored& v){

			addu1(v.get_value());
		}

		void add(const IntStored& v){

			add4(v.get_value());
		}

		void add(const UIntStored& v){

			addu4(v.get_value());
		}

		void add(const LongIntStored& v){

			add8(v.get_value());
		}

		void add(const ULongIntStored& v){

			addu8(v.get_value());
		}

		void add(const BlobStored& v){

			add(v.get_value(),v.get_size());
		}

		void add(const VarCharStored& v){

			add(v.get_value(),v.get_size()-1);
		}

		void addTerm(const char* k, size_t s){
			add(k, s);
		}

		void clear(){
			
			size = 0;
			row = 0;
			if(!buf.empty())
				data()[0] = 0;
		}

		~DynamicKey(){
			std::string empty;
			if(empty.capacity() != buf.capacity())
				remove_btree_totl_used (buf.capacity());

		}

		DynamicKey():size(0),row(0){
			
		}

		DynamicKey(const DynamicKey& right):size(0),row(0){
			*this = right;
		}

		inline bool left_equal_key(const DynamicKey& right) const {
			//if( size != right.size ) return false;
			if( size == 0 ) return false;

			int r = memcmp(data(), right.data(), std::min<_BufferSize>(size,right.size) );
			return r == 0;
		}
		
		inline bool equal_key(const DynamicKey& right) const {
			if( size != right.size ) return false;
			int r = memcmp(data(), right.data(), size );
			return r == 0;

		}

		DynamicKey& operator=(const DynamicKey& right){
			_resize_buffer(right.size);
			memcpy(data(), right.data(), right.size);

			size = right.size;
			row = right.row;
			return *this;
		}
		inline operator size_t() const {
			size_t r = 0;
			MurmurHash3_x86_32(data(), size, 0, &r);
			return r;
		}
		inline bool operator<(const DynamicKey& right) const {

			
			const nst::u8 *ld = data();
			const nst::u8 *rd = right.data();
			nst::u8 lt = *ld;
			nst::u8 rt = *rd;
			int r = 0,l=0,ll=0;	
			while(lt == rt){
				switch(lt){
				case DynamicKey::I1 :
					l=sizeof(nst::i8);
					if(*(nst::i8*)ld < *(nst::i8*)rd)
						return true;
					else if(*(nst::i8*)ld > *(nst::i8*)rd)
						return false;
					else r = 0;
					break;
				case DynamicKey::I2:
					l = sizeof(nst::i16);
					if(*(nst::i16*)ld < *(nst::i16*)rd)
						return true;
					else if(*(nst::i16*)ld > *(nst::i16*)rd)
						return false;
					else r = 0;
					break;
				case DynamicKey::I4:
					l = sizeof(nst::i32);
					if(*(nst::i32*)ld < *(nst::i32*)rd)
						return true;
					else if(*(nst::i32*)ld > *(nst::i32*)rd)
						return false;
					else r = 0;
					break;
				case DynamicKey::I8 :
					l = 8;
					if(*(nst::i64*)ld < *(nst::i64*)rd)
						return true;
					else if(*(nst::i64*)ld > *(nst::i64*)rd)
						return false;
					else r = 0;
					break;
				case DynamicKey::R4 :
					l = sizeof(float);					
					if(*(float*)ld < *(float*)rd)
						return true;
					else if(*(float*)ld > *(float*)rd)
						return false;
					else r = 0;

					break;
				case DynamicKey::R8 :
					l = sizeof(double);					
					if(*(double*)ld < *(double*)rd)
						return true;
					else if(*(double*)ld > *(double*)rd)
						return false;
					else r = 0; 
					break;
				case DynamicKey::S:

					r = memcmp(ld+2, rd+2, std::min<nst::i16>(*(const nst::i16*)rd, *(const nst::i16*)ld));
					if(r !=0) return r<0;
					if(*(const nst::i16*)rd != *(const nst::i16*)ld)
						return (*(const nst::i16*)rd < *(const nst::i16*)ld);
					break;
				};
				if( r != 0 ) break;
				++l;
				ld += l;
				rd += l;
				ll += l;
				if(ll >= size) break;
				if(ll >= right.size) break;
				lt = *ld;
				rt = *rd;
			}
			if(r != 0) return r < 0;
			if(size != right.size)
				return size < right.size;
			return (row < right.row);
		}

		inline bool operator!=(const DynamicKey& right) const {
			if(size != right.size) return true;
			int r = memcmp(data(), right.data(), size);
			if(r != 0) return true;
			return (row != right.row);
		}

		inline bool operator==(const DynamicKey& right) const {
			return !(*this != right);
		}

		NS_STORAGE::u32 stored() const {
			return NS_STORAGE::leb128::signed_size((*this).size)+(*this).size+NS_STORAGE::leb128::signed_size((*this).row);
		};

		NS_STORAGE::buffer_type::iterator store(NS_STORAGE::buffer_type::iterator w) const {
			using namespace NS_STORAGE;
			buffer_type::iterator writer = w;
			writer = leb128::write_signed(writer, row);
			writer = leb128::write_signed(writer, size);

			const u8 * end = data()+size;
			for(const u8 * d = data(); d < end; ++d,++writer){
				*writer = *d;
			}
			return writer;
		};

		NS_STORAGE::buffer_type::const_iterator read(NS_STORAGE::buffer_type::const_iterator r) {
			using namespace NS_STORAGE;
			buffer_type::const_iterator reader = r;
			row = leb128::read_signed(reader);
			size_t size = leb128::read_signed(reader);
			if(size > MAX_BYTES){
				size = MAX_BYTES;
			}
			u8 * end = _resize_buffer(size)+size;
			for(u8 * d = data(); d < end; ++d,++reader){
				*d = *reader;
			}
			
			(*this).size = size;
			return reader;
		};
	};
	void test_ints(){
		typedef collums::DynamicKey _Sk;
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

	class col_index{
	public:
		typedef stored::IntTypeStored<char> index_value;
		typedef DynamicKey index_key;//StaticKey
		stored::abstracted_storage storage;
		typedef stx::btree_map<index_key, index_value, stored::abstracted_storage> _IndexMap;
		typedef _IndexMap::iterator iterator_type;
		typedef ImplIterator<_IndexMap> IndexIterator;

		class IndexScanner : public Poco::Notification{
		protected:


			std::string name;
			
		protected:
			bool scan_index(){
				stored::abstracted_storage storage(name);
				storage.begin();
				storage.set_transaction_r(true);

				_IndexMap index(storage);
				index.share(storage.get_name());

				iterator_type s = index.begin();
				iterator_type e = index.end();
				nst::i64 ctr= 0;
				for(;s!=e;++s){
					ctr++;
					if(ctr % 100000ull == 0ull){
						//printf(" %lld items in index %s\n",ctr,storage.get_name().c_str());
					}
				}
				printf(" %lld items in index %s\n",(nst::lld)ctr,storage.get_name().c_str());
				storage.rollback();
				return true;
			}
		public:
			IndexScanner(std::string name)
			:	name(name)
			{

			}

			virtual void doTask(){

				(*this).scan_index();

			}

			virtual ~IndexScanner(){

			}
		public:
			typedef Poco::AutoPtr<IndexScanner> Ptr;
		};

		typedef asynchronous::QueueManager<IndexScanner> IndexScannerManager;

		IndexScannerManager &get_scanner(){
			static IndexScannerManager scanner(1);
			return scanner;
		}
	private:
		_IndexMap index;
		
		_IndexMap::iterator the_end;
		bool modified;
	private:

	public:

		col_index(std::string name)
		:	storage(name)
		,	index(storage)
	
		,	modified(false)
		{
			using namespace NS_STORAGE;
			//printf("sizeof(index_key):%lld\n",(u64)sizeof(index_key));
			the_end = index.end();
			index.share(name);

			//get_scanner().add(new IndexScanner(storage.get_name()));

		}

		~col_index(){
			storage.close();
		}
		void set_end(){
			the_end = index.end();
		}

		void reduce_use(){
			index.reduce_use();

		}
		void share(){

			//index.share(storage.get_name());
		}
		void unshare(){

		}

		void scan(){



		}

		std::string get_name() const {
			return storage.get_name();
		}
		IndexIterator first(){
			return IndexIterator (index, index.begin());
		}
		void from_initializer(IndexIterator& out, const _IndexMap::iterator::initializer_pair& ip){
			out.from_initializer(index, ip);
		}
		void lower_(IndexIterator& out,  const index_key& k){

			out.set(index.lower_bound(k),the_end,index);
		}

		IndexIterator lower(const index_key& k){
			return IndexIterator(index, index.lower_bound(k));
		}

		IndexIterator upper(const index_key& k){
			return IndexIterator(index, index.upper_bound(k));
		}

		IndexIterator end(){
			return IndexIterator (index, index.end());
		}

		IndexIterator find(const index_key& k){
			return IndexIterator (index, index.find(k));
		}

		void add(const index_key& k, const index_value& v){
			modified = true;
			index.insert(k,'0');
		}

		void remove(const index_key& k){
			modified = true;
			index.erase(k);
		}

		void flush(){
			index.reduce_use();
			index.flush();
			set_end();
		}
		void begin(bool read){
			stored::abstracted_tx_begin(read, storage, index);
			set_end();

		}

		void rollback(){
			index.reduce_use();
			if(modified)
				storage.rollback();
			modified = false;
		}

		void commit1(){
			if(modified){
				// this doesnt seem to work
				// index.flush();
				// !!! this seems to work
				index.reduce_use();
				
			}
		}

		void commit2(){
			if(modified)
				storage.commit();
			modified = false;

		}
	};
};
#endif
