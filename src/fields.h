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
#ifndef CO_STORE_CEP20130823
#define CO_STORE_CEP20130823
#ifdef _MSC_VER
#include <Windows.h>
#endif

#include <stx/storage/basic_storage.h>
#include <bit_symbols.h>
#include <stx/btree.h>
#include <stx/btree_map>

#include <stx/btree_map.h>

#include <stx/btree_multimap>
#include <stx/btree_multimap.h>

#include <stx/btree_set>

#include "transactional_storage.h"
#include "MurmurHash3.h"
#include <map>
#include <vector>
#ifdef _MSC_VER
#include <conio.h>
#endif
#include "abstracted_storage.h"
#include "suffix_array.h"


#ifdef _MSC_VER
#define CONVERSION_NOINLINE_ /// _declspec(noinline)
#else
#define CONVERSION_NOINLINE_
#endif

extern	char treestore_column_cache ;
extern  char treestore_column_encoded ;
extern  char treestore_predictive_hash ;
extern  char treestore_reduce_tree_use_on_unlock;


namespace NS_STORAGE = stx::storage;
namespace nst = stx::storage;
typedef NS_STORAGE::synchronized synchronized;
namespace stored{

	static const double	MB = 1024.0f*1024.0f;
	static const double	GB = 1024.0f*1024.0f*1024.0f;

	inline double units(NS_STORAGE::u64 in, double unit){
		return in / unit;
	}

	struct empty_encoder{
		inline bool encoded(bool multi) const
		{
			return false;
		}
		typedef nst::u16 count_t;
		/// empty value decoder
		template<typename _Stored>
		void decode(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader,_Stored* values, count_t occupants) const {			
		}

		/// empty value encoder
		template<typename _Stored>
		void encode(nst::buffer_type& buffer, nst::buffer_type::iterator& writer,const _Stored* values, count_t occupants) const {			
		}

		/// byte size of encoded buffer for given data values
		template<typename _Stored>
		size_t encoded_size(const _Stored* values, count_t occupants) const {
			return 0;
		}
		
	};
	
	struct integer_encoder{
	public:
		typedef nst::u16 count_t;
	private:
		
		
	public:

		integer_encoder(){
			
		}
		 
		inline bool encoded(bool multi) const
		{
			return true;
		}
		
		/// decoder
		template<typename _Stored>
		void decode(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader,_Stored* values, count_t occupants) {			
			size_t bytes = occupants * sizeof(_Stored);
			nst::u8* dest = (nst::u8*)values;
			const nst::u8* src = (const nst::u8*)&(*reader);
			memcpy(dest, src, bytes);

			reader+=bytes;
		}

		/// encoder
		template<typename _Stored>
		void encode(nst::buffer_type& buffer, nst::buffer_type::iterator& writer,const _Stored* values, count_t occupants) const {			
			size_t bytes = occupants * sizeof(_Stored);
			//nst::u8* dest = &(buffer[0]);
			const nst::u8* src = (const nst::u8*)values;
			//memcpy(dest, src, bytes);
			for(size_t b = 0; b < bytes; ++b){
				(*writer) = src[b];
				++writer;
			}
			//writer+=bytes;
		}

		/// byte size of encoded buffer for given data values
		template<typename _Stored>
		size_t encoded_size(const _Stored* values, count_t occupants) {
			size_t bytes = occupants * sizeof(_Stored);
			return bytes;
		}
		
	};
	template<typename _IntType>
	class IntTypeStored {

	public:
		typedef integer_encoder list_encoder;
		typedef _IntType value_type;
		_IntType value;
	public:
		size_t total_bytes_allocated() const {
			return sizeof(*this);
		}
		inline _IntType get_value() const {

			return value;
		}
		inline void set_value(_IntType nv) {
			value = nv;
		}

		explicit IntTypeStored(_IntType i):value(i){
		}
		IntTypeStored():value(0) { 
		}
		IntTypeStored (const _IntType& init):value(init){
		}
		IntTypeStored (const IntTypeStored& init) : value(init.value){
		}
		IntTypeStored& operator=(const IntTypeStored& right)  {
			value = right.value;
			return *this;
		}
		inline bool operator<(const IntTypeStored& right) const {
			return (value < right.value);
		}
		inline bool operator>(const IntTypeStored& right) const {
			return (value > right.value);
		}
		inline bool operator!=(const IntTypeStored& right) const {
			return (value != right.value);
		}
		inline bool operator==(const IntTypeStored& right) const {
			return (value == right.value);
		}

		NS_STORAGE::u32 stored() const {
			return NS_STORAGE::leb128::signed_size((NS_STORAGE::i64)value);
		};
		NS_STORAGE::buffer_type::iterator store(NS_STORAGE::buffer_type::iterator writer) const {
			return NS_STORAGE::leb128::write_signed(writer, (NS_STORAGE::i64)value);
		};
		NS_STORAGE::buffer_type::const_iterator read(const NS_STORAGE::buffer_type& buffer, NS_STORAGE::buffer_type::const_iterator r) {
			NS_STORAGE::buffer_type::const_iterator reader = r;
			value = (value_type)NS_STORAGE::leb128::read_signed64(reader,buffer.end());
			return reader;
		};
		size_t get_hash() const{
			return (size_t)value;
		}
	};
	
	
	
	template<typename _FType>
	class FTypeStored {
	public:
		typedef empty_encoder list_encoder;
		typedef _FType value_type;		
	protected:
		_FType value;
	public:
		size_t total_bytes_allocated() const {
			return sizeof(*this);
		}
		inline _FType get_value() const {
			return value;
		}
		inline void set_value(_FType nv) {
			value = nv;
		}
		FTypeStored() : value((_FType)0.0f) { 
		}
		FTypeStored (const _FType& init):value(init){
		}
		FTypeStored (const FTypeStored& init) : value(init.value){
		}
		FTypeStored& operator=(const FTypeStored& right) {
			value = right.value;
			return *this;
		}
		inline bool operator<(const FTypeStored& right) const {
			return (value < right.value);
		}
		inline bool operator>(const FTypeStored& right) const {
			return (value < right.value);
		}
		inline bool operator!=(const FTypeStored& right) const {
			return (value != right.value);
		}
		inline bool operator==(const FTypeStored& right) const {
			return (value == right.value);
		}
		NS_STORAGE::u32 stored() const {
			return sizeof(value);
		};
		NS_STORAGE::buffer_type::iterator store(NS_STORAGE::buffer_type::iterator w) const {
			NS_STORAGE::buffer_type::iterator writer = w;
			memcpy((NS_STORAGE::u8*)&(*writer), &value, sizeof(value));
			writer += sizeof(value);
			return writer;
		};
		NS_STORAGE::buffer_type::const_iterator read(const NS_STORAGE::buffer_type& buffer,NS_STORAGE::buffer_type::const_iterator r) {
			
			NS_STORAGE::buffer_type::const_iterator reader = r;
			if(reader != buffer.end()){
				memcpy(&value, (const NS_STORAGE::u8*)&(*reader), sizeof(value));
				reader += sizeof(value);
			}
			return reader;
		};
		size_t get_hash() const{
			return (size_t)value;
		}
	};

	template<typename _FType>
	class VectorTypeStored {
	public:
		typedef empty_encoder list_encoder;
		typedef _FType value_type;
	protected:
		_FType value;
	public:

		inline _FType get_value(){
			return value;
		}
		VectorTypeStored(){
		}
		VectorTypeStored (const _FType& init):value(init){
		}

		VectorTypeStored (const VectorTypeStored& init) : value(init.value){
		}

		VectorTypeStored& operator=(const VectorTypeStored& right) {
			value = right.value;
			return *this;
		}

		bool operator<(const VectorTypeStored& right) const {
			return (value < right.value);
		}

		bool operator!=(const VectorTypeStored& right) const {
			return (value != right.value);
		}

		NS_STORAGE::u32 stored() const {
			return NS_STORAGE::leb128::signed_size(value.size())+ value.size()*sizeof(_FType::value_type);
		};

		NS_STORAGE::buffer_type::iterator store(NS_STORAGE::buffer_type::iterator w) const {
			NS_STORAGE::buffer_type::iterator writer =  NS_STORAGE::leb128::write_signed(w, value.size());
			memcpy((NS_STORAGE::u8*)writer, &value, value.size()*sizeof(_FType::value_type));
			writer += value.size();
			return writer;
		};

		NS_STORAGE::buffer_type::const_iterator read(const NS_STORAGE::buffer_type& buffer, NS_STORAGE::buffer_type::const_iterator r) {
			NS_STORAGE::buffer_type::iterator reader = r;
			if(reader != buffer.end()){
				value.resize(NS_STORAGE::leb128::read_signed(reader));
				memcpy(&value, (NS_STORAGE::u8*)reader, value.size()*sizeof(_FType::value_type));
				reader += value.size();
			}
			return reader;
		};
		size_t get_hash() const{
			return (size_t)value;
		}
	};
	template<bool CHAR_LIKE, int _MConstSize = 16>
	class Blobule {
	protected:
		static const int _ConstSize = _MConstSize;
	public:
		typedef empty_encoder list_encoder;
		static const int CHAR_TYPE = CHAR_LIKE ? 1 : 0;
	protected:
		
		typedef NS_STORAGE::u16 _BufferSize;
		static const size_t max_buffersize = 1 << (sizeof(_BufferSize) << 3);
		_BufferSize bytes;// bytes within dyn or static buffer
		_BufferSize size;// bytes used
		NS_STORAGE::u8 buf[_ConstSize];

		inline NS_STORAGE::u8 *extract_ptr(){
			return (NS_STORAGE::u8*)(*(size_t*)(buf));
		}
		inline const NS_STORAGE::u8 *extract_ptr() const {
			return (const NS_STORAGE::u8*)(*(const size_t*)(buf));
		}
		NS_STORAGE::u8* data(){
			null_check();
			if(bytes <= _ConstSize){
				return buf;
			}
			return extract_ptr();
		}
		const NS_STORAGE::u8* data() const{
			null_check();
			if(bytes <= _ConstSize){
				return buf;
			}
			return extract_ptr();
		}
		void null_check() const {
			
		}
		NS_STORAGE::u8* dyn_resize_buffer(NS_STORAGE::u8* r,size_t mnbytes){
			null_check();
			using namespace NS_STORAGE;
			if(mnbytes > 32768){
				printf("WARNING: large buffer\n");
			}
			size_t nbytes = std::min<size_t>(mnbytes, max_buffersize);
			NS_STORAGE::u8 * nbuf = (NS_STORAGE::u8 *)allocation_pool.allocate(nbytes);
			memcpy(nbuf, r, std::min<size_t>(nbytes, size));
			if(bytes > _ConstSize){
				allocation_pool.free(r,bytes);				
			}
			memcpy(buf, &nbuf, sizeof(u8*));
			bytes = (u32)nbytes;
			return nbuf;
		}
		NS_STORAGE::u8* _resize_buffer(size_t mnbytes){
			null_check();
			using namespace NS_STORAGE;
			size_t nbytes = std::min<size_t>(mnbytes, max_buffersize);
			NS_STORAGE::u8* r = data();
  			if(nbytes > bytes){
				r = dyn_resize_buffer(r,nbytes);
			}
			return r;
		}
		void _add( const void * v, size_t count, bool end_term = false){
			null_check();
			_BufferSize extra = end_term ? 1 : 0;
			_resize_buffer(count + extra);
			memcpy(data(),v,count);
			if(end_term) data()[count] = 0;
			size+=(_BufferSize)count + extra;
		}
		bool less(const Blobule& right) const {
			null_check();
			using namespace NS_STORAGE;

			const u8 * lp = data();
			const u8 * rp = right.data();
			int r = memcmp(lp,rp,std::min<_BufferSize>(size,right.size));
			if(r != 0){
				return (r < 0);
			}

			return (size < right.size);
		}
		bool greater(const Blobule& right) const {
			null_check();
			using namespace NS_STORAGE;

			const u8 * lp = data();
			const u8 * rp = right.data();
			int r = memcmp(lp,rp,std::min<_BufferSize>(size,right.size));
			if(r != 0){
				return (r > 0);
			}

			return (size > right.size);
		}
		bool not_equal(const Blobule& right) const {
			null_check();
			using namespace NS_STORAGE;
			if(size != right.size){
				return true;
			}
			const u8 * lp = data();
			const u8 * rp = right.data();
			int r = memcmp(lp,rp,std::min<_BufferSize>(size,right.size));
			if(r != 0){
				return true;
			}

			return size != right.size;

		}
	public:
		size_t total_bytes_allocated() const {
			if(bytes > _ConstSize)
				return sizeof(*this) + bytes;
			return sizeof(*this);
		}
		char * get_value(){
			null_check();
			return chars();
		}
		const char * get_value() const {
			null_check();
			return chars();
		}
		char * chars(){
			
			return (char *)data();
		}
		const char * chars() const {
			return (const char *)data();
		}
		void add(){
			_add(NULL, 0);
		}
		void add(NS_STORAGE::i64 val){
			using namespace NS_STORAGE;
            NS_STORAGE::i64 v = val;
			_add(&v, sizeof(v));

		}
		void add(float v){
			_add(&v, sizeof(v));
		}
		void add(double v){
			_add(&v, sizeof(v));
		}

		void add(const char * nt,size_t n){
			_add(nt,(_size_type)n);
		}
		void addterm(const char * nt,size_t n){
			_add(nt,(_size_type)n,true);
		}
		void add(const char * nt){
			_add(nt,strlen(nt)+1);
		}
		void set(const char * nt){
			size = 0;
			_add(nt,strlen(nt)+1);
		}
		void set(NS_STORAGE::i64 v){
			size = 0;
			add(v);
		}
		void set(float v){
			size = 0;
			add(v);
		}
		void set(double v){
			size = 0;
			add(v);
		}
		void set(const char * nt,size_t n){
			size = 0;
			_add(nt,(int)n);
		}
		void setterm(const char * nt,size_t n){
			size = 0;
			_add(nt,(int)n,true);
		}
		void set(){
			size = 0;
			add();
		}

		Blobule ()
		:	bytes(_ConstSize)
		,	size(0)
		{
			//memset(buf,0,sizeof(buf));
			buf[0] = 0;
		}
		inline ~Blobule(){
			if(bytes > _ConstSize){				
				allocation_pool.free(data(), bytes)	;	
			}
			
			/// bytes = _ConstSize;
			/// size = 0;
			/// buf[0] = 0;
		}

		Blobule (const Blobule& init)
		:	bytes(_ConstSize)
		,	size(0)
		{
			*this = init;
		}
		Blobule(const char *right)
		:	bytes(_ConstSize)
		,	size(0)
		{
			*this = right;
		}

		Blobule(const NS_STORAGE::i64& right)
		:	bytes(_ConstSize)
		,	size(0)
		{
			set(right);

		}
		Blobule(const double& right)
		:	bytes(_ConstSize)
		,	size(0)
		{
			set(right);

		}

		inline Blobule& operator=(const Blobule& right)
		{
			using namespace NS_STORAGE;
			if(right.size > _ConstSize){
				u8 * d = _resize_buffer(right.size);
				size = right.size;
				memcpy(d,right.data(),size);//right.size+1
			}else{
				size = right.size;
				if(size)
					memcpy(data(), right.data(), _ConstSize);//right.size+1
			}


			return *this;
		}

		Blobule& operator=(const char *right) {
			set(right);
			return *this;
		}
		Blobule& operator=(const NS_STORAGE::i64& right) {
			set(right);
			return *this;
		}
		Blobule& operator=(const double& right) {
			set(right);
			return *this;
		}
		inline bool operator>(const Blobule& right) const {
			return greater(right);
		}
		inline bool operator<(const Blobule& right) const {
			return less(right);
		}

		inline bool operator!=(const Blobule& right) const {

			return not_equal(right);
		}
		inline bool operator==(const Blobule& right) const {

			return !not_equal(right);
		}
		inline size_t  get_size() const {
			return size;
		}
		NS_STORAGE::u32 stored() const {
			return NS_STORAGE::leb128::signed_size((*this).size)+(*this).size;
		};
		size_t get_hash() const{
			nst::u32 h = 0;
			MurmurHash3_x86_32(data(), (nst::u32)get_size(), 0, &h);
			return h;
		}
		NS_STORAGE::buffer_type::iterator store(NS_STORAGE::buffer_type::iterator w) const {
			using namespace NS_STORAGE;
			if(size > 32768){
				printf("WARNING: large buffer %lld\n",(long long)size);
			}
			buffer_type::iterator writer = w;
			writer = leb128::write_signed(writer, size);
			const u8 * end = data()+size;
			for(const u8 * d = data(); d < end; ++d,++writer){
				*writer = *d;
			}
			return writer;
		};

		NS_STORAGE::buffer_type::const_iterator read(const NS_STORAGE::buffer_type& buffer, NS_STORAGE::buffer_type::const_iterator r) {
			using namespace NS_STORAGE;
			buffer_type::const_iterator reader = r;
			
			if(reader != buffer.end()){
				bool fast = false;
				
				if(fast){

					size = *reader;

					if(size <= 8){
						++reader;
						const nst::u8 * d = &(*reader);	
						*((nst::u64*)data()) = *((const nst::u64*)d);					
					}else{
						size = leb128::read_signed(reader);				
						_resize_buffer(size);				
						memcpy(data(),&(*reader),size);						
					}
					reader += size;
				}else{
					size_t s = leb128::read_signed(reader);				

					_resize_buffer(s);
					const u8 * end = data()+s;
				
					for(u8 * d = data(); d < end ; ++d,++reader){
						*d = *reader;
					}				
					size = s;
				}
			}
			return reader;
		};
	};

	template <typename _Hashed>
	struct fields_hash
	{
		size_t operator()(_Hashed const & x) const 
		{
			return x.get_hash();
		}
	};



};
namespace stored{
	typedef NS_STORAGE::u64 _Rid;
	static const _Rid MAX_ROWS = 0xFFFFFFFFul;

	typedef std::vector<_Rid,sta::col_tracker<_Rid> > _Parts;
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
	typedef stored::Blobule<false, 24> BlobStored;
	typedef stored::Blobule<true, 22> VarCharStored;

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

		typedef std::vector<nst::u8, sta::pool_alloc_tracker<nst::u8> > _Data;

		static const size_t MAX_BYTES = 2048;

		typedef _Rid row_type ;

		typedef NS_STORAGE::u16 _BufferSize;

	protected:

		static const size_t static_size = sizeof(_Data)*8;
		static const size_t max_buffer_size = 1 << (sizeof(_BufferSize) << 3);

		nst::u8 buf[static_size];
		_BufferSize bs;// bytes used

		inline _Data& get_Data(){
			return *(_Data*)&buf[0];
		}
		inline const _Data& get_Data() const {
			return *(const _Data*)&buf[0];
		}
		inline nst::u8* data(){
			if(bs==static_size)
				return &(get_Data())[0];
			return &buf[0];
		}
		inline const nst::u8* data() const{
			if(bs==static_size)
				return &(get_Data())[0];
			return &buf[0];
		}
		inline nst::u8* _resize_buffer(size_t mnbytes){

			using namespace NS_STORAGE;
			size_t nbytes = std::min<size_t>(mnbytes, max_buffer_size);
			if(nbytes >= static_size && bs < nbytes){
				bs = static_size;
				new (&get_Data()) _Data();
			}else
				bs = (_BufferSize)nbytes;
			if(bs==static_size){
				if(get_Data().capacity() < nbytes){
					nst::i64 d = get_Data().capacity();
					get_Data().resize(nbytes);					
				}else
					get_Data().resize(nbytes);
			}

			return data();
		}
		NS_STORAGE::u8* _append( size_t count ){
			size_t os = size();			
			NS_STORAGE::u8* r = _resize_buffer( os + count ) + os;

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
		size_t total_bytes_allocated() const {
			if(bs==static_size)
				return sizeof(*this) + get_Data().capacity();
			return sizeof(*this);
		}
		int size() const {
			if(bs==static_size)
				return (int)get_Data().size();
			return bs;
		}
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
			addDynInt((nst::i64)v);

		}

		void addu8(unsigned long long v){

			addDynInt((nst::u64)v);

		}

		void add4(long v){

			addDynInt((nst::i32)v);

		}

		void addu4(unsigned int v){

			addDynInt((nst::i32)v);

		}

		void add2(short v){

			addDynInt((nst::i16)v);
		}

		void addu2(unsigned short v){

			addDynInt((nst::u16)v);
		}
		/// 3rd level
		void add(const stored::FloatStored & v){

			addf4(v.get_value());
		}

		void add(const stored::DoubleStored & v){

			addf8(v.get_value());
		}
		void add(const stored::ShortStored& v){

			add2(v.get_value());
		}

		void add(const stored::UShortStored& v){

			addu2(v.get_value());
		}

		void add(const stored::CharStored& v){

			add1(v.get_value());
		}

		void add(const stored::UCharStored& v){

			addu1(v.get_value());
		}

		void add(const stored::IntStored& v){

			add4(v.get_value());
		}

		void add(const stored::UIntStored& v){

			addu4(v.get_value());
		}

		void add(const stored::LongIntStored& v){

			add8(v.get_value());
		}

		void add(const stored::ULongIntStored& v){

			addu8(v.get_value());
		}

		void add(const stored::BlobStored& v){

			add(v.get_value(),v.get_size());
		}

		void add(const stored::VarCharStored& v){

			add(v.get_value(),v.get_size());
		}

		void addTerm(const char* k, size_t s){
			add(k, s);
		}

		void clear(){

			row = 0;
			if(bs==static_size){
				get_Data().clear();
			}else{
				bs = 0;
				//memset(buf, 0,sizeof(buf));
			}
		}

		~DynamicKey(){
			if(bs==static_size){				
				get_Data().~_Data();				
			}
			//bs = 0;
			//row = 0;
		}

		DynamicKey():
			bs(0),
			row(0)
		{
			//memset(buf, 0,sizeof(buf));
		}

		DynamicKey(const DynamicKey& right):
			bs(0),
			row(0)
		{
			//memset(buf, 0,sizeof(buf));
			*this = right;
		}

		inline bool left_equal_key(const DynamicKey& right) const {
			//if( size != right.size ) return false;
			if( size() == 0 ) return false;

			int r = memcmp(data(), right.data(), std::min<size_t>(size(),right.size()) );
			return r == 0;
		}

		inline bool equal_key(const DynamicKey& right) const {
			if( size() != right.size() ) return false;
			int r = memcmp(data(), right.data(), size() );
			return r == 0;

		}

		DynamicKey& operator=(const DynamicKey& right){
			if(right.bs < static_size){
				bs = right.bs;
				memcpy(buf, right.buf, bs);
			}else{
				_resize_buffer(right.size());
				memcpy(data(), right.data(), right.size());
			
			}
			row = right.row;
			return *this;
		}
		DynamicKey& return_or_copy(DynamicKey& ){
			return *this;
		}
		inline operator size_t() const {
			size_t r = 0;
			MurmurHash3_x86_32(data(), size(), 0, &r);
			return r;
		}
		template<typename _IntType>
		bool p_decode(_IntType& out, int n = 1) const {
			const nst::u8 *ld = data();

			nst::u8 lt = *ld;

			int l=0;
			while(n > 0){
				--n;
				++ld;
				switch(lt){
				case DynamicKey::I1 :
					l=sizeof(nst::i8);
					if(!n){
						out = (_IntType)(*(nst::i8*)ld )	;

					}
					break;
				case DynamicKey::I2:
					l = sizeof(nst::i16);
					if(!n){
						out = (_IntType)(*(nst::i16*)ld );

					}
					break;
				case DynamicKey::I4:
					l = sizeof(nst::i32);
					if(!n){
						out = (_IntType)(*(nst::i32*)ld);

					}
					break;
				case DynamicKey::I8 :
					l = 8;
					if(!n){
						out = (_IntType)(*(nst::i64*)ld);

					}
					break;
				case DynamicKey::R4 :
					l = sizeof(float);
					if(!n){
						out = (_IntType)(*(float*)ld );
						return true;
					}
					break;
				case DynamicKey::R8 :
					l = sizeof(double);
					if(!n){
						out = (_IntType)(*(double*)ld);
						return true;
					}
					break;

				case DynamicKey::S:
					l = *(const nst::i16*)ld;
					if(!n){
						out = 0;
						return false;
					}
					break;
				};
				ld += l;
				lt = *ld;
			};
			return true;
		}

		bool operator<(const DynamicKey& right) const {

			/// partitions the order in a hierarchy
			const nst::u8 *ld = data();
			const nst::u8 *rd = right.data();
			nst::u8 lt = *ld;
			nst::u8 rt = *rd;

			int r = 0,l=0,ll=0;
			while(lt == rt){
				++ld;
				++rd;
				++ll;
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
					l = sizeof(nst::i64);
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

				ld += l;
				rd += l;
				ll += l;
				if(ll >= size()) break;
				if(ll >= right.size()) break;
				lt = *ld;
				rt = *rd;
			}
			if(rt != lt)
				return lt < rt; //total order on types first
			if(r != 0)
				return r < 0;
			if(size() != right.size())
				return size() < right.size();
			return (row < right.row);
		}

		inline bool operator!=(const DynamicKey& right) const {
			if(size() != right.size()) return true;
			int r = memcmp(data(), right.data(), size());
			if(r != 0) return true;
			return (row != right.row);
		}

		inline bool operator==(const DynamicKey& right) const {
			return !(*this != right);
		}

		NS_STORAGE::u32 stored() const {
			return (NS_STORAGE::u32)(NS_STORAGE::leb128::signed_size((*this).size())+(*this).size()+NS_STORAGE::leb128::signed_size((*this).row));
		};

		NS_STORAGE::buffer_type::iterator store(NS_STORAGE::buffer_type::iterator w) const {
			using namespace NS_STORAGE;
			buffer_type::iterator writer = w;
			writer = leb128::write_signed(writer, row);
			writer = leb128::write_signed(writer, size());

			
			memcpy(&(*writer),data(),size());
			writer += size();
			//const u8 * end = data()+size();
			/*for(const u8 * d = data(); d < end; ++d,++writer){
				*writer = *d;
			}*/
			return writer;
		};

		NS_STORAGE::buffer_type::const_iterator read(const NS_STORAGE::buffer_type& buffer, NS_STORAGE::buffer_type::const_iterator r) {
			using namespace NS_STORAGE;
			buffer_type::const_iterator reader = r;
			if(reader != buffer.end()){
				row = leb128::read_signed(reader);
				size_t s = leb128::read_signed(reader);
				//memcpy(_resize_buffer(s), &(*reader), s);
				//reader += s;
				u8 * end = _resize_buffer(s)+s;
				for(u8 * d = data(); d < end; ++d,++reader){
					*d = *reader;
				}
			}

			return reader;
		};
	};
	template<typename _FieldType>
	class PrimitiveDynamicKey{
	public:
		typedef typename _FieldType::value_type _DataType ;

		
		typedef std::vector<nst::u8> _Data;

		static const size_t MAX_BYTES = 2048;

		typedef _Rid row_type ;

		typedef NS_STORAGE::u8 _size_type;
		typedef NS_STORAGE::u8 _BufferSize;

	protected:

		_DataType data;

		/// 1st level
		void addDynInt(nst::i64 v){
			data = v;
		}
		void addDynInt(nst::u64 v){
			data = v;
		}
		void addDynInt(nst::i32 v){
			data = v;
		}
		void addDynInt(nst::u32 v){
			data = v;
		}
		void addDynInt(nst::i16 v){
			data = v;
		}
		void addDynInt(nst::u16 v){
			data = v;
		}
		void addDynInt(nst::i8 v){
			data = v;
		}
		void addDynInt(nst::u8 v){
			data = v;
		}
		void addDynReal(double v){
			data = v;
		}
		void addDynReal(float v){
			data = v;
		}
	public:
		void add(const char* k, size_t s){

		}
	public:
		row_type row;
	public:
		int size() const {
			return sizeof(data);
		}
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
			addDynInt((nst::i64)v);

		}

		void addu8(unsigned long long v){

			addDynInt((nst::u64)v);

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
		/// 3rd level
		void add(const stored::FloatStored & v){

			addf4(v.get_value());
		}

		void add(const stored::DoubleStored & v){

			addf8(v.get_value());
		}
		void add(const stored::ShortStored& v){

			add2(v.get_value());
		}

		void add(const stored::UShortStored& v){

			addu2(v.get_value());
		}

		void add(const stored::CharStored& v){

			add1(v.get_value());
		}

		void add(const stored::UCharStored& v){

			addu1(v.get_value());
		}

		void add(const stored::IntStored& v){

			add4(v.get_value());
		}

		void add(const stored::UIntStored& v){

			addu4(v.get_value());
		}

		void add(const stored::LongIntStored& v){

			add8(v.get_value());
		}

		void add(const stored::ULongIntStored& v){

			addu8(v.get_value());
		}

		void add(const stored::BlobStored& v){

			add(v.get_value(),v.get_size());
		}

		void add(const stored::VarCharStored& v){

			add(v.get_value(),v.get_size());
		}

		void addTerm(const char* k, size_t s){
			add(k, s);
		}

		void clear(){
			row = 0;
			data = 0;
		}

		~PrimitiveDynamicKey(){
			row = 0;
			data = 0;
		}

		PrimitiveDynamicKey():
			data(0),row(0)
		{

		}

		PrimitiveDynamicKey(const PrimitiveDynamicKey& right):
			data(0),row(0)
		{
			*this = right;
		}
		PrimitiveDynamicKey(const DynamicKey& right)

		{
			row =right.row;
			right.p_decode(data);
		}
		inline bool left_equal_key(const DynamicKey& right) const {
			_DataType rdata;
			right.p_decode(rdata);
			return data == rdata;
		}

		inline bool left_equal_key(const PrimitiveDynamicKey& right) const {

			return data == right.data;
		}

		inline bool equal_key(const PrimitiveDynamicKey& right) const {
			if( data == right.data)
				return row == right.row;
			return false;

		}

		PrimitiveDynamicKey& operator=(const PrimitiveDynamicKey& right){
			data = right.data;
			row = right.row;
			return *this;
		}

		PrimitiveDynamicKey& operator=(const DynamicKey& right){
			row =right.row;
			right.p_decode(data);
			return *this;
		}

		DynamicKey& return_or_copy(DynamicKey& x){
			x.clear();
			_FieldType f;
			f.set_value(data);
			x.add(f);
			x.row = (*this).row;
			return x;
		}
		inline operator size_t() const {
			//size_t r = 0;
			//MurmurHash3_x86_32(&data, sizeof(data), 0, &r);
			//return r;
			return (size_t)data;
		}


		bool operator<(const PrimitiveDynamicKey& right) const {
			if( data == right.data)
				return row < right.row;

			return (data < right.data);
		}

		inline bool operator!=(const PrimitiveDynamicKey& right) const {
			if( data == right.data)
				return row != right.row;

			return data != right.data;
		}

		inline bool operator==(const PrimitiveDynamicKey& right) const {
			return !(*this != right);
		}

		NS_STORAGE::u32 stored() const {
			_FieldType f;
			f.set_value(data);

			DynamicKey s;
			s.add(f);
			s.row = (*this).row;
			return s.stored();
		};

		NS_STORAGE::buffer_type::iterator store(NS_STORAGE::buffer_type::iterator w) const {
			using namespace NS_STORAGE;
			_FieldType f;
			DynamicKey s;
			f.set_value((*this).data);
			s.add(f);
			s.row = (*this).row;
			return s.store(w);
		};

		NS_STORAGE::buffer_type::const_iterator read(const NS_STORAGE::buffer_type& buffer, NS_STORAGE::buffer_type::const_iterator r) {
			using namespace NS_STORAGE;
			buffer_type::const_iterator reader = r;
			DynamicKey s;
			reader = s.read(buffer, reader);
			s.p_decode(data);
			row = s.row;
			return reader;
		};
	};

	static const nst::u32 MAX_HIST = 1 << 31;
	static const nst::u32 MAX_ENTROPY = 1 << 31;
	

		template<typename _IntType>
		struct entropy_t{
			typedef _IntType _Sampled;
			typedef std::unordered_map<_Sampled, nst::u64, fields_hash<_Sampled>, std::equal_to<_Sampled>, ::sta::col_tracker<_Sampled> > _UnsortedHistogram;
			/// typedef ::google::dense_hash_map<_Sampled, nst::u64, fields_hash<_Sampled>, std::equal_to<_Sampled>> _UnsortedHistogram;
			/// typedef std::map<_Sampled, nst::u64, std::less<_Sampled>, ::sta::col_tracker<_Sampled> > _UnsortedHistogram;
			typedef std::map<_Sampled, nst::u64, std::less<_Sampled>, ::sta::col_tracker<_Sampled> > _Histogram;
			typedef std::vector<_Sampled, ::sta::col_tracker<_Sampled> > _UniqueSamples; //
		protected:
			nst::u64 samples;
			nst::u64 bytes_allocated;
			_UnsortedHistogram histogram;
			_Histogram sorted_histogram;
			_UniqueSamples unique;
			void clear_sorted(){
				_Histogram sorted_histogram;
				(*this).sorted_histogram.swap(sorted_histogram);
				_UniqueSamples u;
				(*this).unique.swap(u);
			}
			void clear_unsorted(){
				_UnsortedHistogram h;
				
				(*this).histogram.swap(h);
				_UniqueSamples u;
				(*this).unique.swap(u);
			}
		public:
			_UniqueSamples& get_unique_samples(){
				return unique;
			}
			const _UniqueSamples& get_unique_samples() const {
				return unique;
			}
			_Histogram& get_sorted_histogram(){
				if(sorted_histogram.empty()){
					for(_UnsortedHistogram::iterator u = histogram.begin(); u!=histogram.end();++u){
						sorted_histogram[(*u).first] = (*u).second;
					}
					//clear_unsorted();
				}
				return sorted_histogram;
			}
			const _UnsortedHistogram& get_unsorted_histogram() const {
				return histogram;
			}
			_UnsortedHistogram& get_unsorted_histogram() {
				return histogram;
			}
			const _Histogram& get_sorted_histogram() const {				
				return sorted_histogram;
			}

			size_t total_bytes_allocated() const {
				return bytes_allocated;
			}
			entropy_t():samples(0){
				bytes_allocated = sizeof(*this);
			}
			void clear(){
				samples = 0;
				bytes_allocated = sizeof(*this);
				
				clear_unsorted();
				clear_sorted();
			}
			void sample(const _IntType& data){
				if(!sorted_histogram.empty()){
					clear_sorted();
				}
				size_t before = histogram.size();
				_UnsortedHistogram::iterator u = histogram.find(data);
				if(u == histogram.end()){
					++histogram[data];
					(*this).unique.push_back(data.get_value());
				}else{
					++((*u).second);
				}
				
				++samples;
				if(before < histogram.size()){
					bytes_allocated += data.total_bytes_allocated();
				}
			}

			nst::u64 get_samples() const {
				return samples;
			}

			nst::u64 get_entropy(){
				return histogram.size();
			}
					
		};

		template<typename _IntType>
		struct int_entropy_t : public entropy_t<_IntType>{
			
			
			typename _IntType::value_type get_max_val()  const {
				if(histogram.empty())
					return 0;
				size_t l = get_unique_samples().size();
				nst::i64 high =get_at(0);
				for (size_t i = 1; i < l; i++) {
					if(high < get_at(i)){
						high = get_at(i);
					}
				}
				return (_IntType::value_type) high;

			}
			
			typename _IntType::value_type get_min_val()  const {
				if(histogram.empty())
					return 0;
				size_t l = get_unique_samples().size();
				nst::i64 low =get_at(0);
				for (size_t i = 1; i < l; i++) {
					if( get_at(i) < low ){
						low = get_at(i);
					}
				}
				return (_IntType::value_type)low;

			}
			nst::i64 get_at(size_t at)const{
				return get_unique_samples()[at].get_value();
			}
			typename nst::u64 get_abs()  {
				if(get_unique_samples().empty())
					return 0;
				size_t l = get_unique_samples().size();
				nst::i64 low  = get_at(0),high = get_at(0);
 
				size_t start = (l & 1) ? 1 : 0;
				
 
				for (size_t i = start; i < l; i+=2) {
 
					if (get_at(i) < get_at(i+1)) {
						if (low > get_at(i)) {
							low = get_at(i);
						}
						if (high < get_at(i+1)) {
							high = get_at(i+1);
						}
					} else {
						if (low > get_at(i+1)) {
							low = get_at(i+1);
						}
						if (high < get_at(i)) {
							high = get_at(i);
						}
					}
				}								
				return labs(high-low);
			}
		};


		class UninitializedCodeException : public std::exception{
		public :
			/// the code has not been initialized
			UninitializedCodeException() throw(){
				printf("*********** UninitializedCodeException\n");
			}
			~UninitializedCodeException() throw(){};
		};

		/// class for fixed size entropy coded buffer
		template<typename _IntType>
		struct int_fix_encoded_buffer{
			typedef nst::u32 _CodeType;
			typedef symbol_vector<_CodeType, sta::tracker<_CodeType, sta::col_counter> > _Symbols;

			typedef entropy_t<_IntType> _Entropy;
			struct hash_fixed{
				size_t operator()(const _IntType& i) const {
					return i.get_hash();
				}
			};
            typedef std::unordered_map<_IntType, _CodeType, fields_hash<_IntType>, std::equal_to<_IntType>, sta::col_tracker<_IntType> > _CodeMap;
			/// typedef ::google::dense_hash_map<_IntType, _CodeType, fields_hash<_IntType>, std::equal_to<_IntType> > _CodeMap;
			
			/// typedef std::map<_IntType, _CodeType, std::less<_IntType>, sta::col_tracker<_IntType> > _CodeMap;
			typedef std::vector<_IntType, sta::col_tracker<_IntType> > _DeCodeMap;

			_Entropy stats;
			_CodeMap codes;
			_DeCodeMap decodes;

			_Symbols symbols;
			_IntType _null_val;
			_CodeType code_size;

			int_fix_encoded_buffer() : code_size(0){

			}

			size_t capacity() const {
				size_t is = sizeof(_IntType);
				size_t bs = sizeof(_CodeType);
				return (is+bs)*codes.size()+is*decodes.capacity()+symbols.capacity();
			}

			_Entropy& get_stats(){
				return stats;
			}
			const _Entropy& get_stats() const {
				return stats;
			}

			void clear(){
                _CodeMap c;
                _DeCodeMap d;
				code_size = 0;
				stats.clear();
				codes.swap(c);
				decodes.swap(d);
				symbols.clear();
			}

			/// Find smallest X in 2^X >= value
			

			bool empty() const {
				return code_size == 0;
			}

			void resize(_Rid rows){
				if(code_size==0) throw UninitializedCodeException();
				symbols.resize(rows);

			}


			void encode(_Rid row, const _IntType &val){
				if(codes.count(val)){
					_CodeType code = codes[val];
					symbols.set(row, code);

				}else
					throw UninitializedCodeException();
			}

			const _IntType& decode(_Rid row) const {
				_CodeType code = 0;
				if(code_size > 0){
					code = symbols.get(row);
					
					return decodes[code];
					
				}else
					throw UninitializedCodeException();
				return _null_val;
			}
			_IntType& decode(_Rid row) {
				_CodeType code = 0;
				if(code_size > 0){
					code = symbols.get(row);
					
					return decodes[code];
					
				}else
					throw UninitializedCodeException();
				return _null_val;
			}
			bool initialize(_Rid rows){

				if( ( (double)stats.get_samples() / (double)stats.get_entropy() ) > 1 && stats.get_entropy() < MAX_ENTROPY){
					nst::u32 words = 0;
					{
						typename _Entropy::_Histogram::iterator h = (*this).stats.get_sorted_histogram().begin();
						
						decodes.resize((*this).stats.get_sorted_histogram().size());

						for(;h != (*this).stats.get_sorted_histogram().end();++h){
							codes[(*h).first] = words;						
							decodes[words] = (*h).first;
							++words;
						}
					}
					(*this).stats.clear();

					if(words > 0){

						code_size = std::max<_CodeType>(1, bits::bit_log2((_CodeType)words));
						symbols.set_code_size(code_size);

						resize(rows);
					}


				}
				return code_size > 0;

			}
			void optimize(){
                _CodeMap c;
				(*this).codes.swap(c);
				(*this).stats.clear();
			}
		};	/// fixed in encoded buffer

		/// class for fixed size entropy coded buffer
		template<typename _IntType>
		struct int_fix_unmapped_encoded_buffer{
			typedef nst::u32 _CodeType;
			typedef symbol_vector<_CodeType,  sta::tracker<_CodeType, sta::col_counter> > _Symbols;

			typedef int_entropy_t<_IntType> _Entropy;
			struct hash_fixed{
				size_t operator()(const _IntType& i) const {
					return i.get_hash();
				}
			};
            //typedef std::unordered_map<_IntType, _CodeType, fields_hash<_IntType> > _CodeMap;
			typedef std::map<_IntType, _CodeType, std::less<_IntType>, sta::tracker<_IntType, sta::col_counter> > _CodeMap;
			typedef std::vector<_IntType, sta::tracker<_IntType, sta::col_counter> > _DeCodeMap;

			_Entropy stats;
			_CodeMap codes;
			_DeCodeMap decodes;

			_Symbols symbols;
			_IntType _null_val;
			_CodeType code_size;
			typename _IntType::value_type min_decoded;
			mutable _IntType unmapped;
			int_fix_unmapped_encoded_buffer() : code_size(0){
				
			}

			size_t capacity() const {
				size_t is = sizeof(_IntType);
				size_t bs = sizeof(_CodeType);
				return (is+bs)*codes.size()+is*decodes.capacity()+symbols.capacity();
			}

			_Entropy& get_stats(){
				return stats;
			}
			const _Entropy& get_stats() const{
				return stats;
			}
			void clear(){
                _CodeMap c;
                _DeCodeMap d;
				code_size = 0;
				stats.clear();
				codes.swap(c);
				decodes.swap(d);
				symbols.clear();
			}

			bool empty() const {
				return code_size == 0;
			}

			void resize(_Rid rows){
				if(code_size==0) throw UninitializedCodeException();
				symbols.resize(rows);
			}

			void encode(_Rid row, const _IntType &val){
				if(codes.empty()){
					_CodeType code = (_CodeType)val.get_value() -(_CodeType)min_decoded;
					symbols.set(row, code);
					return;
				}
				if(codes.count(val)){
					_CodeType code = codes[val];
					symbols.set(row, code);

				}else
					throw UninitializedCodeException();
			}

			const _IntType& decode(_Rid row) const {
				_CodeType code = 0;
				if(code_size > 0){
					code = symbols.get(row);
					if(decodes.empty()){
						unmapped.set_value(code + min_decoded);
						return unmapped;
					}
					
					return decodes[code];
					
				}else
					throw UninitializedCodeException();
				return _null_val;
			}
			_IntType& decode(_Rid row) {
				_CodeType code = 0;
				if(code_size > 0){
					code = symbols.get(row);
					if(decodes.empty()){
						unmapped.set_value(code + min_decoded);
						return unmapped;
					}
					
					return decodes[code];
					
				}else
					throw UninitializedCodeException();
				return _null_val;
			}
			bool initialize(_Rid rows){

				if( ( (double)stats.get_samples() / (double)stats.get_entropy() ) > 1 && stats.get_entropy() < MAX_ENTROPY){
					size_t instances = (*this).stats.get_unique_samples().size();
					/// bits per symbol in mapped encoding
					nst::u32 bits_in_histogram = bits::bit_log2((nst::u32)(*this).stats.get_entropy()) * rows + 8 * sizeof(_IntType) * stats.get_entropy();
					/// bits per symbol in abs encoding
					nst::u32 bits_in_abs = bits::bit_log2((nst::u32)(*this).stats.get_abs()) * rows;
					/// not sure about negative values yet, although translation i.o. mapping should sort them out
					if(bits_in_abs < bits_in_histogram){						
						decodes.swap(_DeCodeMap());
						codes.swap(_CodeMap());
						min_decoded = stats.get_min_val() ;
						code_size = std::max<_CodeType>(1, bits::bit_log2((_CodeType)stats.get_abs()));	
						symbols.set_code_size(code_size);
						resize(rows);
						
					}else{
						
						
						nst::u32 words = 0;
						{
							decodes.resize((*this).stats.get_unique_samples().size());
							typename _Entropy::_UniqueSamples::iterator h = (*this).stats.get_unique_samples().begin();
							for(;h != (*this).stats.get_unique_samples().end();++h){

								codes[(*h)] = words;
								decodes[words] = (*h);
								++words;

							}
						}
						(*this).stats.clear();

						if(words > 0){

							code_size = std::max<_CodeType>(1, bits::bit_log2((_CodeType)words));
							symbols.set_code_size(code_size);

							resize(rows);
						}
					}


				}
				return code_size > 0;

			}
			void optimize(){
                _CodeMap c;
				(*this).codes.swap(c);
				(*this).stats.clear();
			}
		};	/// fixed in encoded buffer

		template<class _BufType>
		class lz_buffer{
		private:
			_BufType _decoded_val;
		public:

			lz_buffer(){
			}

			bool empty() const {
				return true;
			}

			void sample(const _BufType &data){
			}

			void resize(_Rid rows){
			}

			size_t capacity() const {
				return 0;
			}

			bool initialize(_Rid rows){
				return true;
			}

			void encode(_Rid row, const _BufType &val){

			}

			const _BufType& decode(_Rid row) const {

				return _decoded_val;
			}
			_BufType& decode(_Rid row) {

				return _decoded_val;
			}
			void optimize(){
			}
		};

		/// entropy coding decision class
		template<typename _D>
		class standard_entropy_coder{
		private:
			nst::u64 bsamples;
			_D empty_Val;
		public:
			standard_entropy_coder():bsamples(0){
			}
			bool empty() const {
				return true;
			}
			void clear(){
			}
			size_t total_bytes_allocated() const{
				return sizeof(*this);
			}
			void sample(const _D &data){
				++bsamples;
			}
			void finish(_Rid rows){
			}
			bool applicable() const {
				return false;
			}
			bool good() const {
				return false;
			}
			void set(_Rid rid, const _D& data){

			}
			void optimize(){
			}
			const _D& get(_Rid _Rid) const {
				return empty_Val;
			}
			_D& get(_Rid _Rid) {
				return empty_Val;
			}
			nst::u64 get_entropy()  {
				return 0;
			}
			size_t capacity() const {
				return 0;
			}
		};
#define _ENTROPY_CODING_
#ifdef _ENTROPY_CODING_
		
		template<>
		struct standard_entropy_coder<LongIntStored >{
			typedef LongIntStored _DataType;
			int_fix_unmapped_encoded_buffer<_DataType> coder;
			///int_fix_encoded_buffer<_DataType> coder;
			bool empty() const {
				return coder.empty();
			}
			
			bool applicable() const {
				return true;
			}
			
			void sample(const _DataType &data){
				coder.get_stats().sample(data);
			}
			
			size_t total_bytes_allocated() const {
				return coder.get_stats().total_bytes_allocated();
			}

			void finish(_Rid rows){
				coder.initialize(rows);
			}
			void clear(){
				coder.clear();
			}
			bool good() const {
				return !coder.empty();
			}
			void optimize(){
				coder.optimize();
			}
			void set(_Rid row, const _DataType& data){
				coder.encode(row, data);
			}
			const _DataType& get( _Rid row) const {
				return coder.decode(row);
			}
			_DataType& get( _Rid row) {
				return coder.decode(row);
			}
			nst::u64 get_entropy() {
				return 0; //coder.get_stats().get_entropy();
			}
			size_t capacity() const {
				return coder.capacity();
			}
		};
		
		template<>
		struct standard_entropy_coder<ULongIntStored >{
			typedef ULongIntStored _DataType;
			int_fix_unmapped_encoded_buffer<_DataType> coder;
			///int_fix_encoded_buffer<_DataType> coder;
			bool empty() const {
				return coder.empty();
			}
			
			bool applicable() const {
				return true;
			}
			
			void sample(const _DataType &data){
				coder.get_stats().sample(data);
			}
			
			size_t total_bytes_allocated() const {
				return coder.get_stats().total_bytes_allocated();
			}

			void finish(_Rid rows){
				coder.initialize(rows);
			}
			void clear(){
				coder.clear();
			}
			bool good() const {
				return !coder.empty();
			}
			void optimize(){
				coder.optimize();
			}
			void set(_Rid row, const _DataType& data){
				coder.encode(row, data);
			}
			const _DataType& get( _Rid row) const {
				return coder.decode(row);
			}
			_DataType& get( _Rid row) {
				return coder.decode(row);
			}
			nst::u64 get_entropy() {
				return 0; //coder.get_stats().get_entropy();
			}
			size_t capacity() const {
				return coder.capacity();
			}
		};
		template<>
		struct standard_entropy_coder<IntStored >{
			typedef IntStored _DataType;
			int_fix_unmapped_encoded_buffer<_DataType> coder;
			///int_fix_encoded_buffer<_DataType> coder;
			bool empty() const {
				return coder.empty();
			}
			
			bool applicable() const {
				return true;
			}
			
			void sample(const IntStored &data){
				coder.get_stats().sample(data);
			}
			
			size_t total_bytes_allocated() const {
				return coder.get_stats().total_bytes_allocated();
			}

			void finish(_Rid rows){
				coder.initialize(rows);
			}
			void clear(){
				coder.clear();
			}
			bool good() const {
				return !coder.empty();
			}
			void optimize(){
				coder.optimize();
			}
			void set(_Rid row, const IntStored& data){
				coder.encode(row, data);
			}
			const IntStored& get( _Rid row) const {
				return coder.decode(row);
			}
			IntStored& get( _Rid row) {
				return coder.decode(row);
			}
			nst::u64 get_entropy() {
				return 0; //coder.get_stats().get_entropy();
			}
			size_t capacity() const {
				return coder.capacity();
			}
		};

		template<>
		struct standard_entropy_coder<UIntStored >{
			typedef UIntStored _DataType;
			int_fix_unmapped_encoded_buffer<_DataType> coder;
			/// int_fix_encoded_buffer<_DataType> coder;
			bool empty() const {
				return coder.empty();
			}
			bool applicable() const {
				return true;
			}
			void sample(const UIntStored &data){
				coder.get_stats().sample(data);
			}
			size_t total_bytes_allocated() const {
				return coder.get_stats().total_bytes_allocated();
			}
			void finish(_Rid rows){
				coder.initialize(rows);
			}
			void clear(){
				coder.clear();
			}
			bool good() const {
				return !coder.empty();
			}
			void optimize(){
				coder.optimize();
			}
			void set(_Rid row, const UIntStored& data){
				coder.encode(row, data);
			}
			const UIntStored&  get(_Rid row) const {
				return coder.decode(row);
			}
			UIntStored&  get(_Rid row) {
				return coder.decode(row);
			}
			nst::u64 get_entropy() {
				return 0; //coder.get_stats().get_entropy();
			}
			size_t capacity() const {
				return coder.capacity();
			}
		};

		template<>
		struct standard_entropy_coder<ShortStored >{
			int_fix_unmapped_encoded_buffer<ShortStored> coder;
			/// int_fix_encoded_buffer<ShortStored> coder;
			bool empty() const {
				return coder.empty();
			}
			bool applicable() const {
				return true;
			}
			void sample(const ShortStored &data){
				coder.get_stats().sample(data);
			}
			size_t total_bytes_allocated() const {
				return coder.get_stats().total_bytes_allocated();
			}
			void finish(_Rid rows){
				coder.initialize(rows);
			}
			void clear(){
				coder.clear();
			}
			bool good() const {
				return !coder.empty();
			}
			void optimize(){
				coder.optimize();
			}
			void set(_Rid row, const ShortStored& data){
				coder.encode(row, data);
			}
			const ShortStored& get(_Rid row) const {
				return coder.decode(row);
			}
			ShortStored& get(_Rid row) {
				return coder.decode(row);
			}
			nst::u64 get_entropy() {
				return coder.get_stats().get_entropy();
			}
			size_t capacity() const {
				return coder.capacity();
			}
		};
		template<>
		struct standard_entropy_coder<UShortStored >{
			int_fix_unmapped_encoded_buffer<UShortStored> coder;
			/// int_fix_encoded_buffer<UShortStored> coder;
			bool empty() const {
				return coder.empty();
			}
			bool applicable() const {
				return true;
			}
			void sample(const UShortStored &data){
				coder.get_stats().sample(data);
			}
			size_t total_bytes_allocated() const{
				return coder.get_stats().total_bytes_allocated();
			}
			void finish(_Rid rows){
				coder.initialize(rows);
			}
			void clear(){
				coder.clear();
			}
			bool good() const {
				return !coder.empty();
			}
			void optimize(){
				coder.optimize();
			}
			void set(_Rid row, const UShortStored& data){
				coder.encode(row, data);
			}
			const UShortStored& get(_Rid row) const {
				return coder.decode(row);
			}
			UShortStored& get(_Rid row) {
				return coder.decode(row);
			}
			nst::u64 get_entropy() {
				return 0; //coder.get_stats().get_entropy();
			}
			size_t capacity() const {
				return coder.capacity();
			}
		};

		template<>
		struct standard_entropy_coder<VarCharStored >{
			int_fix_encoded_buffer<VarCharStored> coder;
			bool empty() const {
				return coder.empty();
			}
			bool applicable() const {
				return true;
			}
			size_t total_bytes_allocated() const {
				return coder.get_stats().total_bytes_allocated();
			}
			void sample(const VarCharStored &data){
				coder.get_stats().sample(data);
			}
			void finish(_Rid rows){
				coder.initialize(rows);
			}
			void clear(){
				coder.clear();
			}
			bool good() const {
				return !coder.empty();
			}
			void optimize(){
				coder.optimize();
			}
			void set(_Rid row, const VarCharStored& data){
				coder.encode(row, data);
			}
			VarCharStored& get(_Rid row) {
				return coder.decode(row);
			}
			nst::u64 get_entropy()  {
				return 0; //coder.get_stats().get_entropy();
			}
			size_t capacity() const {
				return coder.capacity();
			}
		};
		template<>
		struct standard_entropy_coder<BlobStored >{
			int_fix_encoded_buffer<BlobStored> coder;
			bool empty() const {
				return coder.empty();
			}
			bool applicable() const {
				return true;
			}
			void sample(const BlobStored &data){
				coder.get_stats().sample(data);
			}
			size_t total_bytes_allocated() const {
				return coder.get_stats().total_bytes_allocated();
			}
			void finish(_Rid rows){
				coder.initialize(rows);
			}
			void clear(){
				coder.clear();
			}
			bool good() const {
				return !coder.empty();
			}
			void optimize(){
				coder.optimize();
			}
			void set(_Rid row, const BlobStored& data){
				coder.encode(row, data);
			}
			const BlobStored& get(_Rid row) const {
				return coder.decode(row);
			}
			BlobStored& get(_Rid row) {
				return coder.decode(row);
			}
			nst::u64 get_entropy() {
				return coder.get_stats().get_entropy();
			}
			size_t capacity() const {
				return coder.capacity();
			}
		};
		/**/
#endif
	class index_iterator_interface{
	public:
		///for quick type check
		int type_id;

		index_iterator_interface() : type_id(0){
		}
		virtual ~index_iterator_interface(){
		}

		virtual bool valid() const = 0;
		virtual bool invalid() const = 0;
		virtual void next() = 0;
		virtual void previous() = 0;
		virtual void first() = 0;
		virtual void last() = 0;
		virtual nst::u64 count(const index_iterator_interface& in) = 0;
		virtual DynamicKey& get_key() = 0;
		virtual void set_end(index_iterator_interface& in) = 0;

	};

	class index_interface{
	private:
		bool destroyed;
	public:
		int ix;
		int fields_indexed;
		stored::_Parts parts;
		stored::_Parts density;
		
		std::string name;
		index_interface() : destroyed(false){
		}
		virtual ~index_interface(){
			if(destroyed)
				printf("destroyed index already\n");
			destroyed = true;
		};
		virtual const DynamicKey *predict(index_iterator_interface& io, DynamicKey& q)=0;
		virtual void cache_it(index_iterator_interface& io)=0;
		virtual bool is_unique() const = 0;
		virtual void set_col_index(int ix)= 0;
		virtual void set_fields_indexed(int indexed)=0;
		virtual void push_part(_Rid part)=0;
		virtual void push_density(_Rid dens) = 0;
		virtual size_t densities() const = 0;
		virtual stored::_Rid& density_at(size_t at) = 0;
		virtual const stored::_Rid& density_at(size_t at) const = 0;
		virtual void end(index_iterator_interface& out)=0;
		virtual index_iterator_interface * get_index_iterator() = 0;
		virtual index_iterator_interface * get_prepared_index_iterator() = 0;
		virtual index_iterator_interface * get_first1() = 0;
		virtual index_iterator_interface * get_last1() = 0;

		virtual void first(index_iterator_interface& out)=0;
		virtual void lower_(index_iterator_interface& out,const DynamicKey& key)=0;
		virtual void lower(index_iterator_interface& out,const DynamicKey& key)=0;
		virtual void upper(index_iterator_interface& out, const DynamicKey& key)=0;
		virtual void add(const DynamicKey& k)=0;
		virtual void remove(const DynamicKey& k) = 0;
		virtual void find(index_iterator_interface& out, const DynamicKey& key) = 0;
		virtual void from_initializer(index_iterator_interface& out, const stx::initializer_pair& ip) = 0;
		virtual void reduce_use() = 0;

		virtual void begin(bool read,bool shared=true) = 0;

		virtual void commit1_asynch() = 0;

		virtual void commit1()= 0;

		virtual void commit2() = 0;

		virtual void rollback() = 0;

		virtual void share() = 0;
		virtual void unshare() = 0;

		virtual void clear_cache() = 0;
		virtual void reduce_cache() = 0;

		typedef index_interface * ptr;
	};

};
#endif
