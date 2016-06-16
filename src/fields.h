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
#include <stx/storage/pool.h>
extern stx::storage::allocation::pool allocation_pool;
#ifdef _MSC_VER
#define CONVERSION_NOINLINE_ /// _declspec(noinline)
#else
#define CONVERSION_NOINLINE_
#endif

extern  char treestore_reduce_tree_use_on_unlock;

namespace NS_STORAGE = stx::storage;
namespace nst = stx::storage;
typedef NS_STORAGE::synchronized synchronized;
typedef NS_STORAGE::synchronizing synchronizing;
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
		/// col bytes functions
		size_t col_bytes() const{
			return sizeof(value) ;
		}

		nst::u8 col_byte(size_t which) const {			
			return ((const nst::u8*)(&value))[which];			
		}
		
		void col_resize_bytes(size_t to){
			value = 0;			
		}

		void col_push_byte(size_t which, nst::u8 v) {			
			((nst::u8*)(&value))[which] = v;							
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
		/// col bytes functions
		size_t col_bytes() const{
			return sizeof(value) ;
		}

		nst::u8 col_byte(size_t which) const {			
			return ((const nst::u8*)(&value))[which];			
		}
		
		void col_resize_bytes(size_t to){
			value = 0;			
		}

		void col_push_byte(size_t which, nst::u8 v) {			
			((nst::u8*)(&value))[which] = v;							
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
	class StaticBlobule {
	protected:
		static const int _ConstSize = _MConstSize;
	public:
		typedef empty_encoder list_encoder;
		static const int CHAR_TYPE = CHAR_LIKE ? 1 : 0;
		//typedef bool attached_values;
	protected:

		typedef NS_STORAGE::u16 _BufferSize;
		static const size_t max_buffersize = _ConstSize;
		
		_BufferSize size;// bytes used
		NS_STORAGE::u8 buf[_ConstSize];
		//const NS_STORAGE::u8* attached;
		void set_size(size_t ns){
			this->size = std::min<_BufferSize>(ns, _ConstSize);
		}

		inline bool is_static() const {			
			return true;
		}

		NS_STORAGE::u8* data(){
			return buf;			
		}

		const NS_STORAGE::u8* data() const{			
			return buf;			
		}

		void null_check() const {
			if(!this){
				printf("[ERROR] [TS] Invalid Blob object\n");
			}
			if(size > _ConstSize){
				printf("[ERROR] [TS] Invalid Static Blob object size\n");
			}

		}
		void _add( const void * v, size_t count, bool end_term = false){
			null_check();

			_BufferSize extra = end_term ? 1 : 0;
			if((size + count + extra) > _ConstSize){
				printf("[WARNING] [TS] Attempt to set Invalid Static Blob object size\n");
			}else{

				memcpy(data()+size,v,count);
				if(end_term) data()[size+count] = 0;
				set_size(size+count+extra);
			}
			
		}
		bool less(const StaticBlobule& right) const {
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
		bool greater(const StaticBlobule& right) const {
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
		bool not_equal(const StaticBlobule& right) const {
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
		void free_ptr(){
		
			size = 0;			

		}

	public:
		size_t total_bytes_allocated() const {
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
			_add(nt,n);
		}
		void addterm(const char * nt,size_t n){
			_add(nt,n,true);
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

		StaticBlobule ()
		:	size(0)
		//,	attached(nullptr)
		{
			//memset(buf,0,sizeof(buf));
			buf[0] = 0;
		}
		void clear(){
			free_ptr();
			//attached = nullptr;			
			size = 0;
			buf[0] = 0;
		}

		inline ~StaticBlobule(){
			free_ptr();
			size = 0;
		}

		StaticBlobule (const StaticBlobule& init)
		:	size(0)
		//,	attached(nullptr)
		{
			*this = init;
		}
		StaticBlobule(const char *right)
		:	size(0)
		//,	attached(nullptr)
		{
			*this = right;
		}

		StaticBlobule(const NS_STORAGE::i64& right)
		:	size(0)
		//,	attached(nullptr)
		{
			set(right);

		}
		StaticBlobule(const double& right)
		:	size(0)
		//,	attached(nullptr)
		{
			set(right);

		}
		
		/// col bytes functions
		size_t col_bytes() const{
			return get_size();
		}
		
		nst::u8 col_byte(size_t which) const {			
			return data()[which];			
		}
		
		void col_resize_bytes(size_t to){
			set_size(to);			
		}
		
		void col_push_byte(size_t which, nst::u8 v) {			
			data()[which] = v;			
		}

		inline StaticBlobule& operator=(const StaticBlobule& right)
		{
			using namespace NS_STORAGE;
			
			set_size(right.size);
			memcpy(data(),right.data(),this->size);
			
			return *this;
		}

		StaticBlobule& operator=(const char *right) {
			set(right);
			return *this;
		}
		StaticBlobule& operator=(const NS_STORAGE::i64& right) {
			set(right);
			return *this;
		}
		StaticBlobule& operator=(const double& right) {
			set(right);
			return *this;
		}
		inline bool operator>(const StaticBlobule& right) const {
			return greater(right);
		}
		inline bool operator<(const StaticBlobule& right) const {
			return less(right);
		}

		inline bool operator!=(const StaticBlobule& right) const {

			return not_equal(right);
		}
		inline bool operator==(const StaticBlobule& right) const {

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
			memcpy(((u8 *)&(*writer)),data(),size);
			writer += size;
			//const u8 * end = data()+size;
			//for(const u8 * d = data(); d < end; ++d,++writer){
			//	*writer = *d;
			//}
			return writer;
		};

		NS_STORAGE::buffer_type::const_iterator read(const NS_STORAGE::buffer_type& buffer, NS_STORAGE::buffer_type::const_iterator r) {
			using namespace NS_STORAGE;
			buffer_type::const_iterator reader = r;

			if(reader != buffer.end()){
				
				size = leb128::read_signed(reader);
				if(size > _ConstSize){
					printf("[ERROR] [TS] reading invalid buffer size for temp\n");
				}
				u8 * d = data();
				u8 * end = data() + size;
				u8 * dend = data() + _ConstSize;
				for(u8 * d = data(); d < end; ++d,++reader){
					if(d < dend)
						*d = *reader;
				}				
			}
			return reader;
		};
	};

	template<bool CHAR_LIKE, int _MConstSize = 16>
	class Blobule {
	protected:
		static const int _ConstSize = _MConstSize;
	public:
		typedef empty_encoder list_encoder;
		static const int CHAR_TYPE = CHAR_LIKE ? 1 : 0;
		//typedef bool attached_values;
	protected:

		typedef NS_STORAGE::u16 _BufferSize;
		static const size_t max_buffersize = 1 << (sizeof(_BufferSize) << 3);
		_BufferSize bytes;// bytes within dyn or static buffer
		_BufferSize size;// bytes used
		NS_STORAGE::u8 buf[_ConstSize];
		
		//const NS_STORAGE::u8* attached;
		inline NS_STORAGE::u8 *extract_ptr(){
			return (NS_STORAGE::u8*)(*(size_t*)(buf));
		}
		inline const NS_STORAGE::u8 *extract_ptr() const {
			return (const NS_STORAGE::u8*)(*(const size_t*)(buf));
		}
		inline bool is_static() const {
			//if(attached!=nullptr) return false;
			return (bytes <= _ConstSize); /// <= ???
		}
		NS_STORAGE::u8* data(){
			null_check();

			if(is_static()){
				return buf;
			}
			return extract_ptr();
		}
		const NS_STORAGE::u8* data() const{
			null_check();
			if(is_static()){
				return buf;
			}
			return extract_ptr();
		}
		void set_ptr(const NS_STORAGE::u8* nbuf){
			NS_STORAGE::u8* d = const_cast<NS_STORAGE::u8*>(nbuf);
			memcpy(buf, &d, sizeof(NS_STORAGE::u8*));
		}
		void set_ptr(const NS_STORAGE::i8* nbuf){
			NS_STORAGE::i8* d = const_cast<NS_STORAGE::i8*>(nbuf);
			memcpy(buf, &d, sizeof(NS_STORAGE::i8*));
		}
		void null_check() const {
		}
		void _null_check() const {
			if(!this){
				err_print("Invalid Blob object");
			}
			if(bytes == 0){
				err_print("Invalid Blob object size");
			}

		}
		NS_STORAGE::u8* dyn_resize_buffer(NS_STORAGE::u8* r,size_t mnbytes){
			null_check();
			using namespace NS_STORAGE;
			//if(mnbytes > max_buffersize){
			//	err_print("large buffer");
			//}
			size_t nbytes = std::min<size_t>(mnbytes, max_buffersize);
			NS_STORAGE::u8 * nbuf = (NS_STORAGE::u8*)allocation_pool.allocate(nbytes); ///new NS_STORAGE::u8 [nbytes]; //
			//add_col_use(nbytes);
			memcpy(nbuf, r, std::min<size_t>(nbytes, bytes));
			if(!is_static()){
				allocation_pool.free(r,bytes);
				//delete r;
				//remove_col_use(bytes);				
			}
			set_ptr(nbuf);
			bytes = (u32)nbytes;
			size = std::min<_BufferSize>(size, bytes);
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
		void free_ptr(){
			if(bytes > _ConstSize){
				using namespace NS_STORAGE;
				allocation_pool.free(data(), bytes)	;
				//delete data();
				bytes = _ConstSize;
				size = 0;
				//remove_col_use(bytes);
			}

		}

	public:
		size_t total_bytes_allocated() const {
			if(!is_static())
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
			_add(nt,n);
		}
		void addterm(const char * nt,size_t n){
			_add(nt,n,true);
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
		//,	attached(nullptr)
		{
			//memset(buf,0,sizeof(buf));
			buf[0] = 0;
		}
		void clear(){
			free_ptr();
			//attached = nullptr;
			bytes = _ConstSize;
			size = 0;
			buf[0] = 0;
		}

		inline ~Blobule(){
			free_ptr();
			bytes = 0;
		}

		Blobule (const Blobule& init)
		:	bytes(_ConstSize)
		,	size(0)
		//,	attached(nullptr)
		{
			*this = init;
		}
		Blobule(const char *right)
		:	bytes(_ConstSize)
		,	size(0)
		//,	attached(nullptr)
		{
			*this = right;
		}

		Blobule(const NS_STORAGE::i64& right)
		:	bytes(_ConstSize)
		,	size(0)
		//,	attached(nullptr)
		{
			set(right);

		}
		Blobule(const double& right)
		:	bytes(_ConstSize)
		,	size(0)
		//,	attached(nullptr)
		{
			set(right);

		}
		
		/// col bytes functions
		size_t col_bytes() const{
			return get_size();
		}
		
		nst::u8 col_byte(size_t which) const {			
			return data()[which];			
		}
		
		void col_resize_bytes(size_t to){
			_resize_buffer(to);			
			this->size = to;
		}
		
		void col_push_byte(size_t which, nst::u8 v) {			
			data()[which] = v;			
		}

		inline Blobule& operator=(const Blobule& right)
		{
			using namespace NS_STORAGE;
			if(!right.is_static()){
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
			memcpy(((u8 *)&(*writer)),data(),size);
			writer += size;
			//const u8 * end = data()+size;
			//for(const u8 * d = data(); d < end; ++d,++writer){
			//	*writer = *d;
			//}
			return writer;
		};

		NS_STORAGE::buffer_type::const_iterator read(const NS_STORAGE::buffer_type& buffer, NS_STORAGE::buffer_type::const_iterator r) {
			using namespace NS_STORAGE;
			buffer_type::const_iterator reader = r;

			if(reader != buffer.end()){
				bool fast = true;

				//if(fast){

					size = *reader;

					//if(size <= 8){
					//	++reader;
					//	const nst::u8 * d = &(*reader);
					//	*((nst::u64*)data()) = *((const nst::u64*)d);
					//}else{

				size = leb128::read_signed(reader);
				_resize_buffer(size);
				u8 * end = data()+size;
				//for(u8 * d = data(); d < end; ++d,++reader){
				//	*d = *reader;
				//}
				memcpy(data(),&(*reader),size);
					//}
				reader += size;
				
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
	static const _Rid MAX_ROWS = std::numeric_limits<_Rid>::max();

	typedef std::vector<_Rid > _Parts; //,sta::col_tracker<_Rid>
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
	typedef stored::Blobule<false, 3*sizeof(NS_STORAGE::u8*)> BlobStored;
	typedef stored::Blobule<true,3*sizeof(NS_STORAGE::u8*)> VarCharStored;
	
	class BareKey{
	public:
		typedef nst::u8* _Data;
		typedef _Rid row_type ;
		typedef NS_STORAGE::u32 _BufferSize;
	protected:
		_Data d;
		_BufferSize bs;
		
	public:
		row_type row;
		BareKey() : bs(0),row(0),d(nullptr){
		}
		BareKey(_Data d, _BufferSize s, row_type r) : bs(s),row(r),d(d){
		}
		BareKey(const BareKey& other){
			*this = other;			
		}
		~BareKey(){
		}
		
		void resize(_BufferSize s,_Data data){
			this->bs = s;
			this->d = data;
		}

		_BufferSize size() const {
			return bs;
		}
		
		const _Data data() const{
			return d;
		}
		
		BareKey& operator=(const BareKey& other){
			bs = other.bs;
			d = other.d;
			row = other.row;
		}

	};
	template<int _StaticSize>
	class DynamicKey{
	public:

		enum StoredType{
			I1 = 1,
			I2 ,
			I4 ,
			I8 ,
			UI1 ,
			UI2 ,
			UI4 ,
			UI8 ,
			R4 ,
			R8 ,
			S,
			KEY_MAX

		};

		typedef nst::u8* _Data;

		static const size_t MAX_BYTES = 2048;

		typedef _Rid row_type ;

		typedef NS_STORAGE::u16 _BufferSize;

	protected:

		static const size_t static_size = sizeof(_Data)+_StaticSize;
		static const size_t max_buffer_size = 1 << (sizeof(_BufferSize) << 3);

		nst::u8 buf[static_size];
		_BufferSize bs;// bytes used
		_BufferSize cap;
		inline bool is_static() const {
			return cap <= static_size;
		}
		inline bool is_dynamic() const {
			return !(cap <= static_size);
		}
		inline _Data extract_ptr(){
			return (_Data)(*(size_t*)(buf));
		}
		inline const _Data extract_ptr() const {
			return (const _Data)(*(const size_t*)(buf));
		}
		
		void set_ptr(const _Data nbuf){
			_Data d = const_cast<_Data>(nbuf);
			if(sizeof(buf) < sizeof(_Data)){
				err_print("not enough space");
			}
			memcpy(buf, &d, sizeof(_Data));
		}
		
		void set_ptr(const NS_STORAGE::i8* nbuf){
			_Data d = const_cast<NS_STORAGE::i8*>(nbuf);
			memcpy(buf, &d, sizeof(NS_STORAGE::i8*));
		}
		_Data dyn_resize_buffer(NS_STORAGE::u8* r,size_t mnbytes){
			NS_STORAGE::u8 * nbuf = r;
			using namespace NS_STORAGE;
			const size_t ex = 64ll;
			if(mnbytes > cap){
				size_t nbytes = mnbytes; // ((mnbytes/ex)*ex)+ex;		
				//if(nbytes < mnbytes){
				//	err_print("Invalid resize size");
				//}
				nbuf = (NS_STORAGE::u8*)allocation_pool.allocate(nbytes); ///new NS_STORAGE::u8 [nbytes]; //
				memcpy(nbuf, r, std::min<size_t>(nbytes, cap));
				if(is_dynamic()){
					allocation_pool.free(r,cap);
				}
				set_ptr(nbuf);
				cap = (u32)nbytes;
			}					
			return nbuf;
		}
		_Data _resize_buffer(size_t mnbytes){
			using namespace NS_STORAGE;
			size_t nbytes = std::min<size_t>(mnbytes, max_buffer_size);
			_Data r = data();
			if(nbytes > static_size){
				r = dyn_resize_buffer(r,nbytes);
			}
			bs = (u32)nbytes;	
			
			return r;
		}
		
		_Data _append( size_t count ){
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
			*_append(1) = DynamicKey::UI8;
			*(nst::u64*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::i32 v){
			*_append(1) = DynamicKey::I4;
			*(nst::i32*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::u32 v){
			*_append(1) = DynamicKey::UI4;
			*(nst::u32*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::i16 v){
			*_append(1) = DynamicKey::I2;
			*(nst::i16*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::u16 v){
			*_append(1) = DynamicKey::UI2;
			*(nst::u16*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::i8 v){
			*_append(1) = DynamicKey::I1;
			*(nst::i8*)_append(sizeof(v)) =  v;
		}
		void addDynInt(nst::u8 v){
			*_append(1) = DynamicKey::UI1;
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
		void addDynInf(){
			*_append(1) = DynamicKey::KEY_MAX;
			*(nst::u8*)_append(sizeof(nst::u8)) = 0xff;
		}
	public:
		void add(const char* k, size_t s){
			size_t sad = s;
			*_append(1) = DynamicKey::S;			
			( *(nst::u16*)_append(sizeof(nst::u16))) = (nst::u16)sad;
			if(sad > 0){
				memcpy(_append(sad), k, sad);
			}
		}
	public:
		row_type row;
	public:
		BareKey get_bare() const {
			return BareKey((BareKey::_Data)data(),(BareKey::_BufferSize)bs,row);
		}
		template<int _StaticSize>
		BareKey get_bare(DynamicKey<_StaticSize>& ) const {
			return get_bare();
		}
		inline nst::u8* data(){
			if(is_static())
				return &buf[0];
			
			_Data r = extract_ptr();		
			return r;
		}
		inline const nst::u8* data() const{
			if(is_static())
				return &buf[0];
			const _Data r = extract_ptr();			
			
			return r;
			
		}
		void verify() const {
			iterator k = this->begin();
			while(k.valid()){
				k.next();
			}
		}
		size_t total_bytes_allocated() const {
			if(is_static())				
				return sizeof(*tkhis);
			return sizeof(*this) + cap;
		}
		int size() const {
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
		void addInf(){
			addDynInf();
		}
		void add(const stored::VarCharStored& v){
			if(v.get_size()){
				add(v.get_value(),v.get_size()-1); /// exclude the terminator
			}
		}
		template<int L>
		void add(const stored::StaticBlobule<true, L>& v){
			if(v.get_size()){
				add(v.get_value(),v.get_size()-1); /// exclude the terminator
			}
		}

		void addTerm(const char* k, size_t s){
			add(k, s);
		}

		void clear(){

			row = 0;
			bs = 0;
		}

		~DynamicKey(){
			if(is_dynamic()){
				allocation_pool.free(extract_ptr(),cap);
			}
			//bs = 0;
			//row = 0;
		}

		DynamicKey():
			bs(0),
			row(0),
			cap(static_size)
		{
			//memset(buf, 0,sizeof(buf));
		}
		template<int _InStaticSize>		
		DynamicKey(const DynamicKey<_InStaticSize>& right):
			bs(0),
			row(0),
			cap(static_size)
		{			
			*this = right;
		}
		
		DynamicKey(const DynamicKey& right):
			bs(0),
			row(0),
			cap(static_size)
		{			
			*this = right;
		}

		/// returns true if this is a prefix of the other key
		template<int _InStaticSize>	
		inline bool is_prefix(const DynamicKey<_InStaticSize>& other) const {
			if( other.size() < size() ) return false; /// this cannot be a prefix of something smaller

			int r = memcmp(data(), other.data(), size() );
			return r == 0;
		}

		struct iterator{
			const nst::u8 *ld;
			nst::u32 s;			
			nst::u32 ll;
			iterator(const BareKey& other) : s(other.size()),ld(other.data()),ll(0){
			}
			iterator(const DynamicKey& other) : s(other.size()),ld(other.data()),ll(0){
			}
			iterator(const nst::u8 *ld,nst::u32 s) : s(s),ld(ld),ll(0){				
			}
			iterator(const iterator&  right) {
				(*this) = right;
			}
			iterator& operator=(const iterator&  right) {
				ld = right.ld;
				s = right.s;
				ll = right.ll;
			}
			inline nst::u32 size() const {
				return s;
			}

			inline nst::u8 get_type() const {
				if(!valid()) return 0;
				return *ld;
			}
			
			inline nst::u32 get_length() const {
				if(!valid()) return 0;
				nst::u32 l = 0;
				int t = get_type();
				switch(t){
				case DynamicKey::I1 :
				case DynamicKey::UI1 :
					l=sizeof(nst::i8);
					break;
				case DynamicKey::I2:
				case DynamicKey::UI2:
					l = sizeof(nst::i16);
					break;
				case DynamicKey::I4:
				case DynamicKey::UI4:
					l = sizeof(nst::i32);
					break;
				case DynamicKey::I8 :
				case DynamicKey::UI8 :
					l = sizeof(nst::i64);
					break;
				case DynamicKey::R4 :
					l = sizeof(float);
					break;
				case DynamicKey::R8 :
					l = sizeof(double);
					break;
				case DynamicKey::S:{					
					nst::i16 
					ls = sizeof(nst::i16),					
					ll = *(const nst::i16*)ld;				
					l = ll+ls;
								   }
					break;
				case DynamicKey::KEY_MAX:
					l = sizeof(nst::u8);
					break;
				default:
					err_print("unknown type in index key");
					break;
				};	
				return l;
			}
			
			inline nst::u32 get_data_length() const {
				if(!valid()) return 0;
				nst::u32 l = 0;
				int t = get_type();
				switch(t){
				case DynamicKey::UI1 :
				case DynamicKey::I1 :
					l=sizeof(nst::i8);
					break;
				case DynamicKey::UI2:
				case DynamicKey::I2:
					l = sizeof(nst::i16);
					break;
				case DynamicKey::UI4:
				case DynamicKey::I4:
					l = sizeof(nst::i32);
					break;
				case DynamicKey::UI8 :
				case DynamicKey::I8 :
					l = sizeof(nst::i64);
					break;
				case DynamicKey::R4 :
					l = sizeof(float);
					break;
				case DynamicKey::R8 :
					l = sizeof(double);
					break;
				case DynamicKey::S:										
					l = *(const nst::i16*)(ld+sizeof(nst::u8));								
					break;
				case DynamicKey::KEY_MAX:
					l = sizeof(nst::u8);
					break;
				default:
					err_print("unknown type in index key");
					break;
				};	
				return l;
			}
			
			inline bool valid() const {
				if(ll >= this->size()) return false;				
				return true;
			}
			
			inline const nst::u8 * get_data() const {
				if(get_type() == DynamicKey::S){								
					return &ld[sizeof(nst::u8) + sizeof(nst::i16)];
				}
				return &ld[sizeof(nst::u8)];					
			}

			inline void next(){
				nst::u32 l = get_length()+sizeof(nst::u8);
				ld += l; // increments buffer
				ll += l; // increments length																	
			}
		};
		iterator begin() const {
			return iterator(data(),size());
		}
		nst::u32 count_parts() const {
			const nst::u8 *ld = data();
			
			nst::u8 lt = *ld;
			
			nst::u32 r = 0,l=0,ll=0;
			while(true){
				++ld;			
				++ll;
				switch(lt){
				case DynamicKey::I1 :
					l=sizeof(nst::i8);
					break;
				case DynamicKey::UI2:
				case DynamicKey::I2:
					l = sizeof(nst::i16);
					break;
				case DynamicKey::I4:
				case DynamicKey::UI4:
					l = sizeof(nst::i32);
					break;
				case DynamicKey::UI8 :
				case DynamicKey::I8 :
					l = sizeof(nst::i64);
					break;
				case DynamicKey::R4 :
					l = sizeof(float);
					break;
				case DynamicKey::R8 :
					l = sizeof(double);
					break;
				case DynamicKey::S:{					
					nst::i16 
					ls = sizeof(nst::i16),					
					ll = *(const nst::i16*)ld;				
					l = ll+ls;
								   }
					break;
				case DynamicKey::KEY_MAX:
					l = sizeof(nst::u8);
					break;
				default:
					err_print("unknown type in index key");
					break;
				};				
				ld += l; // increments buffer
				ll += l; // increments length
				++r;
				if(ll >= size()) break;				
				lt = *ld;								
			}		
			return r;
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
		nst::u8* col_data(){
			return data();
		}
		size_t col_bytes() const{
			return sizeof(row) + size();
		}
		nst::u8 col_byte(size_t which) const {
			if(which < sizeof(row)){
				return ((const nst::u8*)(&row))[which];
			}else{
				return data()[which-sizeof(row)];
			}
		}
		
		void col_resize_bytes(size_t to){
			row = 0;
			_resize_buffer(to-sizeof(row));
			if(size() != to-sizeof(row)){
				err_print("resize error\n");
			}
		}

		void col_push_byte_row(size_t which, nst::u8 v) {
			
			((nst::u8*)(&row))[which] = v;				
			
		}

		template<typename _IntType>
		_IntType col_int(size_t which) const {
			
			if(which < sizeof(row)/sizeof(_IntType)){
				return ((const _IntType*)(&row))[which];
			}else{
				return ((const _IntType*)data())[which-(sizeof(row)/sizeof(_IntType))];
			}
		}
		template<typename _IntType>
		void col_push_int(size_t which, _IntType v) {
			if(which < sizeof(row)/sizeof(_IntType)){
				((_IntType*)(&row))[which] = v;				
			}else{
				((_IntType*)data())[which - sizeof(row)/sizeof(_IntType)] = v;
			}
		}
		template<typename _IntType>
		void col_push_int_row(size_t which, _IntType v) {
			
			((_IntType*)(&row))[which] = v;				
			
		}
		
		template<typename _IntType>
		void col_push_int_key(size_t which, nst::u8 v) {
			
			((_IntType*)data())[which] = v;		
			
		}

		void col_push_byte_key(size_t which, nst::u8 v) {
			
			data()[which] = v;		
			
		}
		void col_push_byte(size_t which, nst::u8 v) {
			if(which < sizeof(row)){
				((nst::u8*)(&row))[which] = v;				
			}else{
				data()[which - sizeof(row)] = v;
			}
		}
		template<int _InStaticSize>
		DynamicKey& operator=(const DynamicKey<_InStaticSize>& right){
			if((void *)this == (void*)&right) return *this;
			if(bs < static_size && right.size() < static_size){
				bs = right.size();
				memcpy(buf, right.data(), bs);
			}else{
				_resize_buffer(right.size());
				
				memcpy(data(), right.data(), right.size());
			}
			
			row = right.row;
			
			return *this;
		}
	
		DynamicKey& operator=(const DynamicKey& right){
			if((void *)this == (void*)&right) return *this;
			if(bs < static_size && right.bs < static_size){
				bs = right.bs;
				memcpy(buf, right.data(), bs);
			}else{
				_resize_buffer(right.size());
				memcpy(data(), right.data(), right.size());
			}
			row = right.row;
			
			return *this;
		}
		template<int _InStaticSize>
		const DynamicKey<_InStaticSize>& return_or_copy(DynamicKey<_InStaticSize>& s) const {
			s = *this;
			return s;
		}
		template<int _InStaticSize>
		DynamicKey<_InStaticSize>& return_or_copy(DynamicKey<_InStaticSize>& s) {
			s = *this;
			return s;
		}
		
		const DynamicKey& return_or_copy(DynamicKey& s) const {			
			return *this;
		}
		
		DynamicKey& return_or_copy(DynamicKey& s) {
			return *this;
		}

		inline size_t get_hash() const {
			size_t r = 0;
			MurmurHash3_x86_32(data(), size(), 0, &r);
			return r;
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
				case DynamicKey::UI1 :
				case DynamicKey::I1 :
					l=sizeof(nst::i8);
					if(!n){
						out = (_IntType)(*(nst::i8*)ld )	;

					}
					break;
				case DynamicKey::UI2:
				case DynamicKey::I2:
					l = sizeof(nst::i16);
					if(!n){
						out = (_IntType)(*(nst::i16*)ld );

					}
					break;
				case DynamicKey::UI4:
				case DynamicKey::I4:
					l = sizeof(nst::i32);
					if(!n){
						out = (_IntType)(*(nst::i32*)ld);

					}
					break;
				case DynamicKey::UI8 :
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
				case DynamicKey::KEY_MAX:
					l = 1;					
					break;
				default:
					err_print("unknown key type");
					break;
				};
				ld += l;
				lt = *ld;
			};
			return true;
		}
		template<int _InStaticSize>	
		bool operator<(const DynamicKey<_InStaticSize>& right) const {

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
				case DynamicKey::UI8 :
					l = sizeof(nst::i64);
					if(*(nst::u64*)ld < *(nst::u64*)rd)
						return true;
					else if(*(nst::u64*)ld > *(nst::u64*)rd)
						return false;
					else r = 0;
				break;
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
				case DynamicKey::S:{
					nst::i16 ls = sizeof(nst::i16);
					nst::i16 lr = *(const nst::i16*)rd;
					nst::i16 ll = *(const nst::i16*)ld;
					r = memcmp(ld+ls, rd+ls, std::min<nst::i16>(lr, ll));
					if(r !=0) return r<0;
					if(lr != ll)
						return (lr < ll);
					l = ll+ls;
								   }
					break;
				case DynamicKey::KEY_MAX:
					l = 1;
					r = 0;
					break;
				default:
					switch(lt){
						case DynamicKey::UI1 :
						l=sizeof(nst::i8);
						if(*(nst::u8*)ld < *(nst::u8*)rd)
							return true;
						else if(*(nst::u8*)ld > *(nst::u8*)rd)
							return false;
						else r = 0;
						break;
					case DynamicKey::UI2:
						l = sizeof(nst::i16);
						if(*(nst::u16*)ld < *(nst::u16*)rd)
							return true;
						else if(*(nst::u16*)ld > *(nst::u16*)rd)
							return false;
						else r = 0;
						break;
					case DynamicKey::UI4:
						l = sizeof(nst::i32);
						if(*(nst::u32*)ld < *(nst::u32*)rd)
							return true;
						else if(*(nst::i32*)ld > *(nst::i32*)rd)
							return false;
						else r = 0;
						break;
					default:
						err_print("unknown key type");
						break;
					};
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
		template<int _InStaticSize>
		inline bool operator!=(const DynamicKey<_InStaticSize>& right) const {
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
			if(size()==0){
				printf("[TS] [WARNING] writing emtpty key\n");
			}
			buffer_type::iterator writer = w;
			writer = leb128::write_signed(writer, row);
			writer = leb128::write_signed(writer, size());


			//memcpy(&(*writer),data(),size());
			//writer += size();
			const u8 * end = data()+size();
			for(const u8 * d = data(); d < end; ++d,++writer){
				*writer = *d;
			}
			return writer;
		};

		NS_STORAGE::buffer_type::const_iterator read(const NS_STORAGE::buffer_type& buffer, NS_STORAGE::buffer_type::const_iterator r) {
			using namespace NS_STORAGE;
			buffer_type::const_iterator reader = r;
			if(reader != buffer.end()){
				row = leb128::read_unsigned64(reader,buffer.end());
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
	typedef DynamicKey<42> StandardDynamicKey;

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
		template<int _StaticSize>
		PrimitiveDynamicKey(const DynamicKey<_StaticSize>& right)

		{
			row =right.row;
			right.p_decode(data);
		}
		template<int _StaticSize>
		inline bool left_equal_key(const DynamicKey<_StaticSize>& right) const {
			_DataType rdata;
			right.p_decode(rdata);
			return data == rdata;
		}

		inline bool left_equal_key(const PrimitiveDynamicKey& right) const {

			return data == right.data;
		}

		inline bool equal_key(const PrimitiveDynamicKey& right) const {
			//if( data == right.data)
			//	return row == right.row;
			return data == right.data;

		}

		PrimitiveDynamicKey& operator=(const PrimitiveDynamicKey& right){
			data = right.data;
			row = right.row;
			return *this;
		}
		template<int _StaticSize>
		PrimitiveDynamicKey& operator=(const DynamicKey<_StaticSize>& right){
			row =right.row;
			right.p_decode(data);
			return *this;
		}

		template<int _StaticSize>
		BareKey get_bare(DynamicKey<_StaticSize>& x) const {
			x.clear();
			_FieldType f;
			f.set_value(data);
			x.add(f);
			x.row = (*this).row;
			return x.get_bare();
		}
		
		template<int _StaticSize>
		const DynamicKey<_StaticSize>& return_or_copy(DynamicKey<_StaticSize>& x) const {
			x.clear();
			_FieldType f;
			f.set_value(data);
			x.add(f);
			x.row = (*this).row;
			return x;
		}
		template<int _StaticSize>
		DynamicKey<_StaticSize>& return_or_copy(DynamicKey<_StaticSize>& x) {
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
		size_t col_bytes() const{
			return sizeof(row) + sizeof(_DataType);
		}
		nst::u8 col_byte(size_t which) const {
			if(which < sizeof(row)){
				return ((const nst::u8*)(&row))[which]; //((row >> ((row_type)(which*8))) & 0xFF);
			}else{
				
				return ((const nst::u8*)(&data))[which-sizeof(row)];
			}
		}
		void col_resize_bytes(size_t){
			row = 0;
		}
		void col_push_byte(size_t which, nst::u8 v) {
			if(which < sizeof(row)){
				((nst::u8*)(&row))[which] = v;				
			}else{
				((nst::u8*)(&data))[which-sizeof(row)] = v;
			}
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

		inline size_t get_hash() const {
			return (size_t)data;
		}
		NS_STORAGE::u32 stored() const {
			_FieldType f;
			f.set_value(data);

			StandardDynamicKey s;
			s.add(f);
			s.row = (*this).row;
			return s.stored();
		};

		NS_STORAGE::buffer_type::iterator store(NS_STORAGE::buffer_type::iterator w) const {
			using namespace NS_STORAGE;
			_FieldType f;
			StandardDynamicKey s;
			f.set_value((*this).data);
			s.add(f);
			s.row = (*this).row;
			return s.store(w);
		};

		NS_STORAGE::buffer_type::const_iterator read(const NS_STORAGE::buffer_type& buffer, NS_STORAGE::buffer_type::const_iterator r) {
			using namespace NS_STORAGE;
			buffer_type::const_iterator reader = r;
			StandardDynamicKey s;
			reader = s.read(buffer, reader);
			s.p_decode(data);
			row = s.row;
			return reader;
		};
	};
	template<class _Any>
	struct byte_col_encoder_impl{
		template<typename _PrimitiveInt>
		size_t size_int(const _PrimitiveInt value) const {
			return sizeof(_PrimitiveInt);
		}
		template<typename _PrimitiveInt>
		size_t store_int(nst::buffer_type::iterator& writer,_PrimitiveInt value) const {
			*((_PrimitiveInt*)&(*writer)) = value;
			writer+=sizeof(_PrimitiveInt);
			return sizeof(_PrimitiveInt);
		}
		template<typename _PrimitiveInt>
		size_t read_int(_PrimitiveInt &value, nst::buffer_type::const_iterator& reader) const {
			value = *((const _PrimitiveInt*)&(*reader));
			reader += sizeof(_PrimitiveInt);
			return sizeof(_PrimitiveInt);
		}
		template<typename _IntType>
		void var_int_encode(nst::buffer_type::iterator& writer, _IntType value){
			_IntType r = value;
			if(value < 0){
				r = -r;
			}
			while(r > 63){ /// the last value also contains the sign
				(*writer) = ((nst::u8)(value & 127)) | 128;
				++writer;
				r >>= 7; /// 127
			}
			if(value < 0){
				(*writer) = ((nst::u8)(value & 63)) | 64;
			}else{
				(*writer) = ((nst::u8)(value & 63));
			}
			
		}
		template<typename _IntType>
		_IntType var_int_decode(nst::buffer_type::const_iterator& reader){
			_IntType r = 0;			
			for(;;){ /// the last value also contains the sign
				nst::u8 v = (*reader);
				++reader;
				if(!(v & 128)){
					r |= (_IntType)(v & 63);
					if((v & 64)) r = (_IntType)(-1*(_IntType)r);
					break;
				}else{					
					r |= (_IntType)(v & 127);
					r <<= 7;
				}				
			}		
			return r;
		}
	private:
		typedef nst::u32 _IntCodeType;
	private:
		mutable nst::buffer_type pcol;
		mutable std::vector<nst::u8*> data_col;
		mutable std::vector<_IntCodeType> int_col;
	public:
		template<int _StaticSize>
		void encode(nst::buffer_type::iterator& writer, const DynamicKey<_StaticSize>* keys, size_t occupants) const {
			bool tests = false;
			nst::buffer_type::const_iterator reader = writer;
			size_t colls = keys[0].col_bytes();			

			for(size_t k = 1; k < occupants; ++k){
				if(colls != keys[k].col_bytes()){
					colls = 0;
					break;
				}
			}
			
			pcol.resize(occupants*sizeof(_IntCodeType));
			store_int(writer, colls);
			size_t int_colls = colls - (colls % sizeof(_IntCodeType));
			for(size_t c = 0; c < colls/sizeof(_IntCodeType); ++c){
				_IntCodeType tminus = 0,prev = 0;
				_IntCodeType* col = (_IntCodeType*)&pcol[0];
				for(size_t k = 0; k < occupants; ++k){										
					tminus = keys[k].col_int<_IntCodeType>(c);
					_IntCodeType t = tminus;						
					tminus -= prev;
					col[k] = tminus;					
					prev = t;					
				}
				writer = std::copy(pcol.begin(), pcol.end(), writer);
			}
			pcol.resize(occupants);
			nst::u8* col = &pcol[0];
			for(size_t c = int_colls; c < colls; ++c){
				nst::u8 tminus = 0,prev = 0;
				for(size_t k = 0; k < occupants; ++k){										
					tminus = keys[k].col_byte(c);
					nst::u8 t = tminus;						
					tminus -= prev;
					col[k] = tminus;
					prev = t;					
				}
			
				writer = std::copy(pcol.begin(),pcol.end(), writer);
			}	
			if(tests){
				for(size_t k = 0; k < occupants; ++k){
					keys[k].verify();
				}
			
				nst::buffer_type::const_iterator end = writer;
				
				std::vector<DynamicKey<_StaticSize>> test_keys;
				test_keys.resize(occupants);
				nst::buffer_type buffer;
				buffer.insert(buffer.begin(), reader, end);
				nst::buffer_type::const_iterator r_test = buffer.begin();
				decode(buffer, r_test, &test_keys[0], occupants);
				size_t ltest = r_test - buffer.begin();
				size_t rtest = end - reader;
				if(ltest != rtest){
					err_print("Read length does not verify %lld != %lld\n", (long long)ltest,(long long)rtest);
				}
				if(ltest != encoded_size(keys,occupants)){
					err_print("Read length does not verify with encoded size %lld != %lld\n", (long long)ltest,(long long)encoded_size(keys,occupants));
				}
				for(size_t k = 0; k < occupants; ++k){
					if(test_keys[k] != keys[k]){
						err_print("keys dont compare");
					}
				}
			}
		}
		/// col oriented encoding for exploiting the sort order of keys on a b-tree page		
		template<typename _ByteOrientedKey>
		void encode(nst::buffer_type::iterator& writer, const _ByteOrientedKey* keys, size_t occupants) const {
			bool tests = false;
			nst::buffer_type::const_iterator reader = writer;
			size_t colls = keys[0].col_bytes();			

			for(size_t k = 1; k < occupants; ++k){
				if(colls != keys[k].col_bytes()){
					colls = 0;
					break;
				}
			}
			
			pcol.resize(occupants);
			nst::u8* col = &pcol[0];
			store_int(writer, colls);
			for(size_t c = 0; c < colls; ++c){
				//for(size_t k = 0; k < occupants; ++k){
				//	col[k] = keys[k].col_byte(c);
				//}				
				nst::u8 tminus = 0,prev = 0;
				for(size_t k = 0; k < occupants; ++k){										
					tminus = keys[k].col_byte(c);
					nst::u8 t = tminus;						
					tminus -= prev;
					col[k] = tminus;
					prev = t;					
				}
				
				writer = std::copy(pcol.begin(),pcol.end(), writer);
			}
			if(tests){
				nst::buffer_type::const_iterator end = writer;
				
				std::vector<_ByteOrientedKey> test_keys;
				test_keys.resize(occupants);
				nst::buffer_type buffer;
				buffer.insert(buffer.begin(), reader, end);
				nst::buffer_type::const_iterator r_test = buffer.begin();
				decode(buffer, r_test, &test_keys[0], occupants);
				size_t ltest = r_test - buffer.begin();
				size_t rtest = end - reader;
				if(ltest != rtest){
					printf("[ERROR] [TS] Read length does not verify %lld != %lld\n", (long long)ltest,(long long)rtest);
				}
				if(ltest != encoded_size(keys,occupants)){
					printf("[ERROR] [TS] Read length does not verify with encoded size %lld != %lld\n", (long long)ltest,(long long)encoded_size(keys,occupants));
				}
				for(size_t k = 0; k < occupants; ++k){
					if(test_keys[k] != keys[k]){
						printf("houston we have a problem\n");
					}
				}
			}
			
			
		}
		template<typename _ByteOrientedKey>
		size_t encoded_size(const _ByteOrientedKey* keys, size_t occupants) const {
			
			size_t colls = keys[0].col_bytes();
			size_t result = 0;		
			
			for(size_t k = 1; k < occupants; ++k){
				if(colls != keys[k].col_bytes()){
					colls = 0;
					break;
				}
			}
			if(!colls) return 0;
			result += size_int(colls) + (occupants*colls);			
			return result;
		}
		template<int _StaticSize>
		void dddecode(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader,DynamicKey<_StaticSize>* keys, size_t occupants) const {
			size_t colls = 0;
			read_int(colls, reader);
			data_col.resize(occupants);
			for(size_t o = 0; o < occupants; ++o){
				//keys[o] = _ByteOrientedKey();
				keys[o].col_resize_bytes(colls);
				data_col[o] = keys[o].col_data();
			}
			for(size_t c = 0; c < sizeof(_Rid); ++c){				
				
				nst::u8 dt,d = 0;
				
				nst::u32 k = 0;
				nst::u32 o = occupants;
				for( ;k < o; ++k){
					dt =  *reader + d;
					keys[k].col_push_byte_row(c, dt);
					d = dt;
					++reader;
				}
		
			}			
			size_t crow = 0;
			for(size_t c = sizeof(_Rid); c < colls; ++c){				
				
				nst::u8 dt,d = 0;
				
				nst::u32 k = 0;
				nst::u32 o = occupants;
				
				for( ;k < o; ++k){
					dt =  *reader + d;
					data_col[k][crow] = dt;
					d = dt;
					++reader;
				}
				++crow;
			}
		}
		template<int _StaticSize>
		size_t encoded_size(const DynamicKey<_StaticSize>* keys, size_t occupants) const {
			
			size_t colls = keys[0].col_bytes();
			size_t result = 0;		
			
			for(size_t k = 1; k < occupants; ++k){
				if(colls != keys[k].col_bytes()){
					colls = 0;
					break;
				}
			}
			if(!colls) return 0;
			size_t int_colls = colls - (colls % sizeof(_IntCodeType));

			result += size_int(colls);			
			result += (occupants*colls);
			return result;
		}
		template<int _StaticSize>
		void decode(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader,DynamicKey<_StaticSize>* keys, size_t occupants) const {
			size_t colls = 0;
			read_int(colls, reader);
			for(size_t o = 0; o < occupants; ++o){
				//keys[o] = _ByteOrientedKey();
				keys[o].col_resize_bytes(colls);
			}
						
			for(size_t c = 0; c < colls/sizeof(_IntCodeType); ++c){								
				_IntCodeType dt,d = 0;				
				nst::u32 k = 0;
				nst::u32 o = occupants;
				for( ;k < o; ++k){
					dt = *((const _IntCodeType*)&(*reader)) + d;
					keys[k].col_push_int(c, dt);
					d = dt;
					reader+=sizeof(_IntCodeType);
				}		
			}

			for(size_t c = colls - (colls % sizeof(_IntCodeType)); c < colls; ++c){				
				
				nst::u8 dt,d = 0;
				
				nst::u32 k = 0;
				nst::u32 o = occupants;
				for( ;k < o; ++k){
					dt =  *reader + d;
					keys[k].col_push_byte(c, dt);
					d = dt;
					++reader;
				}		
			}
					}
		template<typename _ByteOrientedKey>
		void decode(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader,_ByteOrientedKey* keys, size_t occupants) const {
			size_t colls = 0;
			read_int(colls, reader);
			for(size_t o = 0; o < occupants; ++o){
				//keys[o] = _ByteOrientedKey();
				keys[o].col_resize_bytes(colls);
			}
						
			for(size_t c = 0; c < colls; ++c){				
				
				nst::u8 dt,d = 0;
				
				nst::u32 k = 0;
				nst::u32 o = occupants;
				for( ;k < o; ++k){
					dt =  *reader + d;
					keys[k].col_push_byte(c, dt);
					d = dt;
					++reader;
				}
		
			}
		}
	};
	/// pivot encoder effective for sorted data i.e. indexes
	template<class _ByteOrientedKey, class _Value>
	struct byte_col_encoder  {
	private:
		byte_col_encoder_impl<_ByteOrientedKey> encoder;
		
	public:

		inline bool encoded(bool multi) const{

			return !multi; // only encodes uniques;
		}
		
		inline bool encoded_values(bool multi) const{

			return false; 
		}
		
		typedef nst::u16 count_t;
		/// delegate value decoder
		
		void decode_values(...) {			
		}
		/// delegate value encoder
		void encode_values(...) const {
			
		}
		/// delegate value encoder
		size_t encoded_values_size(...) {
		
			return 0;
		}
		
		/// col oriented encoding for exploiting the sort order of keys on a b-tree page		
		void encode(nst::buffer_type::iterator& writer, const _ByteOrientedKey* keys, size_t occupants) const {
			encoder.encode(writer, keys, occupants);			
		}

		size_t encoded_size(const _ByteOrientedKey* keys, size_t occupants) const {
			return encoder.encoded_size(keys, occupants);						
		}
		
		void decode(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader,_ByteOrientedKey* keys, size_t occupants) const {
			encoder.decode(buffer, reader, keys, occupants);			
		}
		inline bool can(const _ByteOrientedKey &first , const _ByteOrientedKey &last, int size) const
		{
			return false ;
		}
		inline unsigned int interpolate(const _ByteOrientedKey &k, const _ByteOrientedKey &first , const _ByteOrientedKey &last, int size) const
		{
			return 0;
		}
	};

	template <class _StoredRowId, class _Stored>
	struct int_terpolator{
		byte_col_encoder_impl<_Stored> value_encoder;

		template<typename _PrimitiveInt>
		size_t size_int(const _PrimitiveInt value) const {
			return sizeof(_PrimitiveInt);
		}
		template<typename _PrimitiveInt>
		size_t store_int(nst::buffer_type::iterator& writer,_PrimitiveInt value) const {
			*((_PrimitiveInt*)&(*writer)) = value;
			writer+=sizeof(_PrimitiveInt);
			return sizeof(_PrimitiveInt);
		}
		template<typename _PrimitiveInt>
		size_t read_int(_PrimitiveInt &value, nst::buffer_type::const_iterator& reader) const {
			value = *((const _PrimitiveInt*)&(*reader));
			reader += sizeof(_PrimitiveInt);
			return sizeof(_PrimitiveInt);
		}

	//	typedef typename _Stored::list_encoder value_encoder;
	//	value_encoder encoder; /// delegate encoder/decoder
		/// function assumes a list of increasing integer values
		inline bool encoded(bool multi) const
		{
			return !multi; // only encodes uniques;
		}
		inline bool encoded_values(bool multi) const
		{
			return true; //value_encoder.encoded(multi); // only encodes uniques;
		}
		typedef nst::u16 count_t;
		/// delegate value decoder
		void decode_values(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader,_Stored* values, count_t occupants) {

			value_encoder.decode(buffer, reader, values, occupants);
		}
		/// delegate value encoder
		void encode_values(nst::buffer_type& buffer, nst::buffer_type::iterator& writer,const _Stored* values, count_t occupants) const {

			value_encoder.encode(writer, values, occupants);
		}
		/// delegate value encoder
		size_t encoded_values_size(const _Stored* values, count_t occupants) {

			return value_encoder.encoded_size(values, occupants);
		}


		/// decode mixed run length and delta for simple sequences
		void decode(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader,_StoredRowId* keys, count_t occupants) const {
			_StoredRowId prev = 0;
			for(unsigned k = 0; k < occupants; ++k){	
				_Rid val = 0;				
				read_int(val, reader);
				keys[k].set_value(val + prev.get_value());
				prev = keys[k];
			}
			
		}

		/// encoding mixes run length and delta encoding for simple sequences
		int encoded_size(const _StoredRowId* keys, count_t occupants) const {
			return occupants * sizeof(_Rid);
			
		}

		void encode(nst::buffer_type::iterator& writer,const _StoredRowId* keys, count_t occupants) const {
			_StoredRowId prev;
			for(unsigned k = 0; k < occupants; ++k){
				_StoredRowId t = keys[k];
				_Rid val = keys[k].get_value() - prev.get_value();				
				store_int(writer, val);				
				prev = t;
			}
						
		}
		inline bool can(const _StoredRowId &first , const _StoredRowId &last, int size) const
		{
			return ::abs((long long)size - (long long)::abs( (long long)last.get_value() - (long long)first.get_value() )) > 0 ;
		}
		inline unsigned int interpolate(const _StoredRowId &k, const _StoredRowId &first , const _StoredRowId &last, int size) const
		{
			if(last.get_value()>first.get_value())
				return (int)(size*(k.get_value()-first.get_value()))/(last.get_value()-first.get_value());
			return 0;
		}

		
	};

	
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
		virtual bool previous() = 0;
		virtual void first() = 0;
		virtual void last() = 0;
		virtual nst::u64 count(const index_iterator_interface& in) = 0;
		virtual BareKey get_bare_key() = 0;
		virtual StandardDynamicKey& get_key() = 0;
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
		StandardDynamicKey resolved;
		std::string name;
		StandardDynamicKey temp;
		bool do_update;
		void set_ix(int ix){
			this->ix = ix;
		}
		int get_ix(){
			return ix;
		}
		index_interface() : ix(0),fields_indexed(0),destroyed(false),do_update(false){
		}
		virtual ~index_interface(){
			if(destroyed)
				printf("destroyed index already\n");
			destroyed = true;
		};
		virtual bool is_unique() const = 0;
		virtual void set_col_index(int ix)= 0;
		virtual void set_fields_indexed(int indexed)=0;
		virtual void push_part(_Rid part)=0;
		virtual void push_density(_Rid dens) = 0;
		virtual size_t densities() const = 0;
		virtual stored::_Rid& density_at(size_t at) = 0;
		virtual const stored::_Rid& density_at(size_t at) const = 0;
		virtual void end(index_iterator_interface& out)=0;
		virtual index_iterator_interface * get_index_iterator(nst::u64 inst) = 0;		
		virtual index_iterator_interface * get_prepared_index_iterator() = 0;
		virtual index_iterator_interface * get_first1() = 0;
		virtual index_iterator_interface * get_last1() = 0;

		virtual void first(index_iterator_interface& out)=0;
		
		virtual void lower_(index_iterator_interface& out,const StandardDynamicKey& key)=0;
		
		virtual void lower(index_iterator_interface& out,const StandardDynamicKey& key)=0;
		virtual void upper(index_iterator_interface& out, const StandardDynamicKey& key)=0;
		virtual void add(const StandardDynamicKey& k)=0;
		virtual void remove(const StandardDynamicKey& k) = 0;

		virtual _Rid resolve(StandardDynamicKey& k) = 0;
		virtual void find(index_iterator_interface& out, const StandardDynamicKey& key) = 0;
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
//typedef stored::Blobule<false, 48> BlobStored;
//typedef stored::Blobule<true, 48> VarCharStored;

namespace stx{
};
#endif
