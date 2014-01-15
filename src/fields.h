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
#include <stx/storage/basic_storage.h>
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
extern	char treestore_collumn_cache ;
extern  char treestore_predictive_hash ;
extern  char treestore_reduce_tree_use_on_unlock;
namespace storage_workers{
	typedef asynchronous::QueueManager<asynchronous::AbstractWorker> _WorkerManager;
	

	extern unsigned int get_next_counter();
	extern const int MAX_WORKER_MANAGERS;
	extern _WorkerManager & get_threads(unsigned int id);
};

namespace NS_STORAGE = stx::storage;
typedef NS_STORAGE::synchronized synchronized;
namespace stored{

	static const double	MB = 1024.0f*1024.0f;
	static const double	GB = 1024.0f*1024.0f*1024.0f;

	inline double units(NS_STORAGE::u64 in, double unit){
		return in / unit;
	}

	template<typename _IntType>
	class IntTypeStored {

	public:
		typedef _IntType value_type;
		_IntType value;
	public:
		inline _IntType get_value() const {

			return value;
		}
		inline void set_value(_IntType nv) {
			value = nv;
		}
		
		explicit IntTypeStored(_IntType i):value(i){
		}

		IntTypeStored():value(0){
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
		NS_STORAGE::buffer_type::const_iterator read(NS_STORAGE::buffer_type::const_iterator r) {
			NS_STORAGE::buffer_type::const_iterator reader = r;
			value = NS_STORAGE::leb128::read_signed(reader);
			return reader;
		};
	};

	template<typename _FType>
	class FTypeStored {
	protected:
		_FType value;
	public:
		inline _FType get_value() const {
			return value;
		}
		inline void set_value(_FType nv) {
			value = nv;
		}
		FTypeStored() : value((_FType)0.0f){
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
		NS_STORAGE::buffer_type::const_iterator read(NS_STORAGE::buffer_type::const_iterator r) {
			NS_STORAGE::buffer_type::const_iterator reader = r;
			memcpy(&value, (const NS_STORAGE::u8*)&(*reader), sizeof(value));
			reader+=sizeof(value);
			return reader;
		};
	};

	template<typename _FType>
	class VectorTypeStored {
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

		NS_STORAGE::buffer_type::const_iterator read(NS_STORAGE::buffer_type::const_iterator r) {
			NS_STORAGE::buffer_type::iterator reader = r;
			value.resize(NS_STORAGE::leb128::read_signed(reader));
			memcpy(&value, (NS_STORAGE::u8*)reader, value.size()*sizeof(_FType::value_type));
			reader += value.size();
			return reader;
		};
	};
	template<bool CHAR_LIKE, int _ConstSize = 16>
	class Blobule {
	public:
		
		static const int CHAR_TYPE = CHAR_LIKE ? 1 : 0;
	protected:
		typedef NS_STORAGE::u8 _size_type;
		typedef NS_STORAGE::u8 _BufferSize;
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
			if(bytes <= _ConstSize){
				return buf;
			}
			return extract_ptr();
		}
		const NS_STORAGE::u8* data() const{
			if(bytes <= _ConstSize){
				return buf;
			}
			return extract_ptr();
		}
		NS_STORAGE::u8* _resize_buffer(size_t nbytes){
			using namespace NS_STORAGE;
			NS_STORAGE::u8* r = data();
			if(nbytes > bytes){
				nst::add_buffer_use (nbytes);
				NS_STORAGE::u8 * nbuf = new NS_STORAGE::u8[nbytes];
				memcpy(nbuf, r, std::min<size_t>(nbytes, size));
				if(bytes > _ConstSize){
					nst::remove_buffer_use  (bytes);
					delete r;
				}
				memcpy(buf, &nbuf, sizeof(u8*));
				bytes = (u32)nbytes;
				r = nbuf;
			}
			return r;
		}
		void _add( const void * v, size_t count, bool end_term = false){
			_BufferSize extra = end_term ? 1 : 0;
			_resize_buffer(count + extra);
			memcpy(data(),v,count);
			if(end_term) data()[count] = 0;
			size+=(_BufferSize)count + extra;
		}
		bool less(const Blobule& right) const {
			using namespace NS_STORAGE;

			const u8 * lp = data();
			const u8 * rp = right.data();
			int r = memcmp(lp,rp,std::min<_BufferSize>(size,right.size));
			if(r != 0){
				return (r < 0);
			}

			return (size < right.size);
		}
		bool not_equal(const Blobule& right) const {
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

			return false;

		}
	public:
		char * get_value(){
			return chars();
		}
		const char * get_value() const {
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
		~Blobule(){
			if(bytes > _ConstSize){
				nst::remove_buffer_use  (bytes);
				delete data();
			}
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

		inline bool operator<(const Blobule& right) const {
			return less(right);
		}

		inline bool operator!=(const Blobule& right) const {

			return not_equal(right);
		}
		inline bool operator==(const Blobule& right) const {

			return !not_equal(right);
		}
		inline NS_STORAGE::u8  get_size() const {
			return size;
		}
		NS_STORAGE::u32 stored() const {
			return NS_STORAGE::leb128::signed_size((*this).size)+(*this).size;
		};

		NS_STORAGE::buffer_type::iterator store(NS_STORAGE::buffer_type::iterator w) const {
			using namespace NS_STORAGE;
			buffer_type::iterator writer = w;
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
			size = leb128::read_signed(reader);
			_resize_buffer(size);
			const u8 * end = data()+size;
			for(u8 * d = data(); d < end; ++d,++reader){
				*d = *reader;
			}
			return reader;
		};
	};





};
namespace stored{
	typedef NS_STORAGE::u32 _Rid;
	static const _Rid MAX_ROWS = 0xFFFFFFFFul;

	typedef std::vector<int> _Parts;
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
	typedef stored::Blobule<false, 12> BlobStored;
	typedef stored::Blobule<true, 12> VarCharStored;

	template<class _Data>
		struct entropy_t{
			nst::u64 samples;

			entropy_t():samples(0){
					
			}
			void sample(const _Data& data){
				++samples;
			}
			nst::u64 get_samples() const {
				return samples;
			}
			nst::u64 get_entropy() const {
				return samples;
			}
		};

		template<>
		struct entropy_t<stored::IntStored>{
			typedef stored::IntStored _Sampled;
			typedef std::unordered_map<nst::i64,nst::u64> _Histogram;
			nst::u64 samples;
			_Histogram histogram;

			entropy_t():samples(0){
					
			}
			void sample(const _Sampled & data){
				histogram[data.get_value()]++;
			}
			nst::u64 get_samples() const {
				return samples;
			}
			nst::u64 get_entropy() const {
				return histogram.size();
			}

		};

		template<typename _IntType>
		struct int_entropy_t{
			typedef _IntType _Sampled;
			typedef std::map<typename _Sampled, nst::u64> _Histogram;
			nst::u64 samples;

			_Histogram histogram;
				
			int_entropy_t():samples(0){
					
			}
			void clear(){
				samples = 0;
				histogram.clear();
			}
			void sample(const _IntType& data){
				histogram[data]++;
				++samples;
			}

			nst::u64 get_samples() const {
				return samples;
			}

			nst::u64 get_entropy(){
				return histogram.size();
			}

		};

		
		class UninitializedCodeException : public std::exception{
		public :
			/// the code has not been initialized
			UninitializedCodeException() throw(){
				printf("*********** UninitializedCodeException\n");
			}
			~UninitializedCodeException(){};
		};
		/// class for fixed size entropy coded buffer
		template<class _IntType>
		struct int_fix_encoded_buffer{
			typedef nst::u16 _BucketType;/// use a configurable bucket type for larger code size performance
			static const _BucketType BUCKET_BITS = sizeof(_BucketType)<<3;
			typedef int_entropy_t<_IntType> _Entropy;
			
			typedef std::map< typename _IntType, _BucketType> _CodeMap;
			typedef std::vector<typename _IntType> _DeCodeMap;
			typedef std::vector<_BucketType> _Data;
			_Entropy stats;
			_CodeMap codes;
			_DeCodeMap decodes;
			_Data data;
			typename _IntType _null_val;
			_BucketType code_size;
			int_fix_encoded_buffer() : code_size(0){
			}

			size_t capacity() const {
				return sizeof(typename _IntType)*(codes.size()+decodes.size())+data.capacity();
			}

			_Entropy& get_stats(){
				return stats;
			}
			void clear(){
				code_size = 0;
				stats.clear();
				codes.clear();
				decodes.clear();
				data.clear();
			}
			
			/// Find smallest X in 2^X >= value
			inline nst::u32 bit_log2(nst::u32 value){
				nst::u32 bit;
				for (bit=0 ; value > 1 ; value>>=1, bit++) ;
				return bit;
			}

			bool empty() const {
				return code_size == 0;
			}

			void resize(_Rid rows){
				if(code_size==0) throw UninitializedCodeException();
				data.resize(((rows*code_size)/BUCKET_BITS)+1);
			}

			
			void encode(_Rid row, typename const _IntType &val){
				if(codes.count(val)){
					_BucketType bucket_start;
					_BucketType code = codes[val];
					nst::u32 bits_done = row * code_size;
					nst::u32 code_left = code_size;
					_BucketType* current = &data[bits_done / BUCKET_BITS]; /// the first bucket where all the action happens
					do{	/// write over BUCKET_BITS-bit buckets
						bucket_start = bits_done & (BUCKET_BITS-1);/// where to begin in the bucket
						_BucketType todo = std::min<_BucketType>(code_left, BUCKET_BITS-bucket_start);
						*current &= ~(((1 << todo) - 1) << bucket_start); /// clean the destination like 11100001
						*current |=  ( (_BucketType)( code & ( (1 << todo)-1 ) ) ) << bucket_start ;						
						code = (code >> todo);/// drop the bits written ready for next bucket/iteration
						bits_done += todo;
						code_left -= todo;
						if( ( bits_done & (BUCKET_BITS-1) ) == 0){
							++current; /// increment the bucket
						}
					}while(code_left > 0 );

				}else
					throw UninitializedCodeException();
			}
		
			typename const _IntType& decode(_Rid row) const {
				_BucketType code = 0;
				if(code_size > 0){
					_BucketType bucket_start, bucket;
					nst::u32 bit_start = row * code_size;				
					const _BucketType* current = &data[bit_start / BUCKET_BITS];			
					nst::u32 code_left = code_size;
					nst::u32 code_complete = 0;
					for(;;){	/// read from BUCKET_BITS-bit buckets
						bucket_start = bit_start & (BUCKET_BITS-1);/// where to begin in the bucket
						_BucketType todo = std::min<_BucketType>(code_size-code_complete, BUCKET_BITS-bucket_start);
						bucket = (*current >> bucket_start)& ( ( 1 << todo ) - 1);
												
						code |=  bucket << code_complete;		/// if bucket_start == 0 nothing happens				
						
						bit_start += todo;						
						code_complete += todo;
						if(code_complete > code_size )
							throw UninitializedCodeException();
						if(code_complete == code_size )
							break;

						if( ( bit_start & (BUCKET_BITS-1) ) == 0){
							++current; /// increment the bucket
						}
					}
					return decodes[code];
				}else
					throw UninitializedCodeException();
				return _null_val;
			}
			bool initialize(_Rid rows){

				if( ( (double)stats.get_samples() / (double)stats.get_entropy() ) > 1 && stats.get_entropy() < 1<<16){
					
					_Entropy::_Histogram::iterator h = (*this).stats.histogram.begin();
					nst::u64 mfreq = 0;
					nst::u32 words = 0;
					
					for(;h != (*this).stats.histogram.end();++h){
						if(mfreq < (*h).second) mfreq = (*h).second;
						codes[(*h).first] = words;
						decodes.resize(words+1);
						decodes[words] = (*h).first;
						++words;
						
					}
					
					if(words > 0){
						
						code_size = bit_log2((nst::u32)words) + 1;
							
					
						resize(rows);
					}
					
					
				}
				return code_size > 0;

			}
			void optimize(){
				(*this).codes.clear();
				(*this).stats.clear();
			}
		};	/// fixed in encoded buffer
		/// entropy coding decission class
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
			nst::u64 get_entropy() const {
				return 0;
			}
			size_t capacity() const {
				return 0;
			}
		};
#define _ENTROPY_CODING_
#ifdef _ENTROPY_CODING_
		template<>
		struct standard_entropy_coder<IntStored >{
			typedef IntStored _DataType;
			int_fix_encoded_buffer<_DataType> coder;
			bool empty() const {
				return coder.empty();
			}
			bool applicable() const {
				return true;
			}
			void sample(const IntStored &data){
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
			void set(_Rid row, const IntStored& data){
				coder.encode(row, data);
			}
			const IntStored& get( _Rid row) const {
				return coder.decode(row);
			}	
			nst::u64 get_entropy() const {
				return 0;// coder.get_stats().get_entropy();
			}
			size_t capacity() const {
				return coder.capacity();
			}
		};
		template<>
		struct standard_entropy_coder<UIntStored >{
			typedef UIntStored _DataType;
			int_fix_encoded_buffer<_DataType> coder;
			bool empty() const {
				return coder.empty();
			}
			bool applicable() const {
				return true;
			}
			void sample(const UIntStored &data){
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
			void set(_Rid row, const UIntStored& data){
				coder.encode(row, data);
			}
			const UIntStored&  get(_Rid row) const {
				return coder.decode(row);
			}	
			nst::u64 get_entropy() const {
				return 0;// coder.get_stats().get_entropy();
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
			nst::u64 get_entropy() const {
				return 0;// coder.get_stats().get_entropy();
			}
			size_t capacity() const {
				return coder.capacity();
			}
		};
		/**/
#endif
};
#endif
