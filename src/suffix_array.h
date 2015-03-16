#pragma once

#include <stx/storage/types.h>
#include <stx/btree_map.h>
#include "simple_storage.h"
#include "MurmurHash3.h"
#include <fse/fse.h>
namespace nst = stx::storage;
namespace suffix_array{
	const static nst::u32 suffix_size = 16;
	/// the term suffix is used loosely here
	struct suffix{

		nst::u8 data[suffix_size];

		suffix& operator=(const suffix& r){
			memcpy(data,r.data,suffix_size);

			return *this;
		}
		suffix() {
			memset(data,0, suffix_size);
		}
		suffix(const suffix& r){
			*this = r;
		}
		inline nst::u32 size() const {
			return suffix_size;
		}
		void assign(const nst::i8 * s, nst::u32 l){
			memset(data,0, suffix_size);
			memcpy((*this).data, s, suffix_size);

		}
		void assign(const nst::u8 * s, nst::u32 l){
			memset(data,0, suffix_size);
			memcpy((*this).data, s, suffix_size);
		}
		bool operator==(const suffix& r) const {

			return memcmp(data, r.data, suffix_size) == 0;
		}
		bool operator!=(const suffix& r) const {

			return memcmp(data, r.data, suffix_size) != 0;
		}

		bool operator<(const suffix& r) const {
			return memcmp(data, r.data, suffix_size) < 0;
		}

		operator size_t() const{
			return (size_t)get_hash();
		}

		inline nst::u32 get_hash() const {
			nst::u32 r = suffix_size;
			MurmurHash3_x86_32((*this).data, suffix_size, r, &r);
			return r;
		}

		nst::u32 store_size() const {
			return suffix_size;
		}
		nst::buffer_type::iterator store(nst::buffer_type::iterator w) const {
			using namespace nst;
			buffer_type::iterator writer = w;

			const u8 * end = data+suffix_size;
			for(const u8 * d = data; d < end; ++d,++writer){
				*writer = *d;
			}
			return writer;
		};

		nst::buffer_type::const_iterator read(nst::buffer_type::const_iterator r) {
			using namespace nst;
			buffer_type::const_iterator reader = r;
			const u8 * end = data+suffix_size;
			for(u8 * d = data; d < end; ++d,++reader){
				*d = *reader;
			}
			return reader;
		};
	};

	/// very fast ;-) capable of holding large ammounts in compressed ram and disc

	template<typename _BuffType,typename _IntType>
	class suffix_storage : public nst::basic_storage{
	public:

	private:

		typedef nst::sqlite_allocator<nst::stream_address, nst::buffer_type> _Allocations;

		_Allocations allocations;
		nst::stream_address boot;
		std::string name;
	public:

		suffix_storage(std::string name) : allocations( nst::default_name_factory(name)), boot(1),name(name){
			allocations.set_limit(1024*1024*32);
			allocations.begin();

			/// create a block at the boot address if its not there

			allocations.initialize(boot);

		}


		~suffix_storage() {
			try{
				/// TODO: for test move it to a specific 'close' ? function or remove completely
				allocations.rollback();
			}catch(const std::exception& ){
				/// nothing todo in destructor
			}
		}

		bool is_end(nst::buffer_type &b) const {
			return allocations.is_end(b);
		}
		nst::version_type get_allocated_version(){
			return allocations.get_allocated_version();
		}
		void complete(){
			allocations.complete();
		}
		std::string get_name() const {
			return name;
		}
		bool is_readonly() const{
			return false;
		}
		bool get_boot_value(nst::i64 &r, nst::stream_address boot){
			return get_boot_value(r);
		}

		bool get_boot_value(nst::i64 &r){
			r = 0;
			nst::buffer_type &ba = allocations.allocate(boot, nst::read); /// read it
			if(!ba.empty()){
				/// the b+tree/x map needs loading
				nst::buffer_type::const_iterator reader = ba.begin();
				r = nst::leb128::read_signed(reader);
			}
			return !ba.empty();
		}
		void set_boot_value(nst::i64 r,nst::i64 boot){
			set_boot_value(r);
		}
		void set_boot_value(nst::i64 r){

			nst::buffer_type &buffer = allocations.allocate(boot, nst::write); /// read it
			buffer.resize(nst::leb128::signed_size(r));
			nst::buffer_type::iterator writer = buffer.begin();

			nst::leb128::write_signed(writer, r);

		}


		/// a kind of auto commit - by starting the transaction immediately after commit
		void commit(){
			allocations.commit();
			allocations.begin();
		}
		nst::u64 get_greater_version_diff(nst::_VersionRequests& request){
			//if(_transaction != NULL){
				///get_allocations().get_greater_version_diff(request, response);
			//}
			return 0;
		}

		/// Storage functions called by b-tree
		/// allocate a new or existing buffer, new denoted by what == 0 else an existing
		/// buffer with the specfied stream address is returned - if the non nil address
		/// does not exist an exception is thrown'

		nst::buffer_type& allocate(nst::stream_address &what,nst::storage_action how){
			return allocations.allocate(what,how);
		}

		nst::u64 current_transaction_order() const {
			return 0;
		}

		/// function returning the stored size in bytes of a value
		nst::u32 store_size(const _BuffType& k) const {

			return k.store_size();
		}


		/// writes a key to a vector::iterator like writer
		template<typename _Iterator>
		void store(_Iterator& writer, const _BuffType &value) const {
			writer = value.store(writer);
		}


		/// reads a key from a vector::iterator like reader

		void retrieve(const nst::buffer_type& buffer, typename nst::buffer_type::const_iterator& reader, _BuffType &value) const {
			reader = value.read(reader);

		}

		/// function returning the stored size in bytes of a value
		nst::u32 store_size(const _IntType& k) const {
			return nst::leb128::signed_size(k);
		}


		/// writes a key to a vector::iterator like writer
		template<typename _Iterator>
		void store(_Iterator& writer, _IntType value) const {

			writer = nst::leb128::write_signed(writer, value);
		}


		/// reads a key from a vector::iterator like reader

		void retrieve(const nst::buffer_type& buffer, nst::buffer_type::const_iterator& reader, _IntType &value) const {

			value = nst::leb128::read_signed64(reader,buffer.end());
		}

	};
	typedef std::vector<suffix> _SuffixVector;
	typedef suffix_storage<suffix, nst::u64> _SuffixStorage;
	typedef stx::btree_map<suffix, nst::u64, _SuffixStorage > _SuffixArray;
	typedef std::map<suffix, nst::u64 > _StdSuffixArray;

	template<typename _MapType>
	class fe_hash_map{
	public:
		typedef typename _MapType::key_type key_type;
		typedef typename _MapType::mapped_type data_type;
		typedef std::pair<key_type,data_type> _MapPair;
		typedef std::vector<_MapPair> _MapVector;
		typedef std::vector<nst::u8> _FlagVector;
	protected:
		_MapVector data;
		_FlagVector flags;
		_MapType* backing;
		size_t fe_size;
		size_t hashed;
		nst::u32 calc_pos(const key_type &k) const {
			return(nst::u32) ((size_t)k) % fe_size;
		}
		inline bool has_backing() const {
			return backing != nullptr;
		}
	public:
		fe_hash_map() : backing(nullptr){
			hashed = 0;
			fe_size = 8*1024*1024/sizeof(_MapPair);
			data.resize(fe_size);
			flags.resize(fe_size);
		}
		void flush(){
			if(has_backing()){
				for(size_t f =0; f < fe_size;++f){
					if(flags[f] != 0){
						_MapPair unset = data[f];
						get_backing()[unset.first] = unset.second;
						flags[f] = 0;
					}
				}
			}
			hashed = 0;
		}
		void set_backing(_MapType& backing){
			for(size_t f =0; f < fe_size;++f){
				flags[f] = 0;
			}
			hashed = 0;
			(*this).backing = &backing;
		}
		_MapType& get_backing(){
			return *((*this).backing);
		}

		const _MapType& get_backing() const {
			return *((*this).backing);
		}
		inline size_t size() const {
			if (has_backing()) return hashed+get_backing().size();
			return hashed ;
		};
		bool find(data_type& data, const key_type & k) const {
			nst::u32 pos = calc_pos(k);

			if(flags[pos] == 1){
				const _MapPair& r = (*this).data[pos];
				if(r.first == k){
					data = r.second;
					return true;
				}
			}
			typename _MapType::const_iterator f = get_backing().find(k);
			if(f!=get_backing().end()){
				data = (*f).second;
				return true;
			}
			return false;
		}
		data_type& operator[](const key_type & k){
			nst::u32 pos = calc_pos(k);
			_MapPair& r = data[pos];
			if(flags[pos] == 0){
				flags[pos] = 1;
				r.first = k;
				++hashed;
				return r.second;
			}
			if(r.first == k)
				return r.second;
			if((*this).backing != nullptr)
				return get_backing()[k];
			return r.second;
		}
	};
};
///
/// provides a block size mapped suffix array entropy encoded
/// (compressed) buffer for random access queries on compressed
/// data
/// i.e. const char * get(address,l) where l is < the initial block_size
/// the suffix size is currently set to 8
class suffix_array_encoder{
public:

private:
	nst::u32 block_size;
	/// dependant key type conversion
	inline suffix_array::_SuffixArray::key_type get_key(suffix_array::_SuffixArray::iterator& in){
		return in.key();
	}
	inline suffix_array::_SuffixArray::key_type get_key(suffix_array::_StdSuffixArray::iterator& in){
		return (*in).first;
	}
	/// dependant data/mapped type conversion
	suffix_array::_SuffixArray::mapped_type get_data(suffix_array::_SuffixArray::iterator& in){
		return in.data();
	}
	suffix_array::_SuffixArray::mapped_type get_data(suffix_array::_StdSuffixArray::iterator& in){
		return (*in).second;
	}
private:

	typedef std::pair<nst::u64, suffix_array::suffix> _FreqPair;
	typedef std::vector<_FreqPair> _Inverted;
	typedef std::vector<nst::u8> _Bytes;
	typedef std::vector<nst::u64> _Frequencies;
	typedef std::pair<nst::u64, nst::u8> _CharFrequency;
	typedef std::vector<_CharFrequency> _CharFrequencies;
	typedef nst::u32 _CodeType;
	typedef symbol_vector<_CodeType> _Encoded;
	typedef std::vector<nst::u64> _Codes;
	typedef std::vector<suffix_array::suffix> _Suffixes;

public:
	suffix_array_encoder(nst::u32 block_size = 8192) : block_size(block_size){
	}
	~suffix_array_encoder(){
	}

	nst::u32 find_good_bucket_size(const char * buffer, nst::u64 length){
		using namespace suffix_array;
		typedef _StdSuffixArray _SuffixArrayType ;
		typedef fe_hash_map<_SuffixArrayType> _HashFe;

		_HashFe suffix_hasher;
		_SuffixArrayType reduced_array;


		suffix_hasher.set_backing(reduced_array);

		nst::u32 bucket_size = suffix_size;

		nst::u32 min_bucket_size = 0;
		nst::u64 min_compressed = length;
		while(bucket_size > 1){
			nst::u64 p = 0;
			nst::u64 remaining = length;
			nst::u64 buckets = 0;
			for(; p < length/16 ; ){
				suffix s;
				nst::u64 skip = 1;
				nst::u64 length = std::min<nst::u64>(remaining, bucket_size);
				s.assign(&buffer[p], (nst::u32)length);
				suffix_hasher[s]++;
				skip = length;
				remaining -= skip;
				p += skip;
				++buckets;
			}
			suffix_hasher.flush();
			nst::u32 code_size = std::max<_CodeType>(1, bits::bit_log2((_CodeType)reduced_array.size()));
			printf("array size %zd\n",reduced_array.size());
			nst::u64 compressed = ( (buckets * code_size)>>3) + reduced_array.size()*suffix_size;
			if(compressed < min_compressed ){
				min_bucket_size = bucket_size;
				min_compressed = compressed;
				printf("min compressed %lu, min bucket size %lu\n",compressed,(nst::u64)bucket_size);
			}
			reduced_array.clear();
			bucket_size -= 1;
		}

		return min_bucket_size;
	}
	struct coding_parameters{
		nst::u32 max_history;
		nst::u32 bucket_size;
		nst::u32 code_size;
		nst::u64 length;
		_Encoded encoded;
		_Encoded mapped;
		_Encoded short_codes;
		_Suffixes suffixes;

		nst::u64 capacity() const {
			return sizeof(*this) + encoded.capacity() + mapped.capacity() + suffixes.capacity() + short_codes.capacity();
		}

		bool validate() const {
			if(bucket_size < 1 || bucket_size > 16){
				return false;
			}
			if(code_size==0 || code_size > 30){
				return false;
			}
			if(encoded.empty()){
				return false;
			}
			if(suffixes.empty()){
				return false;
			}
			return true;
		}
	};
	template<typename _VectorType>
	void decode(_VectorType &outbuffer, nst::u64 start, nst::u64 length,const coding_parameters& param){

		if(!param.validate()){
			printf("invalid coding parameters\n");
			return;
		}
		nst::u64 current = start;
		nst::u64 remaining = length;
		while(remaining > 0){
			nst::u64 copy_start = (current % param.bucket_size);
			nst::u64 copied = std::min<nst::u64>(remaining, param.bucket_size - copy_start);
			nst::u64 bucket = current / param.bucket_size;
			nst::u64 code = param.encoded.get(bucket);
			if(code >= param.max_history){
				suffix_array::suffix s = param.suffixes[code];
				memcpy(&outbuffer[current-start], &s.data[copy_start],copied);
			}/// else process history
			remaining -= copied;
			current += copied;

		}
	}
	/// encodes and create internal map
	template<typename _SuffixArrayType>
	void encode_internal(_SuffixArrayType& reduced_array, const char * buffer, nst::u64 length){
		using namespace suffix_array;

		{

			typedef fe_hash_map<_SuffixArrayType> _HashFe;

			coding_parameters parameters;
			_HashFe suffix_hasher;
			nst::u64 remaining = length;
			/// nst::u64 factor = remaining/100;

			suffix_hasher.set_backing(reduced_array);
			parameters.length = length;
			nst::u64 buckets = 0;
			parameters.bucket_size = find_good_bucket_size(buffer, length);
			for(nst::u64 p = 0; p < length ; ){
				suffix s;
				nst::u64 skip = 1;
				nst::u64 length = std::min<nst::u64>(remaining, parameters.bucket_size);
				s.assign(&buffer[p], (nst::u32)length);
				suffix_hasher[s]++;
				skip = length;
				remaining -= skip;
				p += skip;
				++buckets;
			}

			suffix_hasher.flush();


			const size_t MAX_SHORT_CODES = 1024;
			_Codes history(MAX_SHORT_CODES);
			_Inverted frequencies ;//= succinct;
			for(typename _SuffixArrayType::iterator s = reduced_array.begin(); s != reduced_array.end(); ++s){
				frequencies.push_back(std::make_pair(get_data(s),get_key(s)));
			}
			std::sort(frequencies.begin(),frequencies.end());
			nst::u64 code = frequencies.size() ;
			reduced_array.clear();
			suffix_hasher.set_backing(reduced_array);
			parameters.suffixes.resize(code+1);
			parameters.max_history = MAX_SHORT_CODES;
			for(_Inverted::iterator i = frequencies.begin(); i != frequencies.end(); ++i){
				suffix_hasher[(*i).second] = code;
				parameters.suffixes[code] = (*i).second;
				--code;
			}
			suffix_hasher.flush();
			printf("there are %lu codes\n",(nst::u64)frequencies.size());

			/// encoding phase


			remaining = length;
			/// TODO: if code size >
			parameters.code_size = std::max<_CodeType>(1, bits::bit_log2((_CodeType)frequencies.size()));
			nst::u64 coded = 0;
			nst::u64 noncoded = 0;
			nst::u64 out_pos = 0;
			nst::u64 short_out_pos = 0;
			nst::u64 history_pos = 0;

			parameters.encoded.set_code_size(parameters.code_size);
			parameters.encoded.resize(length);

			parameters.mapped.set_code_size(1);
			parameters.mapped.resize(buckets);

			parameters.short_codes.set_code_size(bits::bit_log2(MAX_SHORT_CODES));
			parameters.short_codes.resize(buckets);

			typename _SuffixArrayType::iterator closest;
			typename _SuffixArrayType::iterator ends = reduced_array.end();
			parameters.encoded.resize(1+buckets);
			nst::u64 shrtened = 0;
			for(nst::u64 p = 0; p < length ; ){
				suffix s;
				nst::u64 skip = std::min<nst::u64>(remaining, parameters.bucket_size);
				s.assign(&buffer[p], (nst::u32)skip);

				nst::u64 code = 0;

				if(suffix_hasher.find(code, s)){
					++coded;
					nst::u64 short_code = MAX_SHORT_CODES;

					if(history[code % history.size()]==code){
						short_code = code % history.size();
					}

					if(!parameters.mapped.empty() && short_code < MAX_SHORT_CODES){ ///
						shrtened++;
						if(!parameters.mapped.empty())
							parameters.mapped.set(coded-1,1);
						parameters.short_codes.set(short_out_pos++,(nst::u32)short_code);
					}else{
						if(!parameters.mapped.empty())
							parameters.mapped.set(coded-1,0);
						history[code % history.size()] = code;
						++history_pos;
						parameters.encoded.set(out_pos++, (nst::u32)code);
					}
				}else
					++noncoded;
				remaining -=skip;
				p += skip;
			}
			parameters.encoded.trim(out_pos);
			parameters.short_codes.trim(short_out_pos);
			if(false){
				std::vector<nst::u8> decoded(length);
				decode(decoded,0,length,parameters);
				if(memcmp(&decoded[0],buffer,length)!=0){
					nst::u64 errc = 0;
					for(nst::u64 a = 0; a < length;++a){
						if(decoded[a]!=buffer[a]){
							++errc;
						}
					}
					printf("decode errors: %lu\n",errc);
				}
			}
			printf("code size %iu %lu codes written to %lu bytes\n",parameters.code_size,out_pos,parameters.capacity());
		}

	}
	void encode(const char * buffer, nst::u64 length){
		bool use_std = true;
		using namespace suffix_array;


		if(use_std){
			typedef _StdSuffixArray _SuffixArrayType;
			_SuffixArrayType reduced_array;
			size_t t = ::os::millis();
			nst::buffer_type dest(length);
			nst::u64 dsize = FSE_compress(dest.data(),(const unsigned char *)buffer, length);
			printf("Compressed %lu bytes to %lu in %lu ms.\n",length, dsize, ::os::millis()-t);
			/// encode_internal(reduced_array, buffer, length);
		}else{
			typedef _SuffixArray _SuffixArrayType;
			std::string storage_name = "suffix_temp.dat";
			_SuffixStorage storage(storage_name); /// a file to place the storage in
			_SuffixArrayType reduced_array(storage);
			encode_internal(reduced_array, buffer, length);
		}


	}
};
