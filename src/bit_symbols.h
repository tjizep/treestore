/*****************************************************************************

Copyright (c) 2013, Christiaan Pretorius

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
#ifndef _BITSYMBOLS_20140118_CEP_
#define _BITSYMBOLS_20140118_CEP_
#include <stx/storage/basic_storage.h>
namespace nst = stx::storage;

template<typename _IntSymBolType>
class symbol_vector{
public:
	typedef nst::u16 _BucketType;/// use a configurable bucket type for larger code size performance
	typedef nst::u32 _IndexType; /// type to index bits
	static const _BucketType BUCKET_BITS = sizeof(_BucketType)<<3;
	typedef std::vector<_BucketType> _Data;
protected:
	_Data data;
	_BucketType code_size;
public:
	symbol_vector(){
		code_size = 0;
	}

	const bool empty() const {
		return data.empty();
	}

	_BucketType get_code_size() const {
		return code_size;
	}

	/// the code size can be any positive integer <= sizeof(_IntSymBolType)*8
	void set_code_size(_BucketType code_size){
		(*this).code_size = std::min<_BucketType>(sizeof(_IntSymBolType)*8,code_size);
	}
	/// create buckets by the ns
	void resize(_IndexType ns){
		data.resize(((ns*code_size)/BUCKET_BITS)+1);
	}

	/// the capacity in bytes of this bit symbol vector
	size_t capacity() const {
		return data.capacity()*sizeof(_BucketType);
	}

	/// remove data completely - requires resize to enable again
	void clear(){
		data.swap(_Data());
	}
	
	/// write code_size bits at bit-index index
	void set(_IndexType index, typename const _IntSymBolType &val){
		_BucketType bucket_start;
		_IntSymBolType code = val;
		_IndexType bits_done = index * code_size;
		_IndexType code_left = code_size;
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
	}
		
	/// read code_size bits at bit-index index
	typename _IntSymBolType get(_IndexType index) const {
		_IntSymBolType code = 0;
		_BucketType bucket_start, bucket;
		_IndexType bit_start = index * code_size;				
		const _BucketType* current = &data[bit_start / BUCKET_BITS];			
		_IndexType code_left = code_size;
		_IndexType code_complete = 0;
		for(;;){	/// read from BUCKET_BITS-bit buckets
			bucket_start = bit_start & (BUCKET_BITS-1);/// where to begin in the bucket
			_BucketType todo = std::min<_BucketType>(code_size-code_complete, BUCKET_BITS-bucket_start);
			bucket = (*current >> bucket_start)& ( ( 1 << todo ) - 1);
												
			code |=  bucket << code_complete;		/// if bucket_start == 0 nothing happens				
						
			bit_start += todo;						
			code_complete += todo;
			
			if(code_complete == code_size )
				break;

			if( ( bit_start & (BUCKET_BITS-1) ) == 0){
				++current; /// increment the bucket
			}
		}				
		return code;
	}

	_IntSymBolType operator[](_IndexType index) const {
		return get(index);
	}
};

#endif