/*
 * Copyright (C) 2008 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Functions for interpreting LEB128 (little endian base 128) values
 */

#ifndef _STX_STORAGE_LEB128_
#define _STX_STORAGE_LEB128_

#include <stx/storage/types.h>
namespace stx{
	namespace storage{
		namespace leb128{
		/*
		 * Reads an unsigned LEB128 value, updating the given pointer to point
		 * just past the end of the read value. This function tolerates
		 * non-zero high-order bits in the fifth encoded byte.
		 */
		template<typename _Iterator>
		i32 read_unsigned(_Iterator& input) {
			typedef i32 _IntegerType ;
			_Iterator ptr = input;
			_IntegerType result = *(ptr++);

			if (result > 0x7f) {
				_IntegerType cur = *(ptr++);
				result = (result & 0x7f) | ((cur & 0x7f) << 7);
				if (cur > 0x7f) {
					cur = *(ptr++);
					result |= (cur & 0x7f) << 14;
					if (cur > 0x7f) {
						cur = *(ptr++);
						result |= (cur & 0x7f) << 21;
						if (cur > 0x7f) {
							/*
							 * Note: We don't check to see if cur is out of
							 * range here, meaning we tolerate garbage in the
							 * high four-order bits.
							 */
							cur = *(ptr++);
							result |= cur << 28;
						}
					}
				}
			}

			input = ptr;
			return result;
		}

		/*
		 * Reads a signed LEB128 value, updating the given pointer to point
		 * just past the end of the read value. This function tolerates
		 * non-zero high-order bits in the fifth encoded byte.
		 */
		template<typename _Iterator >
		i32 read_signed(_Iterator& input) {
			_Iterator ptr = input;
			i32 result = *ptr++;

			if (result <= 0x7f) {
				result = (result << 25) >> 25;
			} else {
				i32 cur = *ptr++;
				result = (result & 0x7f) | ((cur & 0x7f) << 7);
				if (cur <= 0x7f) {
					result = (result << 18) >> 18;
				} else {
					cur = *ptr++;
					result |= (cur & 0x7f) << 14;
					if (cur <= 0x7f) {
						result = (result << 11) >> 11;
					} else {
						cur = *ptr++;
						result |= (cur & 0x7f) << 21;
						if (cur <= 0x7f) {
							result = (result << 4) >> 4;
						} else {
							/*
							 * Note: We don't check to see if cur is out of
							 * range here, meaning we tolerate garbage in the
							 * high four-order bits.
							 */
							cur = *ptr++;
							result |= cur << 28;
						}
					}
				}
			}

			input = ptr;
			return result;
		}
		inline i32 bit_size(u32 v){
			// find the log base 2 of 32-bit v

			static const int MultiplyDeBruijnBitPosition[32] =
			{
			  0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
			  8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
			};

			v |= v >> 1; // first round down to one less than a power of 2
			v |= v >> 2;
			v |= v >> 4;
			v |= v >> 8;
			v |= v >> 16;

			return MultiplyDeBruijnBitPosition[(u32)(v * 0x07C4ACDDU) >> 27];
		}
        /*
		 * Writes a 32-bit value in unsigned ULEB128 format.
		 *
		 * Returns the updated pointer.
		 */
		template<typename _Iterator >
		_Iterator write_unsigned
		(   _Iterator ptr
        ,   u64 data
        )
		{
			while (true) {
				u8 out = data & 0x7f;
				if (out != data) {
					*ptr++ = out | 0x80;
					data >>= 7;
				} else {
					*ptr++ = out;
					break;
				}
			}

			return ptr;
		}
		template<typename _Iterator >
		_Iterator write_signed
		(   const _Iterator &input
        ,   i64 value
        ){
			_Iterator ptr = input;
			u8 byte;
			for(;;) {
				byte = value & 0x7f;/// low order 7 bits of value;
				value >>= 7;
				u8 sbit = (byte & 0x40);
				if ((value == 0 && sbit==0) || (value == -1 && sbit!=0)){
					*ptr++ = byte ;
					break;
				}else byte |= 0x80;
				*ptr++ = byte ;
			}
			return ptr;
		}


		inline i32 signed_size
		(   i64 value
        ) {
			u8 byte;
			i32 size = 0;
			for(;;) {
				byte = value & 0x7f;/// low order 7 bits of value;
				value >>= 7;
				u8 sbit = (byte & 0x40);
				if ((value == 0 && sbit==0) || (value == -1 && sbit!=0)){
					++size;
					break;
				}else byte |= 0x80;
				++size;
			}
			return size;
		}

		/*
		 * Returns the number of bytes needed to encode "val" in ULEB128 form.
		 */
		inline i32 unsigned_size(u32 data)
		{
			i32 count = 0;

			do {
				data >>= 7;
				count++;
			} while (data != 0);

			return count;
		}
		/*
		 * Reads an unsigned LEB128 value, updating the given pointer to point
		 * just past the end of the read value and also indicating whether the
		 * value was syntactically valid. The only syntactically *invalid*
		 * values are ones that are five bytes long where the final byte has
		 * any but the low-order four bits set. Additionally, if the limit is
		 * passed as non-NULL and bytes would need to be read past the limit,
		 * then the read is considered invalid.
		 */
		template<typename _Iterator >
		i32 read_verify_unsigned
		(   _Iterator& pStream
        ,   _Iterator limit
        ,   bool* okay
        ) {
			_Iterator ptr = pStream;
			i32 result = read_unsigned(pStream);

			if (((pStream >= limit))
					|| (((pStream - ptr) == 5) && (ptr[4] > 0x0f))) {
				*okay = false;
			}

			return result;
		}

		/*
		 * Reads a signed LEB128 value, updating the given pointer to point
		 * just past the end of the read value and also indicating whether the
		 * value was syntactically valid. The only syntactically *invalid*
		 * values are ones that are five bytes long where the final byte has
		 * any but the low-order four bits set. Additionally, if the limit is
		 * passed as non-NULL and bytes would need to be read past the limit,
		 * then the read is considered invalid.
		 */
		template<typename _Iterator >
		i32 read_verify_signed
		(   _Iterator& io
        ,   _Iterator limit
        ,   bool* okay
        ) {
			_Iterator ptr = io;
			i32 result = read_signed(io);

			if (((io >= limit))
					|| (((io - ptr) == 5) && (ptr[4] > 0x0f))) {
				*okay = false;
			}

			return result;
		}


		/// Decode the unsigned 64-bit LEB128 data and resturn it leaving the start changed

		template<typename _Iterator>
		u64 read_unsigned64 (_Iterator& start, _Iterator end){
			typedef u64 _Unsigned;
			_Iterator p = start;
			unsigned int shift = 0;
			_Unsigned result = 0;
			unsigned char byte;

			while (1){
				if (p >= end)
					return 0;

				byte = *p++;
				result |= ((_Unsigned) (byte & 0x7f)) << shift;
				if ((byte & 0x80) == 0)
					break;
				shift += 7;
			}
			start = p;
			return result;
		}

		/// Decode the signed 64-bit LEB128 data and resturn it leaving the start changed


		template<typename _Iterator >
		i64 read_signed64(_Iterator& start, _Iterator end){
			typedef i64 _Signed;
			typedef u64 _Unsigned;
			_Iterator p = start;
			unsigned int shift = 0;
			_Signed result = 0;
			unsigned char byte;

			while (1){
				if (p >= end)
					return 0;

				byte = *p++;
				result |= ((_Unsigned) (byte & 0x7f)) << shift;
				shift += 7;
				if ((byte & 0x80) == 0)
					break;
			}
			if (shift < (sizeof (_Signed) * 8) && (byte & 0x40) != 0)
				result |= -(((_Unsigned) 1) << shift);
			start = p;
			return result;

		}


		};///leb128
	}; ///storage
}; ///stx
#endif
///_STX_STORAGE_LEB128_
