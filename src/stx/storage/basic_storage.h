#ifndef _STX_BSTORAGE_H_
#define _STX_BSTORAGE_H_
#ifdef _MSC_VER
#pragma warning(disable : 4503)
#endif
#include <stx/storage/types.h>
#include <stx/storage/leb128.h>
#include <Poco/Mutex.h>
#include <Poco/Timestamp.h>
#include <lz4.h>
#include <stdio.h>
/// memuse variables
extern long long treestore_max_mem_use ;
extern long long  _reported_memory_size();
extern long long calc_total_use();

extern "C"{
	#include "zlib.h"
};
#include <fse/fse.h>
#include <fse/zlibh.h>
#include "system_timers.h"

namespace stx{

	namespace storage{

		typedef Poco::ScopedLockWithUnlock<Poco::Mutex> synchronized;
		/// ZLIBH
		static void inplace_compress_zlibh(buffer_type& buff){
			typedef char * encode_type_ref;
			buffer_type t;
			i32 origin = buff.size();
			t.resize(ZLIBH_compressBound(origin)+sizeof(i32));
			i32 cp = buff.empty() ? 0 : ZLIBH_compress((encode_type_ref)&t[sizeof(i32)], (const encode_type_ref)&buff[0], origin);			
			*((i32*)&t[0]) = origin;
			t.resize(cp+sizeof(i32));
			buff = t;
			
		}
		
		static void decompress_zlibh(buffer_type &decoded,const buffer_type& buff){
			typedef char * encode_type_ref;			
			i32 d = *((i32*)&buff[0]) ;
			decoded.reserve(d);
			decoded.resize(d);
			ZLIBH_decompress((encode_type_ref)&decoded[0],(const encode_type_ref)&buff[sizeof(i32)]);
		}
		
		static void inplace_decompress_zlibh(buffer_type& buff, buffer_type& dt){
			if(buff.empty()) return;			
			decompress_zlibh(dt, buff);
			buff = dt;
		}
		
		static void inplace_decompress_zlibh(buffer_type& buff){
			if(buff.empty()) return;
			buffer_type dt;
			decompress_zlibh(dt, buff);
			buff = dt;
		}
		/// FSE
		static void inplace_compress_fse(buffer_type& buff){
			typedef unsigned char * encode_type_ref;
			buffer_type t;
			i32 origin = buff.size();
			t.resize(FSE_compressBound(origin)+sizeof(i32));
			i32 cp = buff.empty() ? 0 : FSE_compress((encode_type_ref)&t[sizeof(i32)], (const encode_type_ref)&buff[0], origin);			
			*((i32*)&t[0]) = (i32)origin;
			t.resize(cp+sizeof(i32));
			buff = t;
			
		}
		
		static void decompress_fse(buffer_type &decoded,const buffer_type& buff){
			typedef unsigned char * encode_type_ref;			
			i32 d = *((i32*)&buff[0]) ;
			decoded.reserve(d);
			decoded.resize(d);
			FSE_decompress((encode_type_ref)&decoded[0],d,(const encode_type_ref)&buff[sizeof(i32)]);
		}
		
		static void inplace_decompress_fse(buffer_type& buff, buffer_type& dt){
			if(buff.empty()) return;			
			decompress_fse(dt, buff);
			buff = dt;
		}
		
		static void inplace_decompress_fse(buffer_type& buff){
			if(buff.empty()) return;
			buffer_type dt;
			decompress_fse(dt, buff);
			buff = dt;
		}
		/// LZ4
		static void inplace_compress_lz4(buffer_type& buff){
			typedef char * encode_type_ref;
			buffer_type t;
			i32 origin = buff.size();
			/// TODO: cannot compress sizes lt 200 mb
			t.resize(LZ4_compressBound((int)buff.size())+sizeof(i32));
			i32 cp = buff.empty() ? 0 : LZ4_compress((const encode_type_ref)&buff[0], (encode_type_ref)&t[sizeof(i32)], origin);			
			*((i32*)&t[0]) = origin;			
			t.resize(cp+sizeof(i32));

			inplace_compress_zlibh(t);

			buff = t;			
			
		}
		static void decompress_lz4(buffer_type &decoded,const buffer_type& input){			
			typedef char * encode_type_ref;
			buffer_type buff ;
			decompress_zlibh(buff, input);
			i32 d = *((i32*)&buff[0]) ;
			decoded.reserve(d);
			decoded.resize(d);
			LZ4_decompress_fast((const encode_type_ref)&buff[sizeof(i32)],(encode_type_ref)&decoded[0],d);

		}
		static void inplace_decompress_lz4(buffer_type& buff){
			if(buff.empty()) return;
			buffer_type dt;
			decompress_lz4(dt, buff);
			buff = dt;
		}
		static void inplace_decompress_lz4(buffer_type& buff, buffer_type& dt){
			if(buff.empty()) return;			
			decompress_lz4(dt, buff);
			buff = dt;
		}
		static void inplace_compress_zlib(buffer_type& buff){
			static const int COMPRESSION_LEVEL = 1;
			static const int Z_MIN_MEM = 1024;
			buffer_type t;
			uLongf actual = (uLongf) buff.size();
			t.resize(actual+Z_MIN_MEM+sizeof(uLongf));
			uLongf compressed = actual+Z_MIN_MEM+sizeof(uLongf);
			*((uLongf*)&t[0]) = actual;
			int zErr = compress2((Bytef*)&t[sizeof(actual)],&compressed,&buff[0],actual,COMPRESSION_LEVEL);
			if(zErr != Z_OK){
				printf("ZLIB write Error\n");
			}
			t.resize(compressed+sizeof(actual));
			t.shrink_to_fit();
			buff = t;
		}
		static void decompress_zlib(buffer_type &decoded,const buffer_type& encoded){
			if(encoded.empty()) return;
			uLongf eos = (uLongf)encoded.size();
			uLongf actual = *((uLongf*)&encoded[0]);
			uLongf compressed = eos - sizeof(actual);
			decoded.resize(actual+1024);
			if(Z_OK != uncompress((Bytef*)&decoded[0], (uLongf*)&actual, (Bytef*)&encoded[sizeof(actual)], compressed)){
				printf("ZLIB read error eos %ld, actual %ld, compressed %ld\n",eos,actual,compressed);
				return;
			}
			decoded.resize(actual);
			buffer_type temp = decoded;
			decoded.swap(temp);

		}
		static void inplace_decompress_zlib(buffer_type &decoded){
			if(decoded.empty()) return;
			buffer_type t(decoded);
			decompress_zlib(decoded,t);
		}
		/// allocation type read only ,write or
		enum storage_action{
			read = 0,
			write,
			create
		};

		/// basic storage functions based on vectors of 1 byte unsigned
		class basic_storage{
		protected:
			typedef u8 value_type;

		public:
			/// reading functions for basic types

			buffer_type::iterator write(buffer_type::iterator out, u8 some){
				return stx::storage::leb128::write_unsigned(out, some);
			}

			buffer_type::iterator write(buffer_type::iterator out, u16 some){
				return stx::storage::leb128::write_unsigned(out, some);
			}

			buffer_type::iterator write(buffer_type::iterator out, u32 some){
				return stx::storage::leb128::write_unsigned(out, some);
			}

			buffer_type::iterator write(buffer_type::iterator out, i8 some){
				return stx::storage::leb128::write_signed(out, some);
			}

			/// write the buffer to the out iterator
			buffer_type::iterator write(buffer_type::iterator out, buffer_type::iterator limit, const u8 *some, u32 l){
				const u8* e = some+l;
				for(;some != e && out !=limit ;++some){
					*out++ = *some;
					++some;
				}
				return out;
			}

			/// reading functions for basic types
			u32 read_unsigned(buffer_type::iterator &inout){
				return stx::storage::leb128::read_unsigned(inout);
			}

			i32 read_signed(buffer_type::iterator &inout){
				return stx::storage::leb128::read_signed(inout);
			}

			size_t read(u8 *some, buffer_type::iterator in, buffer_type::iterator limit){
				u8* v = some;
				for(; in != limit; ++in){
					*v++ = *in;
				}
				return v-some;
			}
		};
	};
};
#endif
