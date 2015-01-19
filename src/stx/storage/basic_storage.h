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
#include <stx/storage/pool.h>
/// memuse variables
extern long long treestore_max_mem_use ;
extern long long treestore_current_mem_use ;
extern long long  _reported_memory_size();
extern long long calc_total_use();
/// the allocation pool
extern stx::storage::allocation::pool allocation_pool;
extern "C"{
	#include "zlib.h"
};
#include <fse/fse.h>
#include <fse/zlibh.h>
#include "system_timers.h"

namespace stx{

	namespace storage{
		namespace allocation{
			template <class T>
			class pool_alloc_tracker : public base_tracker<T>{
			public:				
				// rebind allocator to type U
				template <class U>
				struct rebind {
					typedef pool_alloc_tracker<U> other;
				};
				pool_alloc_tracker() throw() {
				}
				pool_alloc_tracker(const pool_alloc_tracker&) throw() {
				}
				template <class U>
				pool_alloc_tracker (const pool_alloc_tracker<U>&) throw() {
				}
				
				// allocate but don't initialize num elements of type T
				pointer allocate (size_type num, const void* = 0) {
					bt_counter c;
					c.add(num*sizeof(T)+overhead());
					return (pointer)allocation_pool.allocate(num);
				}
				// deallocate storage p of deleted elements
				void deallocate (pointer p, size_type num) {
					
					allocation_pool.free((void*)p,num);				
			
					bt_counter c;
					c.remove(num*sizeof(T)+overhead());
				}
			};

			template <class T>
			class buffer_tracker : public base_tracker<T>{
			public:				
				// rebind allocator to type U
				template <class U>
				struct rebind {
					typedef buffer_tracker<U> other;
				};
				buffer_tracker() throw() {
				}
				buffer_tracker(const buffer_tracker&) throw() {
				}
				template <class U>
				buffer_tracker (const buffer_tracker<U>&) throw() {
				}
				
				// allocate but don't initialize num elements of type T
				pointer allocate (size_type num, const void* = 0) {
					buffer_counter c;
					c.add(num*sizeof(T)+overhead());
					return base_tracker::allocate(num);
				}
				// deallocate storage p of deleted elements
				void deallocate (pointer p, size_type num) {
					base_tracker::deallocate(p, num);
					buffer_counter c;
					c.remove(num*sizeof(T)+overhead());
				}
			};

			 // return that all specializations of this allocator are interchangeable
			template <class T1, class T2>
			bool operator== (const buffer_tracker<T1>&,const buffer_tracker<T2>&) throw() {
				return true;
			}
			template <class T1, class T2>
			bool operator!= (const buffer_tracker<T1>&, const buffer_tracker<T2>&) throw() {
				return false;
			}
			template <class T>
			class col_tracker : public base_tracker<T>{
			public:				
				// rebind allocator to type U
				template <class U>
				struct rebind {
					typedef col_tracker<U> other;
				};
				col_tracker() throw() {
				}
				col_tracker(const col_tracker&) throw() {
				}
				template <class U>
				col_tracker (const col_tracker<U>&) throw() {
				}
				
				// allocate but don't initialize num elements of type T
				pointer allocate (size_type num, const void* = 0) {
					col_counter c;
					c.add(num*sizeof(T)+overhead());
					return base_tracker::allocate(num);
				}
				// deallocate storage p of deleted elements
				void deallocate (pointer p, size_type num) {
					base_tracker::deallocate(p, num);
					col_counter c;
					c.remove(num*sizeof(T)+overhead());
				}
			};

			 // return that all specializations of this allocator are interchangeable
			template <class T1, class T2>
			bool operator== (const col_tracker<T1>&,const col_tracker<T2>&) throw() {
				return true;
			}
			template <class T1, class T2>
			bool operator!= (const col_tracker<T1>&, const col_tracker<T2>&) throw() {
				return false;
			}
			template <class T>
			class stl_tracker : public base_tracker<T>{
			public:				
				// rebind allocator to type U
				template <class U>
				struct rebind {
					typedef stl_tracker<U> other;
				};
				stl_tracker() throw() {
				}
				stl_tracker(const stl_tracker&) throw() {
				}
				template <class U>
				stl_tracker (const stl_tracker<U>&) throw() {
				}
				
				// allocate but don't initialize num elements of type T
				pointer allocate (size_type num, const void* = 0) {
					stl_counter c;
					c.add(num*sizeof(T)+overhead());
					return base_tracker::allocate(num);
				}
				// deallocate storage p of deleted elements
				void deallocate (pointer p, size_type num) {
					base_tracker::deallocate(p, num);
					stl_counter c;
					c.remove(num*sizeof(T)+overhead());
				}
			};

			 // return that all specializations of this allocator are interchangeable
			template <class T1, class T2>
			bool operator== (const stl_tracker<T1>&,const stl_tracker<T2>&) throw() {
				return true;
			}
			template <class T1, class T2>
			bool operator!= (const stl_tracker<T1>&, const stl_tracker<T2>&) throw() {
				return false;
			}
		}; /// allocations
		typedef std::vector<u8, sta::buffer_tracker<u8>> buffer_type;
		
		/// ZLIBH
		static void inplace_compress_zlibh(buffer_type& buff){
			typedef char * encode_type_ref;
			buffer_type t;
			i32 origin = (i32) buff.size();
			t.resize(ZLIBH_compressBound(origin)+sizeof(i32));
			i32 cp = buff.empty() ? 0 : ZLIBH_compress((encode_type_ref)&t[sizeof(i32)], (const encode_type_ref)&buff[0], origin);			
			*((i32*)&t[0]) = origin;
			t.resize(cp+sizeof(i32));
			buff = t;
			
		}
		template<typename _VectorType>
		static void decompress_zlibh(buffer_type &decoded,const _VectorType& buff){
			if(buff.empty()){
				decoded.clear();
				
			}else{
				typedef char * encode_type_ref;			
				i32 d = *((i32*)&buff[0]) ;
				decoded.reserve(d);
				decoded.resize(d);
				ZLIBH_decompress((encode_type_ref)&decoded[0],(const encode_type_ref)&buff[sizeof(i32)]);
			}
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
			if(cp < 0){
				cp = (i32)buff.size();
				origin = -cp;
				memcpy(&t[sizeof(i32)], &buff[0], buff.size());
			}
			*((i32*)&t[0]) = (i32)origin;
			t.resize(cp+sizeof(i32));
			buff = t;
			
		}
		template<typename _VectorType>
		static void decompress_fse(buffer_type &decoded,const _VectorType& buff){
			if(buff.empty()){
				decoded.clear();
			}else{
				typedef unsigned char * encode_type_ref;			
				i32 d = *((i32*)&buff[0]) ;
				if(d < 0){
					d = -d;
					decoded.reserve(d);
					decoded.resize(d);
					memcpy(&decoded[0], &buff[sizeof(i32)], d);
				}else{
					decoded.reserve(d);
					decoded.resize(d);
					FSE_decompress((encode_type_ref)&decoded[0],d,(const encode_type_ref)&buff[sizeof(i32)]);
				}
			}
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
		static void inplace_compress_lz4(buffer_type& buff, buffer_type& t){
			typedef char * encode_type_ref;
			
			i32 origin = buff.size();
			/// TODO: cannot compress sizes lt 200 mb
			t.resize(LZ4_compressBound((int)buff.size())+sizeof(i32));
			i32 cp = buff.empty() ? 0 : LZ4_compress((const encode_type_ref)&buff[0], (encode_type_ref)&t[sizeof(i32)], origin);			
			*((i32*)&t[0]) = origin;			
			t.resize(cp+sizeof(i32));

			/// inplace_compress_zlibh(t);
			/// inplace_compress_fse(t);

			buff = t;			
			
		}
		static void inplace_compress_lz4(buffer_type& buff){
			
			buffer_type t;
			inplace_compress_lz4(buff, t);
		}
		static void decompress_lz4(buffer_type &decoded,const buffer_type& buff){			
			if(buff.empty()){
				decoded.clear();
			}else{
				typedef char * encode_type_ref;
				/// buffer_type buff ;
				/// decompress_zlibh(buff, input);
				/// decompress_fse(buff, input);
				i32 d = *((i32*)&buff[0]) ;
				decoded.reserve(d);
				decoded.resize(d);
				LZ4_decompress_fast((const encode_type_ref)&buff[sizeof(i32)],(encode_type_ref)&decoded[0],d);
			}

		}
		static size_t r_decompress_lz4(buffer_type &decoded,const buffer_type& buff){			
			if(buff.empty()){
				decoded.clear();
			}else{
				typedef char * encode_type_ref;
				/// buffer_type buff ;
				/// decompress_zlibh(buff, input);
				/// decompress_fse(buff, input);
				i32 d = *((i32*)&buff[0]) ;
				if(decoded.size() < d){
					decoded.reserve(d);
					decoded.resize(d);
				}
				LZ4_decompress_fast((const encode_type_ref)&buff[sizeof(i32)],(encode_type_ref)&decoded[0],d);
				return d;
			}
			return 0;
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
		/// versions for all

		typedef std::pair<u64, u64> _VersionRequest;

		typedef std::vector<_VersionRequest> _VersionRequests;

	};
};
#endif
