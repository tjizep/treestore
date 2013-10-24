#ifndef _STX_STORAGE_TYPES_H_
#define _STX_STORAGE_TYPES_H_
#include <vector>
namespace stx{
	namespace storage{
		/// unsigned integer primitive types
			typedef unsigned char u8;
			typedef unsigned short u16;
			typedef unsigned int u32;
			typedef unsigned long long u64;
		/// signed integer primitive types
			typedef char i8;
			typedef short i16;
			typedef int i32;
			typedef long long i64;
		/// virtual allocator address type
			typedef unsigned long stream_address ;

			typedef std::vector<u8> buffer_type;
			/// the version type
			typedef unsigned int version_type;
	};
};

#endif
///_STX_STORAGE_TYPES_H_