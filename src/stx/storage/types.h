#ifndef _STX_STORAGE_TYPES_H_
#define _STX_STORAGE_TYPES_H_
#include <vector>
#include <Poco/Types.h>
namespace stx{
	namespace storage{
		/// unsigned integer primitive types
			typedef unsigned char u8;
			typedef Poco::UInt16 u16;
			typedef Poco::UInt32 u32;
			typedef Poco::UInt64 u64;
		/// signed integer primitive types
			typedef char i8;
			typedef Poco::Int16 i16;
			typedef Poco::Int32 i32;
			typedef Poco::Int64 i64;
			typedef long long int lld;
		/// virtual allocator address type
			typedef u32 stream_address ;

			typedef std::vector<u8> buffer_type;
			/// the version type
			typedef unsigned int version_type;
	};
};

#endif
///_STX_STORAGE_TYPES_H_
