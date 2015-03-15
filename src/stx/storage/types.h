#ifndef _STX_STORAGE_TYPES_H_
#define _STX_STORAGE_TYPES_H_

#include <type_traits>

template <typename T, typename NameGetter>
class has_member_impl {
private:
    typedef char matched_return_type;
    typedef long unmatched_return_type;

    template <typename C>
    static matched_return_type f(typename NameGetter::template get<C>*);

    template <typename C>
    static unmatched_return_type f(...);

public:
    static const bool value = (sizeof(f<T>(0)) == sizeof(matched_return_type));
};

template <typename T, typename NameGetter>
struct has_member
    : std::integral_constant<bool, has_member_impl<T, NameGetter>::value>
{};
/**
 * NOTE on treestore: vector is use as a contiguous array. If a version of STL 
 * is encountered that uses a different allocation scheme then a replacement
 * will be provided here.
 */

/*
* Base types and imperfect hash Template Classes v 0.1
*/
/*****************************************************************************

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
			
			/// the version type
			typedef u32 version_type;

	};
};

#endif

///_STX_STORAGE_TYPES_H_
