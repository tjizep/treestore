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

		/// fastest hashtable evah
		template <typename _K, typename _V>
		class imperfect_hash{
		public:
		typedef std::pair<_K,_V> _ElPair;
		struct iterator{
			const imperfect_hash* h;
			size_t pos;
	
			iterator(){
		
			}
			iterator(const imperfect_hash* h, size_t pos): h(h),pos(pos){
		
			}
			iterator(const iterator& r){
				(*this) = r;
			}
			iterator& operator=(const iterator& r){
				h = r.h;
				pos = r.pos;
				return (*this);
			}
			iterator& operator++(){
				++pos;
				/// todo optimize with 32-bit or 64-bit zero counting
				while(pos < h->get_data_size() && !h->exists[pos]){
					++pos;
				}
		
				return (*this);
			}
			iterator operator++(int){
				iterator r = *this;
				++(*this);
				return r;
			}
			_ElPair& operator*(){
				return const_cast<imperfect_hash*>(h)->data[pos];
			}
			const _ElPair& operator*() const {
				return h->data[pos];
			}
			bool operator==(const iterator& r) const {
				return h==r.h&&pos == r.pos;
			}
			bool operator!=(const iterator& r) const {
				return (h!=r.h)||(pos != r.pos);
			}
		};
		protected:
			static const size_t MIN_EXTENT = 511;
			typedef std::vector<u8> _Exists;
	
			typedef std::vector<_ElPair> _Data;
			_Exists exists;
			_Data data;
			_ElPair * _data;
			u8 * _exists;
			size_t extent;

			size_t elements;
			size_t key2pos(const _K& k) const {
				return ((size_t) k ) % extent;
			}
			size_t get_data_size() const {
				return extent+extent/1024;
			}
			void resize_clear(size_t new_extent){
				extent = new_extent;
				elements = 0;
				exists.clear();
				data.clear();
				exists.resize(get_data_size());
				data.resize(get_data_size());
				_data = &data[0];
				_exists = &exists[0];
			};
			void rehash(){
				/// can cause oom e
				_Data temp;
				size_t new_extent = extent * 3;
				///printf("rehash %ld\n",(long int)new_extent);
				for(iterator i = begin();i != end();++i){
					temp.push_back((*i));
				}
				///printf("starting rehash %ld\n",(long int)temp.size());
				resize_clear(new_extent);
				for(typename _Data::iterator d = temp.begin(); d != temp.end();++d){
					(*this)[(*d).first] = (*d).second;
				}
				///printf("complete rehash %ld\n",(long int)elements);
		
			}
		public:
	
			void clear(){
				resize_clear(MIN_EXTENT);
			}
	
			imperfect_hash(){
				clear();
			}
			~imperfect_hash(){
		
			}
			imperfect_hash(const imperfect_hash& right){
				*this = right;
			}
			imperfect_hash& operator=(const imperfect_hash& right){
				exists = right.exists;
				data = right.data;
				elements = right.elements;
				extent = right.extent;
				_data = &data[0];
				_exists = &exists[0];

				return *this;
			}
			void insert(const _K& k,const _V& v){
				(*this)[k] = v;
			}
			_V& operator[](const _K& k){
				size_t pos = 0;
				while(true){
				/// eventualy an out of memory exception will occur
					pos = key2pos(k);
					if(_exists[pos] && _data[pos].first == k){
						return _data[pos].second;
					}
					if(!_exists[pos]){
						_exists[pos] = true;
						_data[pos].first = k;
						++elements;
						return _data[pos].second;
					}
					for(pos = extent; pos < get_data_size();++pos){
						if(!_exists[pos]){
							break;
						}
						if(_data[pos].first == k){
							return _data[pos].second;
						}
					};
					if(pos < get_data_size()){
						_exists[pos] = true;
						_data[pos].first = k;
						++elements;
						return _data[pos].second;
					}
					rehash();
				}
				return data[pos].second;
			}
			bool erase(const _K& k){
				size_t pos = key2pos(k);
				if(_exists[pos] && _data[pos].first == k){
					_exists[pos] = false;
					--elements;
					return true;
				}
				for(pos = extent; pos < get_data_size();++pos){
					if(!_exists[pos]){
						break;
					}
					if(_data[pos].first == k){
						size_t j = pos;
						for(; j < get_data_size()-1;++j){
							if(!_exists[j]){
								break;
							}
							_exists[j] = _exists[j+1];
							_data[j] = _data[j+1];
							
						}
						_exists[j] = false;
						--elements;						
						return true;
					}
				};
				return false;
			}
			size_t count(const _K& k) const {
				size_t pos = key2pos(k);
				if(_exists[pos] && _data[pos].first == k) return 1;
				for(pos = extent; pos < get_data_size();++pos){
					if(!_exists[pos]){
						break;
					}
					if(_data[pos].first == k){
						return 1;
					}
				};
				return 0;
			}
			bool get(const _K& k,_V& v) const {
				size_t pos = key2pos(k);
				if(_exists[pos] && _data[pos].first == k) {
					v = _data[pos].second;
					return true;
				}
				for(pos = extent; pos < get_data_size();++pos){
					if(!_exists[pos]){
						break;
					}
					if(_data[pos].first == k){
						v = _data[pos].second;
						return true;
					}
				}
				return false;
			}
			iterator find(const _K& k) const {
				size_t pos = key2pos(k);
				if(_exists[pos] && _data[pos].first == k) return iterator(this,pos);
				for(pos = extent; pos < get_data_size();++pos){
					if(!_exists[pos]){
						break;
					}
					if(_data[pos].first == k){
						return iterator(this,pos);
					}
				}
				return end();
			}
			iterator begin() const {
				if(elements==0)
					return end();
				size_t start = 0;
				/// todo optimize with min pos member
				while(start < get_data_size()){
					if(exists[start])
						break;
					++start;
				}
				return iterator(this,start);
			}
			iterator end() const {
				return iterator(this,get_data_size());
			}
			size_t size() const {
				return elements;
			}
		};
	};
};

#endif

///_STX_STORAGE_TYPES_H_
