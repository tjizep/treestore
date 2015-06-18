#ifndef _ITERATOR_H_CEP_20140303_
#define _ITERATOR_H_CEP_20140303_
#include "fields.h"
namespace iterator{
	typedef stored::_Rid _Rid;

	class AbstractedIterator{
	public:
		AbstractedIterator(){
		}
		virtual ~AbstractedIterator(){
		}
		virtual bool valid(){
			return true;
		};
		virtual bool invalid(){
			return true;
		};
		virtual void next(){
		}
	};

	template <typename _MapType>
	class ImplIterator {
	private:
		typename _MapType::iterator i;
		typename _MapType::iterator iend;
		
		_MapType * map;

		_MapType &get_map(){
			return *map;
		}
	public:
		void check() const {
#ifdef _DEBUG
			if(map != i.context()){
				printf("context mismatch()\n");
			}
			if(map != iend.context()){
				printf("context mismatch()\n");
			}
#endif
		}

		typedef _MapType map_type;
		typedef typename _MapType::iterator::initializer_pair initializer_pair;
		typedef typename _MapType::key_type key_type;
		typedef typename _MapType::data_type data_type;
		typedef typename _MapType::iterator iterator_type;

		ImplIterator()
		:	map(NULL)
		{
		}

		ImplIterator(_MapType& map, typename _MapType::iterator i){
			(*this).map = &map;
			(*this).i = i;
			iend = get_map().end();
			check();
		}

		ImplIterator(const ImplIterator& right){
			*this = right;
			check();
		}

		ImplIterator& operator=(const ImplIterator& right){
			map = right.map;
			iend.clear();
			i.clear();
			iend = right.iend;
			i = right.i;
			check();
			return *this;
		}

		virtual ~ImplIterator(){
		}
		iterator_type& get_i(){
			return i;
		}
		void set_end(const ImplIterator& the_end){
			iend.clear();
			iend = the_end.i;
			check();
		}
		void seek(const typename _MapType::iterator& the_i){
			i = the_i;
		}
		void set(const typename _MapType::iterator& the_i,const typename _MapType::iterator& the_end, _MapType& amap){
			i.clear();
			iend.clear();
			i = the_i;
			iend = the_end;
			(*this).map = &amap;
			check();
		}

		void lower(const key_type &k){			
			i = get_map().lower_bound(k);
			iend = get_map().end();
			check();
		}
		void find(const key_type &k){
			
			check();
			i = get_map().find(k);
			iend = get_map().end();
			check();
		}
		void first(){
			
			check();
			i = get_map().begin();
			iend = get_map().end();
			check();
		}
		void last(){
			
			check();
			
			iend = get_map().end();
			i = iend;
			if(i != get_map().begin())
				--i;
			check();
		}

		_Rid count(const ImplIterator &last){
			_Rid r = (_Rid)i.count(last.i);
			check();
			return r;
		}

		const key_type& get_key() const{
			return i.key();
		}

		key_type& get_key() {
			return i.key();
		}


		data_type& get_value(){
			return i.data();
		}

		const data_type& get_value() const{
			return i.data();
		}
		
		
		bool valid() const {
			if(map == NULL) return false;
			check();
			return (i!=iend);
		};

		bool invalid() const {
			if(map == NULL) return true;
			check();
			return (i==iend);
		};

		initializer_pair get_initializer(){
			check();
			return i.construction();
		}

		void from_initializer(_MapType& amap, const initializer_pair& init){

			if(map != &amap){
				map = &amap;
				iend.clear();
				iend = get_map().end();
			}
			get_map().restore_iterator(i,init);
			check();
		}

		void next(){
			
			check();
			++i;
		}
		bool previous(){
			if(map == NULL) return false;
			if(i != get_map().begin())	{
				--i;
				return true;
			}
			return false;
		}
	};

};
#endif