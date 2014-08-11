/*****************************************************************************

Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
Copyright (c) 2008, 2009 Google Inc.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2012, Facebook Inc.
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
#ifndef _PREDICTIVECACHE_H_CEP2013_
#define _PREDICTIVECACHE_H_CEP2013_

#include "tree_stored.h"

extern NS_STORAGE::u64 hash_hits;
extern NS_STORAGE::u64 hash_predictions;
extern NS_STORAGE::u64 total_cache_size;
namespace tree_stored{
	template<typename _Keytype>
	struct cached_row{
		cached_row(){
		}

		_Keytype k;

	};

	class eraser_interface{
	public:
		virtual void erase(const CompositeStored& input)=0;
	};


	/// The predictive cache acts like an adaptive 'derandomizer'

	template<typename BasicIterator>
	struct predictive_cache : public eraser_interface{
	private:
		static const NS_STORAGE::u64 CIRC_SIZE = 10000000;/// about 128 MB shared - the cachedrow is 32 bytes

		static const stored::_Rid STORE_INF = (stored::_Rid)-1;
		typedef cached_row<typename BasicIterator::key_type> CachedRow;
		typedef std::vector<CompositeStored> _Erased;

	private:
		void _erase(const CompositeStored& input){
			unsigned int h = ((size_t)input) % hash_size;
			unsigned int predictor = cache_index[h];

			if(predictor && sec_cache[predictor].k.left_equal_key(input) ){
				CachedRow cr;
				sec_cache[predictor] = cr;
				cache_index[h] = 0;
			}
		}
	public:


		void set_hash_size(nst::u32 hash_size){
			if((*this).hash_size != hash_size){
				(*this).clear();
				(*this).hash_size = hash_size;
			}
		}

		~predictive_cache(){
		}

		inline bool hashed() const {
			return !cache_index.empty();
		}

		void enable(){
			enabled = true;
		}

		void disable(){
			enabled = false;
		}

		bool load(){

			if(!loaded){
				if(calc_total_use()+sizeof(CachedRow)*CIRC_SIZE+sizeof(stored::_Rid)*hash_size > (nst::u64)treestore_max_mem_use){
					return false;
				}
				//stx::storage::syncronized ul((*this).plock);
				using namespace NS_STORAGE;
				if(cache_index.empty()){
					cache_index.resize(hash_size);
					sec_cache.reserve(CIRC_SIZE/8);
					total_cache_size+=(sizeof(CachedRow)*sec_cache.capacity() + sizeof(stored::_Rid)*hash_size);
					printf("total_p_cache_size %.4g GiB\n",(double)total_cache_size/(1024.0*1024.0*1024.0));
				}
				loaded = true;
			}
			return loaded;
		}

		/// TODO: NB: the return value should be const
		const CompositeStored* _int_predict_row(stored::_Rid& predictor, BasicIterator& out, const CompositeStored& input){

			using namespace NS_STORAGE;
			if(predictor){
				predictor++;
				const u64 stop = std::min<u64>(sec_cache.size(), predictor + 8);

				while(predictor < stop){ /// this loop finds the hash item based on store history or order

					if(sec_cache[predictor].k.left_equal_key(input) ){
						++hash_hits;
						++hash_predictions;
						return  &sec_cache[predictor].k.return_or_copy(rval);

					}
					predictor++;
				}
			}
			if(cache_index.empty()){
				return NULL;
			}
			if(!enabled){
				predictor = 0;
				return NULL;
			}
			typename BasicIterator::key_type kin = input;
			unsigned int h = ((nst::u32)(size_t)kin) % hash_size;
			predictor = cache_index[h];

			if(predictor && sec_cache[predictor].k.left_equal_key(kin) ){

				++hash_hits; // neither hit nor miss
				return &sec_cache[predictor].k.return_or_copy(rval);

			}
			predictor = 0;
			++misses;
			return NULL;

		}

		const CompositeStored*  predict_row(stored::_Rid& predictor, BasicIterator& out, const CompositeStored& input){

			return _int_predict_row(predictor, out, input);

		}

		/// remove an entry if it exists
		void erase(const CompositeStored& input){
			nst::synchronized critical(erase_lock);
			/// push it on the erase buffer
			erased.push_back(input);
		}

		/// called when the cached isnt used for reading
		void flush_erases(){
			nst::synchronized critical(erase_lock);
			for(_Erased::iterator e = erased.begin(); e != erased.end(); ++e){
				_erase((*e));
			}
			erased.clear();

		}


		/// this function gets called for every missed prediction
		/// thereby 'adapting' to changing workloads
		void store(const BasicIterator & iter){
			if((nst::u64)calc_total_use() > (nst::u64)treestore_max_mem_use){
				clear();
				return;
			}
			if(!enabled) return;
			if(iter.invalid()) return;

			if(!load()) return;
			store_pos = sec_cache.size();
			if(store_pos >= CIRC_SIZE) return;
			size_t h = ((nst::u32)(size_t)iter.key()) % hash_size;
			//if(last_store == 3){
				size_t s = cache_index[h];
				if(s == 0){
					cache_index[h] = (stored::_Rid)sec_cache.size(); //store_pos+1;
				};
			//	last_store = 0;
			//}
			//++last_store;
			CachedRow cr;
			//cr.i = iter.construction();
			cr.k = iter.key();
			total_cache_size-=(sizeof(CachedRow)*sec_cache.capacity());
			sec_cache.push_back(cr);
			total_cache_size+=(sizeof(CachedRow)*sec_cache.capacity());
		}

		void reduce(){


		}

		/// remove everything

		void clear(){
			//stx::storage::syncronized ul((*this).plock);
			if(!cache_index.empty()){
				total_cache_size -= (sizeof(CachedRow)*CIRC_SIZE + sizeof(stored::_Rid)*hash_size);
				cache_index.clear();
				sec_cache.clear();
				sec_cache.resize(1);
			}
		}

		typedef std::vector<CachedRow> _SecCache;
		typedef std::vector<unsigned int> _RowCache;
    private:
        _Erased erased;
		stored::DynamicKey rval;
		nst::u32 hash_size;
		nst::u32 last_store;
		Poco::Mutex erase_lock;
    public:
		stored::_Rid rows;
		_SecCache sec_cache;
		_RowCache cache_index;
		CachedRow empty_row;
		//Poco::Mutex plock;

		NS_STORAGE::u64 store_pos;
		NS_STORAGE::u64 hits;

		NS_STORAGE::u64 misses;
		NS_STORAGE::u64 multi;

        bool enabled;
		bool loaded;

    public:
		predictive_cache()
		:   hash_size(11)
		,   store_pos(0)
		,   hits(0)
		,   misses(0)
		,   multi(0)
		,   enabled(treestore_predictive_hash!=0)
		,   loaded(false)

		{
			last_store = 3;

		}


	};


};
#endif
