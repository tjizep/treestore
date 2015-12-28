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

//#include "tree_stored.h"
#include <rabbit/rabbit.h>
#include "fields.h"
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
		
		typedef typename BasicIterator::key_type key_type;		
		typedef cached_row<typename BasicIterator::key_type> CachedRow;
		typedef std::vector<CompositeStored> _Erased;
		typedef stored::_Rid _Rid;
		
		typedef rabbit::unordered_map<key_type,_Rid, stored::fields_hash<key_type>> _CompHash;
	private:
		void _erase(const CompositeStored& input){
			
		}
		CompositeStored temp;
		key_type temp_key;
	public:


		void initialize(){

			hash_hits = 0;
			hash_predictions = 0;
			misses = 0;
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

			return true;
		}

		/// TODO: NB: the return value should be const
		
		const CompositeStored*  predict_row(stored::_Rid& predictor, BasicIterator& out, const CompositeStored& input){
			this->temp = input;
			this->temp.row = 0;
			_Rid r = 0;
			if(keys.get(this->temp,r)){
				this->temp.row = r;
				return &(this->temp);
			}
			return NULL;
			///return _int_predict_row(predictor, out, input);

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
			this->temp_key = const_cast<BasicIterator&>(iter).key();
			_Rid row = this->temp_key.row;
			this->temp_key.row = 0;
			keys[this->temp_key] = row;
			return;
			
		}

		void reduce(){


		}

		/// remove everything

		void clear(){
			
			//stx::storage::syncronized ul((*this).plock);
			
			loaded = false;
		}

		
    private:
        _Erased erased;
		stored::DynamicKey rval;
		
		
		Poco::Mutex erase_lock;
		_CompHash keys;
    public:
		
		CachedRow empty_row;
		
		
		NS_STORAGE::u64 hits;

		NS_STORAGE::u64 misses;
		NS_STORAGE::u64 multi;

        bool enabled;
		bool loaded;

    public:
		predictive_cache()
		:   hits(0)
		,   misses(0)
		,   multi(0)
		,   enabled(treestore_predictive_hash!=0)
		,   loaded(false)

		{
		

		}


	};


};
#endif
