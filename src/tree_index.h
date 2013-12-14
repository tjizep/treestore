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
#ifndef _TREE_INDEX_H_CEP2013_
#define _TREE_INDEX_H_CEP2013_
#include "collumn.h"
#include "PredictiveCache.h"
namespace tree_stored{
	class tree_index{

	private:
		
		
		CachedRow empty;
	private:
		_Rid get_rid(const CompositeStored& input){
			return input.row;
			
		}
		_PredictiveCache *cache;
	public:
		
		int ix;
		int fields_indexed;
		_Parts parts;
		_Parts density;
		ColIndex index;
		_Rid predictor;
		bool unique;
		tree_index(std::string name, bool unique)
		:	index(name)	, predictor(0), unique(unique)
		{
			cache = get_pcache(name);
		}
		virtual ~tree_index(){}
		
		const CompositeStored *predict(IndexIterator& io, CompositeStored& q){
			return cache->predict_row(predictor,io.get_i(),q);
			
		}
		void cache_it(IndexIterator& io){
			cache->store(io.get_i());
		}

		void begin(bool read){
			index.begin(read);
		}

		void commit1_asynch(){
			index.commit1_asynch();
		}

		void commit1(){
			index.commit1();
		}
		
		void commit2(){
			index.commit2();
		}
		
		void rollback(){
			index.rollback();
		}
		
		void share(){
		}
		void unshare(){
		}
		
		void clear_cache(){
			
		}
		void reduce_cache(){
			
		}
		typedef tree_index* ptr;
	};
	
};
#endif