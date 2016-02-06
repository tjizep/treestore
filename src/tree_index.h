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
#include <stx/btree.h>
#include "iterator.h"



namespace tree_stored{
	typedef stored::_Rid _Rid;
	template<typename index_key_type>
	class col_index{
	public:
		typedef stored::IntTypeStored<char> index_value;
		typedef index_key_type index_key;//StaticKey
		typedef stored::byte_col_encoder<index_key,int> interpolator_type;
		stored::abstracted_storage storage;
		////, std::less<index_key>,interpolator_type
		typedef stx::btree_set< index_key, stored::abstracted_storage, std::less<index_key> ,interpolator_type> _IndexMap; ///
		typedef typename _IndexMap::iterator iterator_type;
		typedef typename _IndexMap::const_iterator const_iterator_type;
		typedef typename ::iterator::ImplIterator<_IndexMap> IndexIterator;
		typedef std::vector<index_key, ::sta::stl_tracker<index_key> > _KeyBuffer;

		class index_iterator_impl : public stored::index_iterator_interface{
		private:
			stored::DynamicKey returned;
		public:
			///for quick type check
			IndexIterator value;
			index_iterator_impl() {//: stored::index_iterator_interface::type_id(1)
			}
			virtual ~index_iterator_impl(){
			}

			bool valid() const{
				return value.valid();
			}
			bool invalid() const {
				return value.invalid();
			};
			void next() {
				value.next();
			};
			bool previous() {
				return value.previous();
			};
			void first() {
				value.first();
			};
			void last() {
				value.last();
			};
			nst::u64 count(const index_iterator_interface& in) {
				return value.count(((index_iterator_impl&)in).value);
			};
			stored::DynamicKey& get_key() {
				return value.get_key().return_or_copy(returned);
			};
			void set_end(index_iterator_interface& in) {
				value.set_end(((index_iterator_impl&)in).value);
			}

		};


		class IndexLoader : public asynchronous::AbstractWorker
		{
		protected:
			Poco::AtomicCounter &loaders_away;
			_IndexMap &index;
			_KeyBuffer buffer;
			bool flush_keys;
		public:
			void flush_key_buffer(){
				if(!buffer.empty()){
					///std::sort(buffer.begin(), buffer.end());

					for(typename _KeyBuffer::iterator b = buffer.begin(); b != buffer.end(); ++ b){
						index.insert((*b));
					}
					if(flush_keys){
						index.flush_buffers();
					}
					buffer.clear();
				}
			}

		public:
			static const int MAX_KEY_BUFFER = 120000;/// size of the write key buffer
			static const int MIN_KEY_BUFFER = 10000;
			IndexLoader(_IndexMap& index,Poco::AtomicCounter &loaders_away)
			:	loaders_away(loaders_away)
			,	index(index)
			,	flush_keys(true)
			{
			}
			void clear(){
				buffer.clear();
			}
			size_t size() const {
				return buffer.size();
			}
			bool is_minimal() const {
				return ((*this).size() < MIN_KEY_BUFFER);
			}

			void set_flush()
			{
				(*this).flush_keys = true;
			}

			bool add(const index_key& k, const index_value& v)
			{
				if(buffer.empty())
					buffer.reserve(MAX_KEY_BUFFER);

				buffer.push_back(k);
				if(buffer.size() > MIN_KEY_BUFFER){

				}
				return buffer.size() < MAX_KEY_BUFFER;
			}

			virtual ~IndexLoader()
			{
				flush_key_buffer();
			}

			virtual void work(){
				flush_key_buffer();
				--loaders_away;
			}
		};
	private:
		_IndexMap index;
		typename _IndexMap::iterator the_end;
		bool modified;
		IndexLoader * loader;
		Poco::AtomicCounter loaders_away;
		unsigned int wid;
		index_key empty;
		mutable index_key rval;
	private:
		void destroy_loader(){
			if(loader != nullptr){
				loader->clear();
				delete loader;
			}
		}
		void wait_for_loaders(){
			destroy_loader();
			while(loaders_away > 0) os::zzzz(20);

		}
	public:

		col_index(std::string name)
		:	storage(name)
		,	index(storage)
		,	modified(false)
		,	loader(nullptr)
		,	wid(storage_workers::get_next_counter())
		,	empty(index_key())
		,	rval(index_key())
		{
			using namespace NS_STORAGE;
			this->the_end = index.end();
			this->index.share(name);
			this->storage.load_all();
		}

		~col_index(){
			wait_for_loaders();
			storage.close();
		}
		nst::u64 get_size(){
			return index.size();
		}
		void set_end(){
			the_end = index.end();
		}

		void reduce_use(){
			index.reduce_use();
		}

		void share(){
		}

		void unshare(){
		}

		void scan(){
		}

		std::string get_name() const {
			return storage.get_name();
		}

		IndexIterator first(){
			return IndexIterator (index, index.begin());
		}

		void from_initializer(IndexIterator& out, const typename _IndexMap::iterator::initializer_pair& ip){
			out.from_initializer(index, ip);
		}

		void lower_(IndexIterator& out,  const index_key& k){
			out.set(index.lower_bound(k),the_end,index);
		}

		std::pair<const index_key&,bool> lower_key( const index_key& k) const {
			dbg_print("Index size %lld\n",index.size());
			typename _IndexMap::const_iterator i = index.lower_bound(k);
			if(i != index.end()){				
				dbg_print("found key row %lld\n",i.key().row);
				return std::make_pair(i.key(),true);
			}
			dbg_print("no key found",index.size());
			return std::make_pair(empty,false);
		}
		bool find_key( stored::DynamicKey& k) const {
			index_key f = k;
			typename _IndexMap::const_iterator i = index.lower_bound(f);
			if(i != index.end()){				
				if(f.equal_key(i.key())){
					k.row = i.key().row;
					return true;
				}				
			}			
			return false;
		}
		IndexIterator lower(const index_key& k){
			return IndexIterator(index, index.lower_bound(k));
		}

		IndexIterator upper(const index_key& k){
			return IndexIterator(index, index.upper_bound(k));
		}

		IndexIterator end(){
			return IndexIterator (index, index.end());
		}

		IndexIterator find(const index_key& k){
			return IndexIterator (index, index.find(k));
		}

		void add(const index_key& k, const index_value& v){
			modified = true;
			if( treestore_current_mem_use > treestore_max_mem_use*0.9 ) {
				while(loaders_away>0){
					os::zzzz(50);
				}
			}
			if(true){
				index.insert(k);

			}else{
				if(loader==nullptr){
					loader = new IndexLoader(index, loaders_away);
				}
				if(!loader->add(k,v)){
					storage_workers::get_threads(wid).add(loader);
					++loaders_away;

					loader = nullptr;
				}
			}
		}

		void remove(const index_key& k){
			modified = true;
			index.erase(k);
		}

		void flush(){
			index.reduce_use();
			index.flush();
			set_end();
		}

		void begin(bool read,bool shared=true){
			stored::abstracted_tx_begin(read, shared, storage, index);
			set_end();
		}

		void rollback(){
			if(modified){
				wait_for_loaders();
				index.reduce_use();

			}
			storage.rollback();
			modified = false;
		}
		void reduce_storage(){
			storage.reduce();
		}
		void commit1_asynch(){
			if(loader!=nullptr){
				if(loaders_away==0 && loader->is_minimal()){
					delete loader;
				}else{
					storage_workers::get_threads(wid).add(loader);
					++loaders_away;
				}
				loader = nullptr;
			}
		}
		void commit1(){
			if(modified){

				wait_for_loaders();

				index.flush_buffers();

			}
			if(treestore_current_mem_use > treestore_max_mem_use){
				storage.reduce();
			}
		}

		void commit2(){
			if(modified){
				if(!storage.commit()){

				}
			}
			modified = false;

		}
	};
	/// stored::DynamicKey
	template<typename index_key_type>
	class tree_index : public stored::index_interface{
	public:
		typedef col_index<index_key_type> ColIndex;
		typedef index_key_type key_type;
		typedef typename ColIndex::IndexIterator IndexIterator;
	private:
		//CachedRow empty;
		Poco::Mutex plock;
	private:
		typedef typename ColIndex::iterator_type _BasicIterator;
		typedef predictive_cache<typename ColIndex::iterator_type> _PredictiveCache;



		typedef std::vector<eraser_interface*> _ErasorList;
		typedef std::shared_ptr<_ErasorList> _ErasorListPtr;
		typedef std::unordered_map<std::string, std::shared_ptr<_ErasorList>> _ECaches;
		_ECaches* get_erasers(){
			static _ECaches pc;
			return &pc;
		}

		void register_eraser(std::string name,eraser_interface* er){
			stx::storage::syncronized ul(plock);
			_ECaches * erasers = get_erasers();
			_ECaches::iterator e = erasers->find(name);
			_ErasorListPtr elist;
			if(e == erasers->end()){
				elist = std::make_shared<_ErasorList>();
				(*erasers)[name] = elist;
			}else{
				elist = (*e).second;
			}
			for(_ErasorList::iterator el = elist->begin(); el != elist->end(); ++el){
				if((*el) == er){
					return;
				}
			}
			elist->push_back(er);
		}

		void unregister_eraser(std::string name,eraser_interface* er){
			stx::storage::syncronized ul(plock);
			_ECaches * erasers = get_erasers();
			_ECaches::iterator e = erasers->find(name);
			_ErasorListPtr elist;
			if(e == erasers->end()){
				elist = std::make_shared<_ErasorList>();
				(*erasers)[name] = elist;
			}else{
				elist = (*e).second;
			}
			for(_ErasorList::iterator el = elist->begin(); el != elist->end(); ++el){
				if((*el) == er){
					elist->erase(el,el);
					return;
				}
			}

		}
		void send_erase(const std::string &name,const CompositeStored& input){
			stx::storage::syncronized ul(plock);
			_ECaches * erasers = get_erasers();
			_ECaches::iterator e = erasers->find(name);
			_ErasorListPtr elist;
			if(e == erasers->end()){
				return;
			}else{
				elist = (*e).second;
			}
			for(_ErasorList::iterator el = elist->begin(); el != elist->end(); ++el){
				(*el)->erase(input);
			}
		}
		stored::_Rid get_rid(const CompositeStored& input){
			return input.row;

		}
		_PredictiveCache cache;

		ColIndex index;
		typedef typename ColIndex::index_iterator_impl iterator_impl_type;
		typedef rabbit::unordered_map<nst::u64,std::shared_ptr<iterator_impl_type>> _IteratorInstances;
		_IteratorInstances current_instances;
		iterator_impl_type cur;
		iterator_impl_type cur_map;
		iterator_impl_type _1st;
		iterator_impl_type _lst;
	public:

		stored::index_iterator_interface * get_index_iterator(nst::u64 inst) {
			std::shared_ptr<iterator_impl_type> &r = current_instances[inst];
			if(r==nullptr){
				r = std::make_shared<iterator_impl_type>();
			}
			return r.get();
		}
		
		stored::index_iterator_interface * get_prepared_index_iterator() {
			cur.value = index.first();
			return &cur;
		}

		stored::index_iterator_interface * get_first1() {
			return &_1st;
		}

		stored::index_iterator_interface * get_last1() {
			return &_lst;
		}


		stored::_Rid predictor;
		bool unique;
		tree_index(std::string name, bool unique)
		:	index(name)	, predictor(0), unique(unique)
		{
			//cache = get_pcache(name);
			//register_eraser(name, &cache);
			(*this).name = name;
		}
		virtual ~tree_index(){
			//unregister_eraser(name, &cache);
			/// index.get_storage().release();
		}

		const CompositeStored *predict(stored::index_iterator_interface& io, CompositeStored& q){

			return cache.predict_row(predictor,((typename ColIndex::index_iterator_impl&)io).value.get_i(),q);

		}
		void cache_it(stored::index_iterator_interface& io){
			if(unique){

				cache.store(((typename ColIndex::index_iterator_impl&)io).value.get_i());
			
			}

		}
		bool is_unique() const {
			return unique;
		}
		void set_col_index(int ix){
			(*this).ix = ix;
		}
		void set_fields_indexed(int indexed){
			(*this).fields_indexed = indexed;
		}
		void push_part(_Rid part){
			this->parts.push_back(part);
		}
		void push_density(_Rid dens) {
			density.push_back(dens);
		}
		virtual size_t densities() const {
			return density.size();
		}
		_Rid& density_at(size_t at) {
			return density[at];
		}
		const _Rid& density_at(size_t at) const {
			return density[at];
		}
		void end(stored::index_iterator_interface& out){
			((typename ColIndex::index_iterator_impl&)out).value = index.end();
		}
		void first(stored::index_iterator_interface& out){
			((typename ColIndex::index_iterator_impl&)out).value = index.first();
		}
		
		void lower_(stored::index_iterator_interface& out,const tree_stored::CompositeStored& key){
			index.lower_(((typename ColIndex::index_iterator_impl&)out).value, key);
		}
		void lower(stored::index_iterator_interface& out,const tree_stored::CompositeStored& key){
			index.lower_(((typename ColIndex::index_iterator_impl&)out).value , key);
		}
		void upper(stored::index_iterator_interface& out, const tree_stored::CompositeStored& key){
			((typename ColIndex::index_iterator_impl&)out).value  = index.upper(key);
		}
		void add(const tree_stored::CompositeStored& k){
			if(k.size()==0){
				printf("[TS] [WARNING] inserting empty key\n");
			}
			index.add(k, 0);
		}
		void remove(const tree_stored::CompositeStored& k){
			index.remove(k);
		}
		void find(stored::index_iterator_interface& out, const tree_stored::CompositeStored& key){
			((typename ColIndex::index_iterator_impl&)out).value = index.find(key);
		}
		_Rid resolve(tree_stored::CompositeStored& k)  {
			k.row = 0;
			if(!index.find_key(k)){
				k.row = stored::MAX_ROWS;
			}
			return k.row;
		}
		void from_initializer(stored::index_iterator_interface& out, const stx::initializer_pair& ip){
			index.from_initializer(((typename ColIndex::index_iterator_impl&)out).value ,ip);
		}
		void reduce_use(){
			index.reduce_use();
			cache.clear();
		}

		void begin(bool read,bool shared){
			index.begin(read,shared);			
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
			index.reduce_use();
		}
		void reduce_cache(){

		}
		typedef tree_index* ptr;
	};

};
#endif
