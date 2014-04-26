#ifndef _TREE_TABLE_H_CEP2013_
#define _TREE_TABLE_H_CEP2013_
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
#include "tree_stored.h"
#include "conversions.h"
#include "tree_index.h"
#include "system_timers.h"

typedef std::vector<std::string> _FileNames;
extern my_bool treestore_efficient_text;
extern char treestore_use_primitive_indexes;
namespace tree_stored{
	class InvalidTablePointer : public std::exception{
	public:
		InvalidTablePointer () throw(){
		}
	};
	
	class abstract_conditional_iterator{
	public:
		abstract_conditional_iterator(){};
		virtual ~abstract_conditional_iterator(){};
		static const _Rid NoRecord = 0xFFFFFFFF;
		/// returns NoRecord if no records where found
		virtual _Rid iterate(_Rid start) = 0;
		virtual _Rid evaluate(_Rid start) = 0;
		typedef std::shared_ptr<abstract_conditional_iterator> ptr;
	};
	class logical_conditional_iterator : public abstract_conditional_iterator{
	protected:
		typedef std::vector<abstract_conditional_iterator::ptr> _LogicalConditions;
		_LogicalConditions logical;
	public:
		logical_conditional_iterator(){};
		virtual ~logical_conditional_iterator(){};
		void push_condition(abstract_conditional_iterator::ptr condition){
			logical.push_back(condition);
		}
		/// returns NoRecord if no records where found
		virtual _Rid iterate(_Rid start) = 0;
		virtual _Rid evaluate(_Rid start) = 0;
		
		typedef std::shared_ptr<logical_conditional_iterator> ptr;
	};

	class conditional_and_iterator : public logical_conditional_iterator{
		private:
		

		public:
		
			conditional_and_iterator(){
			
			
			};
			virtual ~conditional_and_iterator(){};

		
			/// returns NoRecord if no records where found
			virtual _Rid iterate(_Rid _start) {
				_Rid start = _start;
				/// iterative optimization for and condition(s)
				for(_LogicalConditions::iterator l = logical.begin();l != logical.end();++l){
					_Rid pstart = (*l)->iterate(start);
					if(pstart > start){
						/// skip evaluations on sibling/co conditions
						start = pstart;
					}
					if(start == NoRecord)
						break;

				}
				return start;
			}
			virtual _Rid evaluate(_Rid _start) {
				_Rid start = _start;
			
				for(_LogicalConditions::iterator l = logical.begin();l != logical.end();++l){
					if((*l)->evaluate(start) != start){
						return NoRecord;
					}
				}
				return start;
			
			
			}
			typedef std::shared_ptr<conditional_and_iterator> ptr;
		};

	
		class conditional_or_iterator : public logical_conditional_iterator{
		private:
			_Rid max_rid;
		public:
		
			conditional_or_iterator(_Rid max_rid){
				(*this).max_rid = max_rid;
			
			};
			virtual ~conditional_or_iterator(){};
		
			/// returns NoRecord if no records could be found
			virtual _Rid iterate(_Rid _start) {
				_Rid start = _start;
				while(start < max_rid){
				
					for(_LogicalConditions::iterator l = logical.begin();l != logical.end();++l){
						_Rid r = (*l)->evaluate(start) ;
						if(r == start){
							return start;
						}										
					}				
					++start;					
				}			
				return NoRecord;
			}

			virtual _Rid evaluate(_Rid start) {
				for(_LogicalConditions::iterator l = logical.begin();l != logical.end();++l){
					if((*l)->evaluate(start) == start){
						return start;
					}
				}
				return NoRecord;
			
			}
			typedef std::shared_ptr<conditional_or_iterator> ptr;
		};

	
	class abstract_my_collumn {
	public:
		abstract_my_collumn(){
		}
		virtual ~abstract_my_collumn(){
		}
		virtual void seek_retrieve(stored::_Rid row, Field* f) = 0;
		virtual void add_row(stored::_Rid row, Field * f) = 0;
		virtual void erase_row(stored::_Rid row) = 0;
		virtual NS_STORAGE::u32 field_size() const = 0;
		virtual stored::_Rid stored_rows() const = 0;
		virtual void initialize(bool by_tree) = 0;
		virtual void compose(CompositeStored& comp)=0;
		virtual void compose(stored::_Rid r, CompositeStored& comp)=0;
		virtual void compose(CompositeStored & to, Field* f,const uchar * n_ptr, uint flags)=0;
		virtual bool equal(stored::_Rid row, Field* f)=0;
		virtual void reduce_cache_use() = 0;
		virtual void reduce_col_use() = 0;
		virtual void seek_retrieve( Field* f, stored::_Rid row, collums::_RowData& row_data ) = 0;

		virtual nst::u32 get_rows_per_key() = 0;
		virtual void flush() = 0;
		virtual void begin(bool read) = 0;
		virtual void commit1_asynch() = 0;
		virtual void commit1() = 0;
		virtual void commit2() = 0;
		virtual void rollback() = 0;
		/// push down condition
		virtual abstract_conditional_iterator::ptr create_condition(const Item_func* f, const Item* val, Field* target) = 0;

	};





	template <typename _Fieldt>
	class my_collumn : public abstract_my_collumn{
	protected:
		typedef typename collums::collumn<_Fieldt> _Colt;
		typedef std::vector<std::pair<stored::_Rid,_Fieldt> > _RowBuffer;
		static const int MAX_BUFFERED_ROWS = 10000;
		class ColAdder : public asynchronous::AbstractWorker{
		protected:
            Poco::AtomicCounter &workers_away;
			_Colt &col;


			_RowBuffer buffer;
		protected:
			void flush_data(){
				if(!buffer.empty()){
					for(typename _RowBuffer::iterator r = buffer.begin(); r != buffer.end(); ++r){
						col.add((*r).first, (*r).second);
					}
					col.flush2();
					buffer.clear();
				}
			}
		public:
			void clear(){
				buffer.clear();
			}
			ColAdder(_Colt &col,Poco::AtomicCounter &workers_away):workers_away(workers_away),col(col){
			}
			void add(stored::_Rid r,const _Fieldt & val){
				buffer.push_back(std::make_pair(r,val));
			}
			size_t size() const {
				return buffer.size();
			}

			virtual void work(){
				flush_data();
				--workers_away;

			}
			virtual ~ColAdder(){
				flush_data();
			}
		};

		_Colt col;
		ColAdder * worker;
        unsigned int wid;/// worker id for table loading.
		_Fieldt temp;
		conversions convertor;

		Poco::AtomicCounter workers_away;
		nst::u32 number;
		nst::u32 rowsize;
		collums::_LockedRowData * row_datas;



		void wait_for_workers(){
			destroy_worker();

			while (workers_away>0) os::zzzz(20);

		}
		void destroy_worker(){
			if((*this).worker!=nullptr){
				(*this).worker->clear();
				delete (*this).worker;
				(*this).worker = nullptr;
			}
		}
		void add_worker(){
			if((*this).worker!=nullptr){
				if((*this).workers_away == 0 && (*this).worker->size() < MAX_BUFFERED_ROWS){
					delete (*this).worker;
				}else{
					storage_workers::get_threads((*this).wid).add((*this).worker);
					++((*this).workers_away);
				}
				(*this).worker = nullptr;
			}
		}
		void make_item_val(_Fieldt& v, const Item* val){
            #ifdef _MSC_VER
			switch(val->type()){
				case Item::INT_ITEM:
					v.set_value(((const Item_int*)val)->value);
					break;
				case Item::REAL_ITEM:
					v.set_value(((const Item_real*)val)->value);
					break;
				case Item::DECIMAL_ITEM:
					v.set_value(((const Item_decimal*)val)->value);
					break;
				case Item::STRING_ITEM:
					break;
				default:
					break;
			}
			#endif;
		}
	public:

		my_collumn(std::string name, nst::u32 number, nst::u32 rowsize,collums::_LockedRowData *row_datas, bool do_load)
		:	col(name, do_load)
		,	worker(nullptr)
		,	wid(storage_workers::get_next_counter())
		,	number(number)
		,	rowsize(rowsize)
		,	row_datas(row_datas)
		{

		}

		virtual ~my_collumn(){

			wait_for_workers();

		}

		virtual void add_row(collums::_Rid row, Field * f){
			convertor.fget(temp, f, NULL, 0);
			//col.add(row, temp);
			if(worker == nullptr){
				worker = new ColAdder(col,workers_away);
			}
			worker->add(row, temp);
			if(worker->size() >= MAX_BUFFERED_ROWS){
				add_worker();
			}

		}
	protected:
		struct eq_condition{
			inline bool is_true(const _Fieldt &right,const _Fieldt &left) const {
				return right == left;
			}
		};
		
		struct gt_condition{
			inline bool is_true(const _Fieldt &right,const _Fieldt &left) const {
				return right > left; 
			}
		};
		
		struct ge_condition{
			inline bool is_true(const _Fieldt &right,const _Fieldt &left) const {
				return !(right < left);
			}
		};
		
		struct lt_condition{
			inline bool is_true(const _Fieldt &right,const _Fieldt &left) const {
				return right < left;
			}
		};
		
		struct le_condition{
			inline bool is_true(const _Fieldt &right,const _Fieldt &left) const {
				return !(right > left);
			}
		};

		struct empty_condition{
			inline bool is_true(const _Fieldt &,const _Fieldt &) const {
				return true;
			}
		};
		template<typename _Condition>
		class entropic_conditional_iterator : public abstract_conditional_iterator{
		protected:
			_Condition contition;
			_Fieldt value;
			_Colt * col;
		public:
			virtual ~entropic_conditional_iterator(){};
			entropic_conditional_iterator(_Colt* col, _Fieldt value){
				(*this).col = col;
				(*this).value = value;
			}
			/// returns NoRecord if no records where found
			virtual _Rid iterate(_Rid start) {
				if(start >= col->get_rows()){
					return NoRecord;
				}
				while(!condition.is_true(col->seek_by_cache(start), value)){
					++start;
					if(start >= col->get_rows()){
						return NoRecord;
					}
				}
				return start;
			}
			virtual _Rid evaluate(_Rid start) {
				
				if(start >= col->get_rows()){
					return NoRecord;
				}

				if(!condition.is_true(col->seek_by_cache(start), value)){
					return NoRecord;					
				}

				return start;
			}
		};

		template<typename _Condition>
		class standard_conditional_iterator : public abstract_conditional_iterator{
		protected:
			_Condition condition;
			_Fieldt value;
			_Colt * col;
		public:
			standard_conditional_iterator(_Colt* col, _Fieldt value){
				(*this).col = col;
				(*this).value = value;
			}
			virtual ~standard_conditional_iterator(){};
			/// returns NoRecord if no records where found
			virtual _Rid iterate(_Rid start) {

				if(start >= col->get_rows()){
					return NoRecord;
				}
				while(!condition.is_true(col->seek_by_cache(start), value)){
					++start;
					
					if(start >= col->get_rows()){
						return NoRecord;
					}
				}
				return start;
			}
			virtual _Rid evaluate(_Rid start) {
				
				if(start >= col->get_rows()){
					return NoRecord;
				}

				if(!condition.is_true(col->seek_by_cache(start), value)){
					return NoRecord;					
				}

				return start;
			}
		};
		
		/// push does condition evaluations

		abstract_conditional_iterator::ptr push_gt_condition(Field* target, const Item* val){
			_Fieldt pushed;
			convertor.make_item_val(pushed,target,val);
			return std::make_shared<standard_conditional_iterator<gt_condition>>(&col, pushed);
		}

		abstract_conditional_iterator::ptr push_ge_condition(Field* target, const Item* val){
			_Fieldt pushed;
			convertor.make_item_val(pushed,target,val);
			return std::make_shared<standard_conditional_iterator<ge_condition>>(&col, pushed);
		}

		abstract_conditional_iterator::ptr push_e_condition(Field* target, const Item* val){
			_Fieldt pushed;
			convertor.make_item_val(pushed,target,val);
			return std::make_shared<standard_conditional_iterator<eq_condition>>(&col, pushed);
		}

		abstract_conditional_iterator::ptr push_lt_condition(Field* target, const Item* val){
			_Fieldt pushed;
			convertor.make_item_val(pushed,target,val);
			return std::make_shared<standard_conditional_iterator<lt_condition>>(&col, pushed);
		}

		abstract_conditional_iterator::ptr push_le_condition(Field* target, const Item* val){
			_Fieldt pushed;
			convertor.make_item_val(pushed,target,val);
			return std::make_shared<standard_conditional_iterator<le_condition>>(&col, pushed);
		}
		abstract_conditional_iterator::ptr push_empty_condition(Field* target, const Item* val){
			_Fieldt pushed;
			convertor.make_item_val(pushed,target,val);
			return std::make_shared<standard_conditional_iterator<empty_condition>>(&col, pushed);
		}

	public:

		virtual abstract_conditional_iterator::ptr create_condition(const Item_func* f, const Item* val, Field* target){
			Item_func::Functype ft = f->functype();
			switch(ft){
			case Item_func::GT_FUNC: // greater than
				return push_gt_condition(target,val);
				break;
			case Item_func::GE_FUNC: // greater than
				return push_ge_condition(target,val);
				break;
			case Item_func::EQ_FUNC: // equal
				return push_e_condition(target,val);
				break;
			case Item_func::LE_FUNC: // less or equal
				return push_le_condition(target,val);
				break;
			case Item_func::LT_FUNC: // less than
				return push_lt_condition(target,val);
				break;
			default: // unimplemented type of conditional binary function
#				ifdef _FURTHER_CONDITIONS_
				for(uint ac =0; ac < f->argument_count(); ++ac){
					const Item * ia = f->arguments()[ac];
					Item::Type it = ia->type();
					cond_push_(ia);
				}
#				endif
				break;
			};

			return push_empty_condition(target, val);
		}
		virtual void pop_conditions(){
		}
		

		virtual void erase_row(collums::_Rid row){
			col.erase(row);
		}

		virtual void initialize(bool by_tree){
			col.initialize(by_tree);
		}

		virtual void compose(CompositeStored & to){
			to.add(temp);
		}

		virtual nst::u32 get_rows_per_key(){
			return col.get_rows_per_key();
		}

		virtual void compose(stored::_Rid r, CompositeStored & to){
			const _Fieldt& field = col.seek_by_tree(r);
			to.add(field);
		}

		virtual void compose(CompositeStored & to, Field* f, const uchar * ptr, uint flags){
			//convertor.fget(temp, f, ptr, flags);
			//to.add(temp.get_value());// TODO: due to this interface, BLOBS are impossible in indexes
			convertor.fadd(to,temp,f,ptr,flags);
		}

		virtual bool equal(stored::_Rid row, Field* f){
			convertor.fget(temp, f, NULL, 0);
			const _Fieldt& t = col.seek_by_cache(row);
			if(col.is_null(t))
				return f->is_null();
			return (t==temp);
		}



		template<typename _PredictedFieldType>
		struct _PredictorContext{
			void store(stored::_Rid row, const _PredictedFieldType& t ){
			}
			const _Fieldt* find(stored::_Rid row){
				return nullptr;
			}
		};

		/// Predictive hash for experimental use, will probably be replaced
		/// by row oriented cache since that would usually have better
		/// memory page read access behaviour

		template<typename _StoredType>
		struct _SimplePredictorContextImplmentor{
			enum {
				MaxStored = 8000000,
				MaxMapped = 5000000
			};
			typedef stored::_Rid _MappingPrimitive;

			nst::u64 mapper_size;
			mutable nst::u64 finds;
			mutable nst::u64 predicted;
			mutable nst::u64 unpredicted;
			mutable nst::u64 nothing;

			typedef std::pair<_MappingPrimitive,_StoredType> _StorePair;
			typedef std::vector<_StorePair> _StoredData;
			typedef std::vector<stored::_Rid> _DataMap;

			_StoredData stored;

			_DataMap mapping;

			_MappingPrimitive store_pos;

			mutable _MappingPrimitive last_found;

			_SimplePredictorContextImplmentor(){
				mapping.resize(MaxMapped);
				stored.resize(MaxStored+1);
				store_pos = 1;
				last_found = 0;
				predicted = 0;
				unpredicted = 0;
				nothing = 0;
				finds = 0;
			}

			~_SimplePredictorContextImplmentor(){

			}

			const _StoredType* find(stored::_Rid row) const {
				//++finds;
				//if(finds % 1000000 == 0){
				//	printf("finds %lld, predicted %lld, unpredicted %lld, not found %lld\n", finds, predicted, unpredicted, nothing);
				//}
				/**/
				if(last_found < stored.size()-4){

					for(int i=0;i<4;++i){
						if(stored[++last_found].first == row){
							//++predicted;
							return &(stored[last_found].second);
						}

					}
				}
				//last_found = 0;
				_MappingPrimitive m = mapping[row  % MaxMapped] ;
				if(m != 0){
					_MappingPrimitive e =  m + 3;
					while (m < e){
						if(stored[m].first == row){
							last_found = m;
							//++unpredicted;
							return &(stored[m].second);
						}
						++m;
					}

				}
				++nothing;
				return nullptr;
			}

			void store(stored::_Rid row, const _StoredType& data){
				if(store_pos < MaxStored){
					_MappingPrimitive m = store_pos++;
					//if(row % 4 == 0)
						mapping[row % MaxMapped] = m ;

					stored[m] = std::make_pair(row, data);
				}
			}
		};


#define _EXPERIMENT_PCACHEp
#ifdef _EXPERIMENT_PCACHE

		template<>
		struct _PredictorContext<stored::IntStored>{
			//_PredictorContextImplmentor<IntStored> implementation;
			_SimplePredictorContextImplmentor<stored::IntStored> implementation;

			void store(_Rid row, const stored::IntStored& t ){
				implementation.store(row, t);
			}

			const _Fieldt* find(_Rid row){
				return implementation.find(row);
			}
		};

		template<>
		struct _PredictorContext<stored::UIntStored>{
			//_PredictorContextImplmentor<IntStored> implementation;
			_SimplePredictorContextImplmentor<stored::UIntStored> implementation;

			void store(_Rid row, const stored::UIntStored& t ){
				implementation.store(row, t);
			}

			const _Fieldt* find(_Rid row){
				return implementation.find(row);
			}
		};
#endif
		_PredictorContext<_Fieldt> predictor;
		/// seek from
		private:
			_Fieldt& ft_from_rowdata(std::vector<nst::u8>& row_data, nst::u16 dp){
				return *((_Fieldt*)&row_data[dp]);
			}
			inline nst::u16 * u16from_rd(std::vector<nst::u8>& row_data){
				return ((nst::u16*)&row_data[0]);
			}

			void seek_retrieve_from_shared_row_data(Field* f, stored::_Rid row, collums::_RowData& row_data) {

				nst::u16 dpos = u16from_rd(row_data)[number];
				if(dpos == 0){
					const _Fieldt& t = col.seek_by_cache(row);
					if(col.is_null(t)){
						f->set_null();
					}else{
						f->set_notnull();

						nst::synchronized sr(row_datas->lock);
						nst::u16 dpos = (nst::u16)row_data.size();
						row_data.resize(dpos+sizeof(_Fieldt));
						new (&row_data[dpos])  _Fieldt(t);

						u16from_rd(row_data)[number] = dpos;
						convertor.fset(row, f, t);
					}
				}else{
					f->set_notnull();
					convertor.fset(row, f, ft_from_rowdata(row_data,dpos));
				}

			}
		public:

		virtual void seek_retrieve(stored::_Rid row, Field* f) {

			const _Fieldt * predicted = predictor.find(row);
			if(predicted!=nullptr){
				f->set_notnull();
				convertor.fset(row, f, * predicted);
				return;
			}
			const _Fieldt& t = col.seek_by_cache(row);

			if(col.is_null(t)){
				f->set_null();
			}else{
				f->set_notnull();

				predictor.store(row, t);

				convertor.fset(row, f, t);
			}
		}
		virtual	void seek_retrieve( Field* f, stored::_Rid row, collums::_RowData& row_data) {
			seek_retrieve_from_shared_row_data(f, row, row_data);
		}

		virtual stored::_Rid stored_rows() const {
			return col.get_rows();
		}

		virtual NS_STORAGE::u32 field_size() const {
			return sizeof(_Fieldt)/2;
		}

		virtual void flush() {
			col.flush();
		}
		virtual void commit1_asynch() {
			add_worker();
		}
		virtual void commit1() {

			wait_for_workers();
			col.reduce_tree_use();
			col.commit1();
		}

		virtual void commit2() {
			col.commit2();
		}

		virtual void begin(bool read) {
			wait_for_workers();
			col.tx_begin(read);
		}
		virtual void rollback() {

			wait_for_workers();
			col.rollback();
		}
		virtual void reduce_col_use() {
			col.reduce_tree_use();
		}
		virtual void reduce_cache_use() {
			col.reduce_cache_use();
		}
	};

	typedef std::vector<stored::index_interface::ptr> _Indexes;
	typedef std::vector<abstract_my_collumn*> _Collumns;
	struct selection_tuple{
		selection_tuple()
		:	col(NULL)
		,	field(NULL)
		,	saved_ptr(NULL)
		{
		}

		selection_tuple(const selection_tuple& right)
		:	col(NULL)
		,	field(NULL)
		,	saved_ptr(NULL)
		{
			*this = right;
		}

		selection_tuple& operator=(const selection_tuple& right){
			col = right.col;
			field = right.field;
			saved_ptr = right.saved_ptr;
			return *this;
		}

		void save_ptr(){
			saved_ptr = NULL;
			if(field != NULL)
				saved_ptr = field->ptr;
		}

		void restore_ptr(){
			if(field != NULL)
				field->ptr = saved_ptr;

		}

		abstract_my_collumn * col;
		Field * field;
        uchar * saved_ptr;
	};

	typedef std::vector<selection_tuple> _Selection;
	class tree_table{
	public:
		typedef stored::IntTypeStored<stored::_Rid> _StoredRowId;
		typedef stored::IntTypeStored<unsigned char> _StoredRowFlag;
		typedef std::vector<abstract_conditional_iterator::ptr> _Conditional;

		typedef stx::btree_map<_StoredRowId, _StoredRowFlag, stored::abstracted_storage> _TableMap;
		struct shared_data{
			shared_data(){
			}
			nst::u64 last_write_lock_time;
		};
		typedef std::map<std::string , std::shared_ptr<shared_data>> _SharedData;
		static Poco::Mutex shared_lock;
		static _SharedData shared;
	protected:
		bool changed;
		inline const char* INDEX_SEP(){
			return "__";
		}
		inline const char* TABLE_SEP(){
			return "_";
		}
		void load_cols(TABLE *table_arg){
			TABLE_SHARE *share= table_arg->s;
			cols.clear();
			cols.resize(share->fields);
			std::string path = table_arg->s->path.str;
			nst::u32 rowsize = 0;
			for (Field **field=share->field ; *field ; field++){
				++rowsize;
			}
			collums::_LockedRowData * lrd = collums::get_locked_rows(storage.get_name());
			for (Field **field=share->field ; *field ; field++){
				ha_base_keytype bt = (*field)->key_type();
				nst::u32 fi = (*field)->field_index;
				switch(bt){
					case HA_KEYTYPE_END:
						// ERROR ??
						break;
					case HA_KEYTYPE_FLOAT:
						cols[(*field)->field_index] = new my_collumn<stored::FloatStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,false);
						break;
					case HA_KEYTYPE_DOUBLE:
						cols[(*field)->field_index] = new my_collumn<stored::DoubleStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,false);
						break;
					case HA_KEYTYPE_SHORT_INT:
						cols[(*field)->field_index] = new my_collumn<stored::ShortStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,false);
						break;
					case HA_KEYTYPE_LONG_INT:
						cols[(*field)->field_index] = new my_collumn<stored::IntStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,false);
						break;
					case HA_KEYTYPE_USHORT_INT:
						cols[(*field)->field_index] = new my_collumn<stored::UShortStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,false);
						break;
					case HA_KEYTYPE_ULONG_INT:
						cols[(*field)->field_index] = new my_collumn<stored::ULongIntStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,false);
						break;
					case HA_KEYTYPE_LONGLONG:
						cols[(*field)->field_index] = new my_collumn<stored::LongIntStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,false);
						break;
					case HA_KEYTYPE_ULONGLONG:
						cols[(*field)->field_index] = new my_collumn<stored::ULongIntStored>(path+TABLE_SEP()+(*field)->field_name,fi, rowsize,lrd,false);
						break;
					case HA_KEYTYPE_INT24:
						cols[(*field)->field_index] = new my_collumn<stored::IntStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd, false);
						break;
					case HA_KEYTYPE_UINT24:
						cols[(*field)->field_index] = new my_collumn<stored::UIntStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd, false);
						break;
					case HA_KEYTYPE_INT8:
						cols[(*field)->field_index] = new my_collumn<stored::CharStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,false);
						break;
					case HA_KEYTYPE_BIT:
						cols[(*field)->field_index] = new my_collumn<stored::BlobStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,false);

						break;

					case HA_KEYTYPE_NUM:			/* Not packed num with pre-space */
					case HA_KEYTYPE_TEXT:			/* Key is sorted as letters */
						cols[(*field)->field_index] = new my_collumn<stored::VarCharStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,treestore_efficient_text);
						break;

					case HA_KEYTYPE_BINARY:			/* Key is sorted as unsigned chars */
						cols[(*field)->field_index] = new my_collumn<stored::BlobStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,treestore_efficient_text);
						break;
					/* Varchar (0-255 bytes) with length packed with 1 byte */
					case HA_KEYTYPE_VARTEXT1:               /* Key is sorted as letters */
					/* Varchar (0-65535 bytes) with length packed with 2 bytes */
					case HA_KEYTYPE_VARTEXT2:		/* Key is sorted as letters */
						cols[(*field)->field_index] = new my_collumn<stored::VarCharStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,treestore_efficient_text);
						break;
					case HA_KEYTYPE_VARBINARY1:             /* Key is sorted as unsigned chars length packed with 1 byte*/
						/* Varchar (0-65535 bytes) with length packed with 2 bytes */
					case HA_KEYTYPE_VARBINARY2:		/* Key is sorted as unsigned chars */
						cols[(*field)->field_index] = new my_collumn<stored::BlobStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,treestore_efficient_text);
						break;

					default:
						//TODO: ERROR ??
						/// default to var bin
						cols[(*field)->field_index] = new my_collumn<stored::BlobStored>(path+TABLE_SEP()+(*field)->field_name,fi,rowsize,lrd,treestore_efficient_text);

						break; //do nothing pass
				}//switch

				//_row_count = std::max<_Rid>(_row_count, cols[(*field)->field_index]->stored_rows());
			}
		}
		_FileNames create_file_names(TABLE *table_arg){

			uint i = 0;
			KEY *pos;
			TABLE_SHARE *share= table_arg->s;
			pos = table_arg->key_info;
			_FileNames names;
			std::string path = table_arg->s->path.str;
			names.push_back(path);
			std::string extension = TREESTORE_FILE_EXTENSION;
			for (i= 0; i < share->keys; i++,pos++){//all the indexes in the table ?
				std::string name = path;
				name += INDEX_SEP();
				name += pos->name;
				//name += extension;
				names.push_back(name);
			}
			for (Field **field=share->field ; *field ; field++){
				std::string name = path;
				name += TABLE_SEP();
				name += (*field)->field_name;
				//name += extension;
				names.push_back(name);
			}

			return names;
		}
		void calc_density(TABLE *table_arg){
				uint i, j;
			KEY *pos;
			TABLE_SHARE *share= table_arg->s;
			pos = table_arg->key_info;
			std::string path = share->path.str;
			for (i= 0; i < share->keys; i++,pos++){//all the indexes in the table ?
				std::string index_name = path + INDEX_SEP() + pos->name;

				stored::index_interface::ptr index = (*this).indexes[i];

				for (j= 0; j < pos->usable_key_parts; j++){
					Field *field = pos->key_part[j].field;// the jth field in the key
					nst::u32 fi=field->field_index;
					if(j == 0){
						index->push_density((*this).cols[fi]->get_rows_per_key());
					}else{
						/*const _Rid sample = std::min<_Rid>(_row_count, 10000);
						typedef std::set<CompositeStored> _Uniques;
						_Uniques uniques;
						CompositeStored ir;
						for(_Rid row = 0; row < sample; ++row){
							for(_Parts::iterator p = index->parts.begin(); p != index->parts.end(); ++p){
								(*this).cols[(*p)]->compose(row, ir);
							}
							uniques.insert(ir);
							ir.clear();
						}
						_Rid d = sample / uniques.size();
						if(d > 1) d /= 2;*/
						stored::_Rid d = 1;
						index->push_density(d);
					}
				}
			}
		}
		void load_indexes(TABLE *table_arg){
			uint i, j;
			KEY *pos;
			TABLE_SHARE *share= table_arg->s;
			pos = table_arg->key_info;
			std::string path = share->path.str;
			for (i= 0; i < share->keys; i++,pos++){//all the indexes in the table ?
				std::string index_name = path + INDEX_SEP() + pos->name;
				stored::index_interface::ptr index = nullptr;
				bool field_primitive =false;
				if(treestore_use_primitive_indexes && pos->usable_key_parts==1){
					field_primitive = true;
					Field **field = &(pos->key_part[0].field);// the jth field in the key
					ha_base_keytype bt = (*field)->key_type();
					nst::u32 fi = (*field)->field_index;
					switch(bt){
						case HA_KEYTYPE_END:
							// ERROR ??
							break;
						case HA_KEYTYPE_FLOAT:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::FloatStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

							break;
						case HA_KEYTYPE_DOUBLE:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::DoubleStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

							break;
						case HA_KEYTYPE_SHORT_INT:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::ShortStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

							break;
						case HA_KEYTYPE_LONG_INT:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::IntStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

							break;
						case HA_KEYTYPE_USHORT_INT:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::UShortStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

							break;
						case HA_KEYTYPE_ULONG_INT:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::ULongIntStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

							break;
						case HA_KEYTYPE_LONGLONG:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::LongIntStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

							break;
						case HA_KEYTYPE_ULONGLONG:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::ULongIntStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

							break;
						case HA_KEYTYPE_INT24:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::IntStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );
							break;
						case HA_KEYTYPE_UINT24:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::UIntStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

							break;
						case HA_KEYTYPE_INT8:
							index = new tree_index<stored::PrimitiveDynamicKey<stored::CharStored>>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

							break;
						case HA_KEYTYPE_BIT:
						case HA_KEYTYPE_NUM:
						case HA_KEYTYPE_TEXT:
						case HA_KEYTYPE_BINARY:
						case HA_KEYTYPE_VARTEXT1:
						case HA_KEYTYPE_VARTEXT2:
						case HA_KEYTYPE_VARBINARY1:
						case HA_KEYTYPE_VARBINARY2:
						default:
							field_primitive = false;
							break; //do nothing pass
					}//switch


				}

				if(!field_primitive){
					index = new tree_index<stored::DynamicKey>(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );
				}

				index->set_col_index(i);
				index->set_fields_indexed (pos->usable_key_parts);
				for (j= 0; j < pos->usable_key_parts; j++){
					Field *field = pos->key_part[j].field;// the jth field in the key
					index->push_part(field->field_index);

				}

				get_indexes().push_back(index);
			}
		}
		void delete_extendsions(){

		}
	protected:
		_FileNames file_names;
		stored::_Rid _row_count;
		int locks;
		nst::u64 last_lock_time;
		nst::u64 last_unlock_time;
		collums::_LockedRowData* row_datas;

	public:
		nst::u32 get_col_count() const {
			return cols.size();
		}
		collums::_LockedRowData* get_row_datas(){
			return row_datas;
		}
		tree_table(TABLE *table_arg)
		:	changed(false)
		,	_row_count(0)
		,	last_lock_time(os::micros())
		,	last_unlock_time(os::micros())
		,	row_datas(nullptr)
		,	storage(table_arg->s->path.str)
		,	locks(0)
		,	table(nullptr)		
		{
			{
				nst::synchronized sync(shared_lock);
				std::shared_ptr<shared_data> sshared;
				if(shared.count(storage.get_name())==0){
					shared[storage.get_name()] = std::make_shared<shared_data>();
				}
				sshared = shared[storage.get_name()];
				(*this).share = sshared.get();
			}
			load(table_arg);
			_row_count = 0;
			rollback();
		}
		~tree_table(){
			clear();
			delete_extendsions();
		}
		_Conditional conditional;
		abstract_conditional_iterator::ptr rcond;
		CompositeStored temp;
		_Indexes indexes;
		_Collumns cols;
		stored::abstracted_storage storage;
		_TableMap * table;
		shared_data * share;

		_TableMap& get_table(){

			if(nullptr==table)
				table = new _TableMap(storage);
			return *table;

		}

		const _TableMap& get_table() const {
			if(nullptr==table)
				throw InvalidTablePointer();
			return *table;

		}

		void begin_table(){
			stored::abstracted_tx_begin(!changed, storage, get_table());
			init_rowcount();

		}

		void rollback_table(){
			commit_table1();
			storage.rollback();

		}

		void commit_table1(){
			if(changed){
				get_table().reduce_use();
			}
		}

		void commit_table2(){
			if(changed){
				storage.commit();
			}
		}
		
		bool is_transacted() const {
			return storage.is_transacted();
		}

		void begin(bool read){
			changed = !read;
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				(*x)->begin(read);
			}
			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->begin(read);
			}
			begin_table();
		}

		void commit1(){
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				(*x)->commit1_asynch();
			}
			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->commit1_asynch();

			}
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				(*x)->commit1();
			}
			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->commit1();

			}
			commit_table1();
		}

		void commit2(){
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				(*x)->commit2();
			}
			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->commit2();

			}
			commit_table2();
			changed = false;
		}

		void rollback(){
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				(*x)->rollback();

			}
			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->rollback();
			}
			rollback_table();
		}

		void clear(){

			rollback();

			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				delete (*x);

			}

			indexes.clear();
			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				delete (*c);

			}
			cols.clear();
			if(nullptr != table)
				delete table;
			storage.close();
			table = nullptr;
			changed = false;
		}

		const _FileNames &get_file_names() const {
			return file_names;
		}

		_Indexes& get_indexes(){
			return indexes;
		};

		void check_load(TABLE *table_arg){
			if(cols.empty()){
				load(table_arg);
			}

		}

		void load(TABLE *table_arg){
			clear();
			printf("load table %s\n", table_arg->alias);
			(*this).load_indexes(table_arg);
			(*this).load_cols(table_arg);
			(*this).calc_density(table_arg);
			file_names = create_file_names(table_arg);
		}

		void reduce_use(){
			//printf("reducing cols+index use\n");

			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){

				(*x)->reduce_use();

			}

			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->reduce_col_use();

			}

			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->reduce_cache_use();

			}
			get_table().reduce_use();

		}

		void reduce_col_use(){

			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				(*x)->clear_cache();
			}

			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->reduce_col_use();
			}

			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->reduce_cache_use();

			}
		}
		void reduce_use_indexes(){
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				(*x)->clear_cache();
			}
		}
		void reduce_use_collum_trees_only(){

			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->reduce_col_use();
			}

		}
		void reduce_use_collum_trees(){

			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->reduce_col_use();
			}
			get_table().reduce_use();
		}

		void reduce_use_collum_caches(){
			/// reduce on table granularity

			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->reduce_cache_use();
			}

		}

		void check_use(){

			reduce_use();

		}
		void reduce_cache_use(){
			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->reduce_cache_use();

			}
		}
		inline const CompositeStored& get_index_query() const {
			return temp;
		}

		/// returns the rows per key for a given collumn

		nst::u32 get_rows_per_key(nst::u32 i, nst::u32 part) const {
			if(indexes.size() > i && indexes[i]->densities() > part){
				return indexes[i]->density_at(part);
			}
			return 1;
		}

		/// this function does not enumerate all covering sets
		/// but it will not give false positives

		bool read_set_covered_by_index
		(	TABLE* table
		,	uint ax
		,	_Selection& s
		){
			KEY & ki = table->key_info[ax];
			KEY_PART_INFO * pi = ki.key_part;

			size_t at = 0;
			for (size_t i = 0; i < ki.usable_key_parts;++i,++pi){
				Field *field= pi->field;
				if(bitmap_is_set(table->read_set, (*field).field_index)){
					++at;
				}
			}
			return
			(	at == s.size() //first: the full read_set has to be covered
			&& 	at <= ki.usable_key_parts // this condition will probably not be reached
			);

		}
		stored::_Rid key_to_rid
		(	TABLE* table
		,	uint ax
		,	const CompositeStored& input
		)
		{

			return input.row;
		}
		
		void pop_condition(){
			rcond = nullptr;
		}
		void set_root_condition(abstract_conditional_iterator::ptr rcond){
			(*this).rcond = rcond;
		}
		logical_conditional_iterator::ptr create_and_condition(){
			return std::make_shared<conditional_and_iterator>();
		}

		logical_conditional_iterator::ptr create_or_condition(){
			return std::make_shared<conditional_or_iterator>((*this)._row_count);
		}

		abstract_conditional_iterator::ptr create_field_condition(const Item_field * fi,const Item_func * fun,const Item * val){

			return cols[fi->field->field_index]->create_condition( fun, val, fi->field);
		}
		bool evaluate_conditions(_Rid row){
			if(rcond == nullptr) return true;/// always true if there are no conditions
			/// very under optimized algorithm
			
			if(rcond->evaluate(row) != row){
				return false;
			}
			
			return true;
		}
		/// return value is either valid or equal to NoRecord
		_Rid iterate_conditions(_Rid row){
			if(rcond == nullptr) return row;/// always true if there are no conditions			
			_Rid r = row;
			while(r <= (*this)._row_count){
				r = rcond->iterate(r) ;				
				if(!(r <= (*this)._row_count))
					return abstract_conditional_iterator::NoRecord;
				if(!is_allocated_row(r)){
					++r;
				}else {
					return r;
				}
			}
			return abstract_conditional_iterator::NoRecord ;
			
		}
		bool is_allocated_row(_Rid row){
			/// TODO: check allocation map 2
			return row < (*this)._row_count;
		}

		void pop_all_conditions(){
			rcond = nullptr;			
		}
		uint count_parts(TABLE* table,uint ax, uint key_part_map){
			KEY & ki = table->key_info[ax];
			uint r = 0;
			for (size_t i = 0; i < ki.usable_key_parts;++i){
				if((key_part_map & (1ul<<i)) != 0){
					++r;
				}
			}
			return r;
		}
		private:
		void _compose_query_nb
		(	TABLE* table
		,	stored::_Rid bound
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_part_map
		){

			KEY & ki = table->key_info[ax];
			KEY_PART_INFO * pi = ki.key_part;
			size_t at = 0;
			const uchar * ptr = key ? key:NULL;
			temp.clear();
			if(key==NULL) return;

			for (size_t i = 0; i < ki.usable_key_parts;++i,++pi){
				uint store_length= pi->store_length;

				/// uint part_length= std::min<uint>(store_length, key_l);
				if((key_part_map & (1ull<<i)) != 0){
					Field *field= pi->field;
					uchar * okey = field->ptr;
					if ((pi->key_part_flag & HA_BLOB_PART)||(pi->key_part_flag & HA_VAR_LENGTH_PART)){
						cols[field->field_index]->compose(temp, field, ptr, conversions::f_use_var_header);
					}else{

						field->ptr = (uchar *)ptr;
						if(pi->null_bit)
							field->ptr++;
						cols[field->field_index]->compose(temp, field, ptr, 0);

					}
					field->ptr = okey;
				}else{
					//if(bound==0xffffffff){
					//	temp.addInf();
					//}else
					//	temp.add();
				}
				ptr += store_length;
				at++;
			}


		}
		void _compose_query
		(	TABLE* table
		,	stored::_Rid bound
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_part_map
		){

			_compose_query_nb(table, bound, ax, key, key_l, key_part_map);
			temp.row = bound;
		}
		public:
		bool check_key_match
		(	const CompositeStored& current
		,	TABLE* table
		,	uint ax
		,	uint parts_map
		){
			/// KEY & ki = table->key_info[ax];
			check_load(table);
			return current.left_equal_key(temp);

		}

		bool check_key_match2
		(	const CompositeStored& current
		,	TABLE* table
		,	uint ax
		,	uint parts_map
		){
			check_load(table);
			KEY & ki = table->key_info[ax];
			if(count_parts(table, ax, parts_map) < ki.usable_key_parts){
				return current.left_equal_key(temp);
			}
			return current.left_equal_key(temp);

		}

		const CompositeStored* predict_sequential
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		,	stored::index_iterator_interface & out

		){
			check_load(table);
			stored::index_interface::ptr ix =  indexes[ax];
			if(!key_map || key==NULL){
				ix->first(out);
				return NULL;
			}
			_compose_query_nb(table, 0ull, ax, key, key_l, key_map);

			if( ix->is_unique() && treestore_predictive_hash == TRUE ){

				return ix->predict(out, temp);

			}
			return NULL;
		}

		void from_initializer
		(	stored::index_iterator_interface & out
		,	uint ax
		,	const stx::initializer_pair& ip)
		{
			indexes[ax]->from_initializer(out,ip);
		}


		void compose_query_find
		(	stored::index_iterator_interface& out
		,	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map

		){
			check_load(table);
			_compose_query(table, 0ull, ax, key, key_l, key_map);

			indexes[ax]->find(out, temp);

		}


		void compose_query_upper
		(	stored::index_iterator_interface& r
		,	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l

		,	uint key_map
		){

			check_load(table);
			if(!key_map || key==NULL){
				indexes[ax]->end(r);
				return ;
			}
			_compose_query(table, 0xFFFFFFFFul, ax, key, key_l, key_map);
			indexes[ax]->upper(r, temp);

		}

		void compose_query_upper_r
		(	stored::index_iterator_interface& r
		,	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		){
			check_load(table);

			if(!key_map || key==NULL){
				indexes[ax]->end(r);
				return ;
			}
			_compose_query(table, 0xFFFFFFFFul, ax, key, key_l, key_map);
			indexes[ax]->upper(r,temp);

		}

		void compose_query_lower
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		,	stored::index_iterator_interface& out
		){
			check_load(table);
			if(!key_map || key==NULL){
				indexes[ax]->first(out);
				return ;
			}

			_compose_query(table, 0ull, ax, key, key_l, key_map);

			indexes[ax]->lower_(out, temp);
		}

		void temp_lower
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		,	stored::index_iterator_interface& out
		){
			check_load(table);
			stored::index_interface::ptr ix =  indexes[ax];
			if(!key_map || key==NULL){
				ix->first(out);
				return ;
			}

			ix->lower_(out, temp);
			if(!changed){
				if(ix->is_unique() && out.valid()){
					ix->cache_it(out);
				}
			}

		}

		void compose_query_lower_r
		(	stored::index_iterator_interface& r
		,	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		){
			check_load(table);

			if(!key_map || key==NULL){
				indexes[ax]->first(r);

			}else{
				_compose_query(table, 0ull, ax, key, key_l, key_map);
				indexes[ax]->lower(r, temp);
			}

		}

		void compose_query_find
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		,	stored::index_iterator_interface& out
		){
			check_load(table);
			if(!key_map){
				indexes[ax]->end(out);
				return ;
			}

			_compose_query(table, 0ull, ax, key, key_l, key_map);
			indexes[ax]->find(out, temp);
		}

		nst::u64 get_last_lock_time() const {
			return last_lock_time;
		}

		nst::u64 get_last_unlock_time() const {
			return last_unlock_time;
		}

		void init_rowcount(){
			_TableMap::iterator t = get_table().end();
			_row_count = 0;
			if(!get_table().empty()){
				--t;
				_row_count = t.key().get_value();
				++_row_count;
			}
			if(false){
				(*this).row_datas = collums::get_locked_rows(storage.get_name());
				if(get_row_datas()->rows.size() < _row_count){
					nst::synchronized sr(get_row_datas()->lock);
					get_row_datas()->rows.resize(_row_count);
				}
			}
		}

		/// aquire lock and resources
		void lock(bool writer)
		{
			if(locks++ == 0){
				changed = writer;
				if(changed)
					 (*this).share->last_write_lock_time = ::os::millis();
				last_lock_time = os::micros();
				begin(!changed);
			}

		}

		static const nst::u64 READER_ROLLBACK_THRESHHOLD = 10000ll;/// in millis

		private:



		public:
		/// release locks and resources
		void unlock(Poco::Mutex* p2_lock)
		{
			if(locks==0){
				printf("too many unlocks \n");
				return;
			}

			if(--locks == 0){
				if(changed)
				{
					commit1();
					/// locks so that all the storages can commit atomically allowing
					/// other transactions to start on the same version
					synchronized _s(*p2_lock);

					commit2();
					if(calc_total_use() > treestore_max_mem_use){
						stored::reduce_all();
					}
					changed = false;
				}else
				{
					bool rolling = false; //treestore_reduce_tree_use_on_unlock;
					if
					(	::os::millis() - (*this).share->last_write_lock_time < READER_ROLLBACK_THRESHHOLD
						||	calc_total_use() > treestore_max_mem_use*0.7f
						||  rolling
					)
						rollback();/// relieves the version load when new data is added to the collums
				}
				last_unlock_time = os::micros();
			}
		}

		void write(TABLE* table){
			check_load(table);
			if(!changed)
			{
				printf("table not locked for writing\n");
				return;
			}

			/// TABLE_SHARE *share= table->s;
			for (Field **field=table->field ; *field ; field++){
				if(bitmap_is_set(table->write_set, (*field)->field_index)){
					if(!(*field)->is_null() && cols[(*field)->field_index]){
						cols[(*field)->field_index]->add_row(_row_count, *field);
					}
				}
			}
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				temp.clear();
				for(stored::_Parts::iterator p = (*x)->parts.begin(); p != (*x)->parts.end(); ++p){
					cols[(*p)]->compose(temp);
				}
				temp.row = _row_count;
				(*x)->add(temp);
			}

			(*this).get_table().insert(_row_count, '0');
			if(_row_count % 300000 == 0){
				printf("%lld rows added to %s\n", (nst::lld)_row_count, table->s->table_name.str);
				//reduce_col_use();
				//reduce_use();
			}
			_row_count++;
		}

		void erase(stored::_Rid rid, TABLE* table){
			check_load(table);
			if(!changed)
			{
				//printf("table not locked for writing\n");
				return;
			}
			erase_row_index(rid);
			(*this).get_table().erase(rid);
			/// TABLE_SHARE *share= table->s;
			for (Field **field=table->field ; *field ; field++){
				if(!(*field)->is_null() && cols[(*field)->field_index]){
					cols[(*field)->field_index]->erase_row(rid);
				}
			}

		}

		void write(stored::_Rid rid, TABLE* table){
			/// erase_row_index must be called before this function
			check_load(table);
			if(!changed)
			{
				//printf("table not locked for writing\n");
				return;
			}

			/// TABLE_SHARE *share= table->s;
			for (Field **field=table->field ; *field ; field++){
				if(bitmap_is_set(table->write_set, (*field)->field_index)){
					if(!(*field)->is_null() && cols[(*field)->field_index]){
						cols[(*field)->field_index]->add_row(rid, *field);
					}
				}
			}
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				temp.clear();
				for(stored::_Parts::iterator p = (*x)->parts.begin(); p != (*x)->parts.end(); ++p){
					cols[(*p)]->compose(rid, temp);
				}
				//temp.add(rid);
				temp.row = rid;
				(*x)->add(temp);
			}

		}

		void erase_row_index(stored::_Rid rid){
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				temp.clear();
				for(stored::_Parts::iterator p = (*x)->parts.begin(); p != (*x)->parts.end(); ++p){
					cols[(*p)]->compose(rid, temp);
				}
				//temp.add(rid);
				temp.row = rid;
				(*x)->remove(temp);
			}
		}

		stored::_Rid row_count() const {
			return get_table().size();
		}

		stored::_Rid table_size() const {
			stored::_Rid r = 0;
			for(_Collumns::const_iterator c = cols.begin();c != cols.end(); ++c){
				r += (*c)->field_size();
			}
			return row_count()*r;
		}
		stored::index_interface* get_index_interface(int at){
			return indexes[at];
		}
		const stored::index_interface* get_index_interface(int at) const {
			return indexes[at];
		}
		_Selection create_output_selection(TABLE* table){
			check_load(table);
			_Selection r;

			/// TABLE_SHARE *share= table->s;
			uint selected = 0;
			uint total = 0;
			for (Field **field=table->field ; *field ; field++){
				if(bitmap_is_set(table->read_set, (*field)->field_index)){
					selected++;
				}
				total++;
			}
			bool by_tree = (total > 4 && (selected > total/4 || selected > 16));
			for (Field **field=table->field ; *field ; field++){

				if(bitmap_is_set(table->read_set, (*field)->field_index)){
					selection_tuple selection;
					selection.col = cols[(*field)->field_index];
					selection.col->initialize(by_tree);

					selection.field = (*field);
					//selection.iter = selection.col->create_iterator(selection.col->stored_rows());
					//selection.iter->set_by_cache();
					selection.save_ptr();
					r.push_back(selection);

				}
			}
			return r;
		}

		_Selection create_input_selection(TABLE* table){
			_Selection r;

			/// TABLE_SHARE *share= table->s;

			for (Field **field=table->field ; *field ; field++){
				if(bitmap_is_set(table->write_set, (*field)->field_index)){
					selection_tuple selection;
					selection.col = cols[(*field)->field_index];
					selection.field = (*field);
					//selection.iter = selection.col->create_iterator(_row_count);
					r.push_back(selection);
				}
			}
			return r;
		}

		typedef tree_table * ptr;
	};

};

#endif
