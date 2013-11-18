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
typedef std::vector<std::string> _FileNames;
namespace tree_stored{
	class InvalidTablePointer : public std::exception{
	public:
		InvalidTablePointer () throw(){
		}
	};

	class abstract_my_collumn {
	public:
		abstract_my_collumn(){
		}
		virtual ~abstract_my_collumn(){
		}
		virtual void add_row(collums::_Rid row, Field * f) = 0;
		virtual void erase_row(collums::_Rid row) = 0;
		virtual NS_STORAGE::u32 field_size() const = 0;
		virtual _Rid stored_rows() const = 0;
		virtual void initialize(bool by_tree) = 0;
		virtual void compose(CompositeStored& comp)=0;
		virtual void compose(_Rid r, CompositeStored& comp)=0;
		virtual void compose(CompositeStored & to, Field* f,const uchar * n_ptr, uint flags)=0;
		virtual bool equal(_Rid row, Field* f)=0;
		virtual void reduce_use() = 0;
		virtual void seek_retrieve(_Rid row, Field* f) = 0;
		virtual nst::u32 get_rows_per_key() = 0;
		virtual void flush() = 0;
		virtual void begin(bool read) = 0;
		virtual void commit1() = 0;
		virtual void commit2() = 0;
		virtual void rollback() = 0;
	};

	template <typename _Fieldt>
	class my_collumn : public abstract_my_collumn{
	protected:
		typedef typename collums::collumn<_Fieldt> _Colt;
		_Colt col;
		_Fieldt temp;
		conversions convertor;

	public:

		my_collumn(std::string name) : col(name){

		}
		virtual ~my_collumn(){
		}

		virtual void add_row(collums::_Rid row, Field * f){
			convertor.fget(temp, f, NULL, 0);
			col.add(row, temp);
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

		virtual void compose(_Rid r, CompositeStored & to){
			const _Fieldt& field = col.seek_by_tree(r);
			to.add(field);
		}

		virtual void compose(CompositeStored & to, Field* f, const uchar * ptr, uint flags){
			//convertor.fget(temp, f, ptr, flags);
			//to.add(temp.get_value());// TODO: due to this interface, BLOBS are impossible in indexes
			convertor.fadd(to,temp,f,ptr,flags);
		}

		virtual bool equal(_Rid row, Field* f){
			convertor.fget(temp, f, NULL, 0);
			const _Fieldt& t = col.seek_by_cache(row);
			return (t==temp);
		}

		virtual void seek_retrieve(_Rid row, Field* f) {
			const _Fieldt& t = col.seek_by_cache(row);
			f->set_notnull();
			convertor.fset(f, t);
		}

		virtual _Rid stored_rows() const {
			return col.get_rows();
		}

		virtual NS_STORAGE::u32 field_size() const {
			return sizeof(_Fieldt)/2;
		}

		virtual void flush() {
			col.flush();
		}
		virtual void commit1() {
			col.commit1();
		}

		virtual void commit2() {
			col.commit2();
		}

		virtual void begin(bool read) {
			col.tx_begin(read);
		}
		virtual void rollback() {
			col.rollback();
		}
		virtual void reduce_use() {
			col.reduce_use();
		}
	};

	typedef std::vector<tree_index::ptr> _Indexes;
	typedef std::vector<abstract_my_collumn*> _Collumns;
	struct selection_tuple{
		selection_tuple()
		:	col(NULL)
		,	field(NULL)
		{
		}

		selection_tuple(const selection_tuple& right)
		:	col(NULL)
		,	field(NULL)
		{
			*this = right;
		}

		selection_tuple& operator=(const selection_tuple& right){
			col = right.col;
			field = right.field;

			return *this;
		}
		abstract_my_collumn * col;
		Field * field;

	};

	typedef std::vector<selection_tuple> _Selection;
	class tree_table{
	public:
		typedef stored::IntTypeStored<_Rid> _StoredRowId;
		typedef stored::IntTypeStored<unsigned char> _StoredRowFlag;
		typedef stx::btree_map<_StoredRowId, _StoredRowFlag, stored::abstracted_storage> _TableMap;
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

			for (Field **field=share->field ; *field ; field++){
				ha_base_keytype bt = (*field)->key_type();
				switch(bt){
					case HA_KEYTYPE_END:
						// ERROR ??
						break;
					case HA_KEYTYPE_FLOAT:
						cols[(*field)->field_index] = new my_collumn<FloatStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_DOUBLE:
						cols[(*field)->field_index] = new my_collumn<DoubleStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_SHORT_INT:
						cols[(*field)->field_index] = new my_collumn<ShortStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_LONG_INT:
						cols[(*field)->field_index] = new my_collumn<IntStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_USHORT_INT:
						cols[(*field)->field_index] = new my_collumn<UShortStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_ULONG_INT:
						cols[(*field)->field_index] = new my_collumn<ULongIntStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_LONGLONG:
						cols[(*field)->field_index] = new my_collumn<LongIntStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_ULONGLONG:
						cols[(*field)->field_index] = new my_collumn<ULongIntStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_INT24:
						cols[(*field)->field_index] = new my_collumn<IntStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_UINT24:
						cols[(*field)->field_index] = new my_collumn<UIntStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_INT8:
						cols[(*field)->field_index] = new my_collumn<CharStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_BIT:
						cols[(*field)->field_index] = new my_collumn<BlobStored>(path+TABLE_SEP()+(*field)->field_name);

						break;

					case HA_KEYTYPE_NUM:			/* Not packed num with pre-space */
					case HA_KEYTYPE_TEXT:			/* Key is sorted as letters */
						cols[(*field)->field_index] = new my_collumn<VarCharStored>(path+TABLE_SEP()+(*field)->field_name);
						break;

					case HA_KEYTYPE_BINARY:			/* Key is sorted as unsigned chars */
						cols[(*field)->field_index] = new my_collumn<BlobStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					/* Varchar (0-255 bytes) with length packed with 1 byte */
					case HA_KEYTYPE_VARTEXT1:               /* Key is sorted as letters */
					/* Varchar (0-65535 bytes) with length packed with 2 bytes */
					case HA_KEYTYPE_VARTEXT2:		/* Key is sorted as letters */
						cols[(*field)->field_index] = new my_collumn<VarCharStored>(path+TABLE_SEP()+(*field)->field_name);
						break;
					case HA_KEYTYPE_VARBINARY1:             /* Key is sorted as unsigned chars length packed with 1 byte*/
						/* Varchar (0-65535 bytes) with length packed with 2 bytes */
					case HA_KEYTYPE_VARBINARY2:		/* Key is sorted as unsigned chars */
						cols[(*field)->field_index] = new my_collumn<BlobStored>(path+TABLE_SEP()+(*field)->field_name);
						break;

					default:
						//TODO: ERROR ??
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
			for (i= 0; i < share->keys; i++,pos++){//all the indexes in the table ?
				std::string name = path;
				name += INDEX_SEP();
				name += pos->name;
				names.push_back(name);
			}
			for (Field **field=share->field ; *field ; field++){
				std::string name = path;
				name += TABLE_SEP();
				name += (*field)->field_name;
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

				tree_index::ptr index = (*this).indexes[i];

				for (j= 0; j < pos->usable_key_parts; j++){
					Field *field = pos->key_part[j].field;// the jth field in the key
					nst::u32 fi=field->field_index;
					if(j == 0){
						index->density.push_back((*this).cols[fi]->get_rows_per_key());
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
						_Rid d = 1;
						index->density.push_back(d);
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

				tree_index::ptr index = new tree_index(index_name, ( pos->flags & (HA_NOSAME|HA_UNIQUE_CHECK) ) != 0 );

				index->ix = i;
				index->fields_indexed = pos->usable_key_parts;
				for (j= 0; j < pos->usable_key_parts; j++){
					Field *field = pos->key_part[j].field;// the jth field in the key
					index->parts.push_back(field->field_index);

				}

				get_indexes().push_back(index);
			}
		}
		void delete_extendsions(){

		}
	protected:
		_FileNames file_names;
		_Rid _row_count;
		int locks;

	public:


		tree_table(TABLE *table_arg)
		:	changed(false)
		,	_row_count(0)
		,	storage(table_arg->s->path.str)
		,	table(nullptr)
		{
			load(table_arg);
			_row_count = 0;
		}
		~tree_table(){
			clear();
			delete_extendsions();
		}

		CompositeStored temp;
		_Indexes indexes;
		_Collumns cols;
		stored::abstracted_storage storage;
		_TableMap * table;

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
			(*this).load_indexes(table_arg);
			(*this).load_cols(table_arg);
			(*this).calc_density(table_arg);
			file_names = create_file_names(table_arg);
		}

		void reduce_use(){
			//printf("reducing cols+index use\n");

			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				(*x)->clear_cache();

			}

			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){

				(*x)->index.reduce_use();

			}

			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->reduce_use();

			}
			get_table().reduce_use();

		}

		void reduce_col_use(){

			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				(*x)->clear_cache();
			}
			for(_Collumns::iterator c = cols.begin(); c!=cols.end();++c){
				(*c)->reduce_use();

			}
		}
		void check_use(){

			reduce_use();

		}

		inline const CompositeStored& get_index_query() const {
			return temp;
		}

		/// returns the rows per key for a given collumn

		nst::u32 get_rows_per_key(nst::u32 i, nst::u32 part) const {
			if(indexes.size() > i && indexes[i]->density.size() > part){
				return indexes[i]->density[part];
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
		_Rid key_to_rid
		(	TABLE* table
		,	uint ax
		,	const CompositeStored& input
		)
		{

			return input.row;
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
		void _compose_query_nb
		(	TABLE* table
		,	_Rid bound
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
		,	_Rid bound
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_part_map
		){

			_compose_query_nb(table, bound, ax, key, key_l, key_part_map);
			temp.row = bound;
		}

		bool check_key_match
		(	const CompositeStored& current
		,	TABLE* table
		,	uint ax
		,	uint parts_map
		){
			/// KEY & ki = table->key_info[ax];
			return current.left_equal_key(temp);

		}

		bool check_key_match2
		(	const CompositeStored& current
		,	TABLE* table
		,	uint ax
		,	uint parts_map
		){
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
		,	IndexIterator & out

		){
			tree_index::ptr ix =  indexes[ax];
			if(!key_map || key==NULL){
				out = ix->index.first();
				return NULL;
			}
			_compose_query_nb(table, 0ull, ax, key, key_l, key_map);

			if( ix->unique ){

				return ix->predict(out, temp);

			}
			return NULL;
		}

		void from_initializer
		(	IndexIterator & out
		,	uint ax
		,	const IndexIterator::initializer_pair& ip)
		{
			indexes[ax]->index.from_initializer(out,ip);
		}


		IndexIterator compose_query_find
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map

		){
			_compose_query(table, 0ull, ax, key, key_l, key_map);
			return indexes[ax]->index.find(temp);
		}


		IndexIterator compose_query_upper
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l

		,	uint key_map
		){
			if(!key_map || key==NULL){
				return indexes[ax]->index.end();
			}
			_compose_query(table, 0xFFFFFFFFul, ax, key, key_l, key_map);
			return indexes[ax]->index.upper(temp);
		}

		IndexIterator compose_query_upper_r
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		){
			if(!key_map || key==NULL){
				return indexes[ax]->index.end();
			}
			_compose_query(table, 0xFFFFFFFFul, ax, key, key_l, key_map);
			return indexes[ax]->index.upper(temp);
		}

		void compose_query_lower
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		,	IndexIterator & out
		){
			if(!key_map || key==NULL){
				out = indexes[ax]->index.first();
				return ;
			}

			_compose_query(table, 0ull, ax, key, key_l, key_map);

			indexes[ax]->index.lower_(out, temp);
		}

		void temp_lower
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		,	IndexIterator & out
		){
			tree_index::ptr ix =  indexes[ax];
			if(!key_map || key==NULL){
				out = ix->index.first();
				return ;
			}

			ix->index.lower_(out, temp);
			if(!changed){
				if(ix->unique && out.valid()){
					ix->cache_it(out);
				}
			}

		}
		IndexIterator compose_query_lower_r
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		){
			if(!key_map || key==NULL){
				return indexes[ax]->index.first();
			}

			_compose_query(table, 0ull, ax, key, key_l, key_map);
			return indexes[ax]->index.lower(temp);
		}

		void compose_query_find
		(	TABLE* table
		,	uint ax
		,	const uchar *key
		,	uint key_l
		,	uint key_map
		,	IndexIterator & out
		){
			if(!key_map){
				out = indexes[ax]->index.end();
				return ;
			}

			_compose_query(table, 0ull, ax, key, key_l, key_map);
			out = indexes[ax]->index.find(temp);
		}


		void init_rowcount(){
			_TableMap::iterator t = get_table().end();
			_row_count = 0;
			if(!get_table().empty()){
				--t;
				_row_count = t.key().get_value();
				++_row_count;
			}
		}

		/// aquire lock and resources
		void lock(bool writer)
		{
			changed = writer;

			begin(!changed);

		}

		/// release locks and resources
		void unlock(Poco::Mutex* p2_lock)
		{
			if(changed)
			{
				commit1();
				/// locks so that all the storages can commit atomically allowing
				/// other transactions to start on the same version
				synchronized _s(*p2_lock);
				commit2();
				changed = false;
			}else
			{
				rollback();
			}
		}
		void write(TABLE* table){
			if(!changed)
			{
				printf("table not locked for writing\n");
				return;
			}

			/// TABLE_SHARE *share= table->s;
			for (Field **field=table->field ; *field ; field++){
				if(!(*field)->is_null() && cols[(*field)->field_index]){
					cols[(*field)->field_index]->add_row(_row_count, *field);
				}
			}
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				temp.clear();
				for(_Parts::iterator p = (*x)->parts.begin(); p != (*x)->parts.end(); ++p){
					cols[(*p)]->compose(temp);
				}
				temp.row = _row_count;
				(*x)->index.add(temp, 0);
			}

			(*this).get_table().insert(_row_count, '0');
			if(_row_count % 300000 == 0){
				printf("%lld rows added to %s\n", (nst::lld)_row_count, table->s->table_name.str);
				//reduce_col_use();
				//reduce_use();
			}
			_row_count++;
		}
		void erase(_Rid rid, TABLE* table){
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
		void write(_Rid rid, TABLE* table){
			if(!changed)
			{
				//printf("table not locked for writing\n");
				return;
			}

			/// TABLE_SHARE *share= table->s;
			for (Field **field=table->field ; *field ; field++){
				if(!(*field)->is_null() && cols[(*field)->field_index]){
					cols[(*field)->field_index]->add_row(rid, *field);
				}
			}
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				temp.clear();
				for(_Parts::iterator p = (*x)->parts.begin(); p != (*x)->parts.end(); ++p){
					cols[(*p)]->compose(rid, temp);
				}
				//temp.add(rid);
				temp.row = rid;
				(*x)->index.add(temp, 0);
			}

		}
		void erase_row_index(_Rid rid){
			for(_Indexes::iterator x = indexes.begin(); x != indexes.end(); ++x){
				temp.clear();
				for(_Parts::iterator p = (*x)->parts.begin(); p != (*x)->parts.end(); ++p){
					cols[(*p)]->compose(rid, temp);
				}
				//temp.add(rid);
				temp.row = rid;
				(*x)->index.remove(temp);
			}
		}
		_Rid row_count() const {
			return get_table().size();
		}

		_Rid table_size() const {
			_Rid r = 0;
			for(_Collumns::const_iterator c = cols.begin();c != cols.end(); ++c){
				r += (*c)->field_size();
			}
			return row_count()*r;
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
