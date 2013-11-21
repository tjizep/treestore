#include "myi.h"

/* Copyright (c) 2012, Christiaan Pretorius. All rights reserved.


MySQL TreeStore Storage Engine

myi.cpp - MySQL Storage Engine

CREATE TABLE test_table (
id     int(20) NOT NULL auto_increment,
name   varchar(32) NOT NULL default '',
other  int(20) NOT NULL default '0',
PRIMARY KEY  (id),
KEY name (name),
KEY other_key (other))
ENGINE="TREESTORE"


*/


//#define ENABLED_DEBUG_SYNC
//#define NDEBUG
#define DBUG_OFF

#define MYSQL_DYNAMIC_PLUGIN

#define MYSQL_SERVER 1

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation                          // gcc: Class implementation
#endif


#define MYSQL_SERVER 1

#ifndef _MSC_VER
#include <cmath>
#define isfinite std::isfinite
#endif

#include "sql_priv.h"
#include "probes_mysql.h"
#include "key.h"                                // key_copy
#include "sql_plugin.h"
#include <m_ctype.h>
#include <my_bit.h>
#include <stdarg.h>

#include "sql_table.h"                          // tablename_to_filename
#include "sql_class.h"                          // THD
      // SSV

#include <limits>

#include <map>
#include <string>
#include "collumn.h"
typedef uchar byte;
ptrdiff_t MAX_PC_MEM = 1024ll*1024ll*1024ll*4ll;
namespace nst = NS_STORAGE;
static NS_STORAGE::u64 read_lookups =0;
NS_STORAGE::u64 hash_hits =0;
NS_STORAGE::u64 hash_predictions =0;
static NS_STORAGE::u64 last_read_lookups ;
static NS_STORAGE::u64 total_cache_size=0;
static NS_STORAGE::u64 ltime = 0;
static Poco::Mutex plock;
static Poco::Mutex p2_lock;
void print_read_lookups(){
	return;
	if(os::millis()-ltime > 1000){
		stx::storage::scoped_ulock ul(plock);
		if(os::millis()-ltime > 1000){
			if(ltime){
				printf
				(   "read_lookups %lld/s hh %lld hp %lld (total: %lld - btt %.4g MB in %lld trees)\n"
				,   (nst::lld)read_lookups-std::min<NS_STORAGE::u64>(read_lookups, last_read_lookups)
				,   (nst::lld)hash_hits
				,   (nst::lld)hash_predictions
				,   (nst::lld)read_lookups
				,   (double)btree_totl_used/(1024.0*1024.0)
				,   (nst::lld)btree_totl_instances
				);
			}
			last_read_lookups = read_lookups;
			hash_hits = 0;
			hash_predictions = 0;
			ltime = ::os::millis();
		}
	}
}
#include "tree_stored.h"
#include "conversions.h"
#include "tree_index.h"
#include "tree_table.h"
typedef std::map<std::string, _FileNames > _Extensions;

// w.t.f.
// The handlerton asks for extensions when the table defs are already destroyed 
static _Extensions save_extensions;

namespace tree_stored{

	typedef std::map<std::string, tree_table::ptr> _Tables;
	class tree_thread{
	protected:
		int locks;
		bool changed;
		Poco::Thread::TID created_tid;
	public:
		int get_locks() const {
			return locks;
		}
		tree_thread() : locks(0),changed(false){
			created_tid = Poco::Thread::currentTid();
			printf("tree thread %ld created\n", created_tid);
		}
		Poco::Thread::TID get_created_tid() const {
			return (*this).created_tid;
		}
		~tree_thread(){
			clear();
			printf("tree thread removed\n");

		}
		_Tables tables;
		void modify(){
			changed = true;
		}
		bool is_writing() const {
			return changed;
		}

		tree_table::ptr compose_table(TABLE *table_arg){
			tree_table::ptr t = tables[table_arg->s->path.str];
			if(t == NULL ){

				/// TABLE_SHARE *share= table_arg->s;
				//uint options= share->db_options_in_use;

				t = new tree_table(table_arg);
				tables[table_arg->s->path.str] = t;
				save_extensions[table_arg->s->path.str] = t->get_file_names();
			}else{

			}

			return t;
		}
		void clear(){
			for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
				delete (*t).second;
			}
			printf("cleared %lld tables\n", (NS_STORAGE::lld)tables.size());
			tables.clear();
		}
		tree_table * lock(TABLE *table_arg, bool writer){
			tree_table * result = NULL;
			synchronized _s(p2_lock);
			result = compose_table(table_arg);
			result->lock(writer);
			if(!locks){
				hash_hits = 0;
				read_lookups = 0;

			}
			++locks;
			return result;
		}
		void release(TABLE *table_arg){
			compose_table(table_arg)->unlock(&p2_lock);
			if(1==locks){
				if(changed)
					NS_STORAGE::journal::get_instance().synch(); /// synchronize to storage
				check_use();
				print_read_lookups();
				changed = false;
			}
			--locks;

		}
		void reduce_tables(){
			if(!locks){
				printf("reducing idle thread %.4g MiB\n",(double)stx::storage::total_use/(1024.0*1024.0));
				for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
					(*t).second->check_use();
				}
			}

		}
		void check_use(){
			if(NS_STORAGE::total_use+btree_totl_used > MAX_EXT_MEM){
				if(!locks){
					for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
						(*t).second->check_use();
					}
				}
				DBUG_PRINT("info",("reducing block storage %.4g MiB\n",(double)stx::storage::total_use/(1024.0*1024.0)));
				stored::reduce_all();

			}
		}
	};
}

static handlerton *static_treestore_hton = NULL;

class static_threads{
public:
	typedef std::vector<tree_stored::tree_thread *> _Threads;
private:
	_Threads threads;
	Poco::Mutex tlock;
public:

	void release_thread(tree_stored::tree_thread * tt){
		if(tt==NULL){
			printf("Invalid argument to release thread\n");
			return ;
		}
		NS_STORAGE::scoped_ulock ul(tlock);

		for(_Threads::iterator t = threads.begin();t!=threads.end(); ++t){
			if((*t) == tt){
				return;
			}
		}
		threads.push_back(tt);
		DBUG_PRINT("info",("added {%lld} t resources\n", (NS_STORAGE::u64)threads.size()));
	}

	tree_stored::tree_thread * reuse_thread(){
		NS_STORAGE::scoped_ulock ul(tlock);
		tree_stored::tree_thread * r = NULL;
		Poco::Thread::TID curt = Poco::Thread::currentTid();

		for(_Threads::iterator t = threads.begin(); t != threads.end(); ++t){
			if((*t))
			{
				if(curt == (*t)->get_created_tid())
				{
					r = (*t);
					break;
				}
			}
		}

		if(r == NULL){
			r = new tree_stored::tree_thread();
		}
		DBUG_PRINT("info",("{%lld} t resources \n", (NS_STORAGE::u64)threads.size()));
		return r;
	}

	void check_use(){
		if(NS_STORAGE::total_use+btree_totl_used > MAX_EXT_MEM){
			(*this).reduce();

			printf("reducing block storage %.4g MiB\n",(double)stx::storage::total_use/(1024.0*1024.0));
			stored::reduce_all();

			}
	}

	void reduce(){
		NS_STORAGE::scoped_ulock ul(tlock);
		/*for(_Threads::iterator t = threads.begin(); t != threads.end(); ++t){
			if((*t)->get_locks()==0)
				(*t)->reduce_tables();
		}*/
	}
};

static static_threads st;

tree_stored::tree_thread** thd_to_tree_thread(THD* thd){
	return(tree_stored::tree_thread**) thd_ha_data(thd, static_treestore_hton );
}

void clear_thread(THD* thd){
	(*thd_to_tree_thread(thd)) = NULL;
}

tree_stored::tree_thread * thread_from_thd(THD* thd){
	if(thd==NULL) return NULL;
	tree_stored::tree_thread** stpp = thd_to_tree_thread(thd);
	return *stpp;
}

tree_stored::tree_thread * new_thread_from_thd(THD* thd){
	tree_stored::tree_thread** stpp = thd_to_tree_thread(thd);
	if((*stpp) == NULL){
		*stpp = st.reuse_thread();
	}
	if(*stpp == NULL){
		printf("the thread thd is NULL\n");
	}
	return *stpp;
}

tree_stored::tree_thread * old_thread_from_thd(THD* thd,THD* thd_old){
	tree_stored::tree_thread** stpp = thd_to_tree_thread(thd);
	if((*stpp) == NULL){
		if(thread_from_thd(thd_old) != NULL){
			*stpp = thread_from_thd(thd_old);
		}else{ // throw an error ??
			return NULL;
		}
	}
	return *stpp;
}

tree_stored::tree_thread* updated_thread_from_thd(THD* thd){
	tree_stored::tree_thread* r = new_thread_from_thd(thd);
	return r;
}

class ha_treestore: public handler{
public:

	static const int TREESTORE_MAX_KEY_LENGTH = collums::StaticKey::MAX_BYTES;
	std::string path;
	tree_stored::tree_table::ptr tt;
	tree_stored::tree_table::_TableMap::iterator r;
	tree_stored::tree_table::_TableMap::iterator r_stop;
	tree_stored::_Rid row;
	tree_stored::_Rid last_resolved;
	typedef tree_stored::_Selection  _Selection;
	tree_stored::_Selection selected;// direct selection filter
	tree_stored::IndexIterator index_iterator;
	tree_stored::tree_thread* get_thread(THD* thd){
		return new_thread_from_thd(thd);
	}
	tree_stored::tree_thread* get_thread(){
		return new_thread_from_thd(ha_thd());
	}
	tree_stored::tree_table::ptr get_tree_table(){
		if(tt==NULL){
			tt = get_thread()->compose_table((*this).table);
			tt->check_load((*this).table);
		}
		return tt;
	}
	void clear_selection(_Selection & selected){

		selected.clear();
	}

	ha_treestore
	(	handlerton *hton
	,	TABLE_SHARE *table_arg
	)
	:	handler(hton, table_arg)
	,	tt(NULL)
	,	row(0)
	,	last_resolved(0)
	{

	}
	~ha_treestore(){
	}


	int rnd_pos(uchar * buf,uchar * pos){
		memcpy(&row, pos, sizeof(row));
		for(_Selection::iterator s = selected.begin(); s != selected.end(); ++s){
			tree_stored::selection_tuple & sel = (*s);
			sel.col->seek_retrieve(row, sel.field);
		}
		return 0;
	}

	void position(const uchar *){
		memcpy(this->ref, &row, sizeof(row));
	}

	int info(uint which){


		if(get_tree_table() == NULL){
			return 0;
		}
		bool do_unlock = false;
		if(get_tree_table()->table==nullptr){
			do_unlock = true;
			get_tree_table()->begin(true);
		}
		if(which & HA_STATUS_NO_LOCK){// - the handler may use outdated info if it can prevent locking the table shared
			stats.data_file_length = get_tree_table()->table_size();
			stats.block_size = 1;
			stats.records = std::max<tree_stored::_Rid>(2, get_tree_table()->row_count());
		}

		if(which & HA_STATUS_TIME) // - only update of stats->update_time required
		{}

		if(which & HA_STATUS_CONST){
			// - update the immutable members of stats
			/// (max_data_file_length, max_index_file_length, create_time, sortkey, ref_length, block_size, data_file_name, index_file_name)
			/*
			  update the 'constant' part of the info:
			  handler::max_data_file_length, max_index_file_length, create_time
			  sortkey, ref_length, block_size, data_file_name, index_file_name.
			  handler::table->s->keys_in_use, keys_for_keyread, rec_per_key
			*/
			//stats.max_data_file_length = MAX_FILE_SIZE;
			//stats.max_index_file_length = 0;
			//stats.sortkey;
			//stats.ref_length;
			stats.block_size = 1;
			stats.mrr_length_per_rec= sizeof(tree_stored::_Rid)+sizeof(void*);
			//handler::table->s->keys_in_use;
			//handler::table->s->keys_for_keyread;

		}

		if(which & HA_STATUS_VARIABLE) {// - records, deleted, data_file_length, index_file_length, delete_length, check_time, mean_rec_length
			stats.data_file_length = get_tree_table()->table_size();
			stats.block_size = 4096;
			stats.records = std::max<tree_stored::_Rid>(2, get_tree_table()->row_count());
			stats.mean_rec_length = stats.data_file_length / stats.records;

		}
		for (ulong	i = 0; i < table->s->keys; i++) {
			for (ulong j = 0; j < table->key_info[i].actual_key_parts; j++) {

				table->key_info[i].rec_per_key[j] = tt->get_rows_per_key(i,j);
			}
		}
		if(which & HA_STATUS_ERRKEY) // - status pertaining to last error key (errkey and dupp_ref)
		{}

		if(which & HA_STATUS_AUTO)// - update autoincrement value
		{

			//handler::auto_increment_value = get_tree_table()->row_count();
		}

		if(do_unlock)
			get_tree_table()->rollback();

		return 0;
	}

	const char *table_type(void) const{
		return "TREESTORE";
	}

	const char **bas_ext(void) const{
		static const char * exts[] = {"", NullS};
		return exts;
	}

	ulong index_flags(uint,uint,bool) const{
		return ( HA_READ_NEXT | HA_READ_RANGE | HA_READ_PREV ); //HA_READ_ORDER|
	}
	uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }
	uint max_supported_keys()          const { return MAX_KEY; }
	uint max_supported_key_parts()     const { return MAX_REF_PARTS; }
	uint max_supported_key_length()    const { return TREESTORE_MAX_KEY_LENGTH; }
	uint max_supported_key_part_length() const { return TREESTORE_MAX_KEY_LENGTH; }
	const key_map *keys_to_use_for_scanning() { return &key_map_full; }
	Table_flags table_flags(void) const{
		return (	HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |// HA_TABLE_SCAN_ON_INDEX |HA_PRIMARY_KEY_IN_READ_INDEX |
					 HA_FILE_BASED |
					/*| HA_REC_NOT_IN_SEQ | HA_AUTO_PART_KEY | HA_CAN_INDEX_BLOBS |*/
					/*HA_NO_PREFIX_CHAR_KEYS |HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |*/
					 HA_PRIMARY_KEY_REQUIRED_FOR_DELETE |
					/* HA_NO_TRANSACTIONS   |*/
					HA_PARTIAL_COLUMN_READ | HA_NULL_IN_KEY /*|HA_CAN_REPAIR*/
);
	}

	double scan_time()
	{
		return((double) (std::max<nst::u64>(1, get_tree_table()->table_size()/8192)));/*assume an average page size of 8192 bytes*/
	}
	/// from InnoDB
	double read_time(uint index, uint ranges, ha_rows rows)
	{
		ha_rows total_rows;
		double	time_for_scan;

		if (index != table->s->primary_key) {

			return(handler::read_time(index, ranges, rows));
		}

		if (rows <= 2) {

			return((double) rows );
		}
		/* Assume that the read time is proportional to the scan time for all
		rows + at most one seek per range. */

		time_for_scan = scan_time();

		if ((total_rows =  get_tree_table()->row_count()) < rows) {

			return(time_for_scan);
		}

		return(ranges + (double) rows / (double) total_rows * time_for_scan);
	}

	int create(const char *n,TABLE *t,HA_CREATE_INFO *ha){
		tt = get_thread()->compose_table(t);
		path = t->s->path.str;
		return 0;
	}

	int open(const char *n,int,uint){
		tt = get_thread()->compose_table(table);
		tt->check_load(table);
		path = table->s->path.str;
		printf("open tt %s\n", table->alias);
		return 0;
	}
	 int delete_table (const char * name){
		 _FileNames files = save_extensions[name];
		 DBUG_PRINT("info",("deleting files %s\n", name));
		 for(_FileNames::iterator f = files.begin(); f != files.end(); ++f){
			const char * name = (*f).c_str();
			handler::delete_table(name);
		 }
		 return 0;
	 }
	int close(void){
		DBUG_PRINT("info",("closing tt %s\n", table->alias));
		printf("closing tt %s\n", table->alias);
		clear_selection(selected);
		if(tt!= NULL)
			tt->clear();
		tt = NULL;
		return 0;
	}

	// tscan 1
	THR_LOCK_DATA **store_lock(THD * thd,THR_LOCK_DATA ** to,thr_lock_type lt){
		DBUG_ENTER("ha_treestore::store_lock");

		DBUG_RETURN(to);
	}
	// tscan 2
	int external_lock(THD *thd, int lock_type){
		DBUG_ENTER("::external_lock");
		if(table == NULL){
			printf("table cannot be locked - invalid argument\n");
			return 0;
		}
		bool writer = false;
		tree_stored::tree_thread * thread = new_thread_from_thd(thd);
		if (lock_type == F_RDLCK || lock_type == F_WRLCK){
			DBUG_PRINT("info", (" *locking %s \n", table->s->normalized_path.str));
			if(lock_type == F_WRLCK){
				writer = true;
				thread->modify();
			}
			(*this).tt = thread->lock(table, writer);
		}else if(lock_type == F_UNLCK){
			DBUG_PRINT("info", (" -unlocking %s \n", table->s->normalized_path.str));
			thread->release(table);
			if(thread->get_locks()==0){
				clear_thread(thd);
				st.release_thread(thread);
				clear_selection(selected);
				/// (*this).tt = 0;
				DBUG_PRINT("info",("transaction finalized : %s\n",table->alias));
			}
		}


		DBUG_RETURN(0);
	}

	// tscan 3
	int rnd_init(bool scan){
		row = 0;
		tt = NULL;
		get_thread()->check_use();
		st.check_use();
		clear_selection(selected);
		last_resolved = 0;
		selected = get_tree_table()->create_output_selection(table);
		(*this).r = get_tree_table()->get_table().begin();
		(*this).r_stop = get_tree_table()->get_table().end();


		return 0;
	}
	// tscan 4

	int rnd_next(byte *buf){
		DBUG_ENTER("rnd_next");
		if((*this).r == (*this).r_stop){
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}
		last_resolved = (*this).r.key().get_value();
		statistic_increment(table->in_use->status_var.ha_read_rnd_next_count, &LOCK_status);
		for(_Selection::iterator s = selected.begin(); s != selected.end(); ++s){
			tree_stored::selection_tuple & sel = (*s);
			sel.col->seek_retrieve(last_resolved, sel.field);
		}
		++((*this).r);
		DBUG_RETURN(0);
	}

	int delete_row(const byte *buf){
		statistic_increment(table->in_use->status_var.ha_delete_count,&LOCK_status);

		get_tree_table()->erase(last_resolved, (*this).table);
		get_thread()->check_use();
		st.check_use();
		return 0;
	}

	int write_row(byte * buf){
		statistic_increment(table->in_use->status_var.ha_write_count,&LOCK_status);
		//if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_INSERT)
		//	table->timestamp_field->set_time();
		if (table->next_number_field && buf == table->record[0])
			update_auto_increment();
		get_tree_table()->write((*this).table);
		get_thread()->check_use();
		st.check_use();
		return 0;
	}

	int update_row(const byte *old_data, byte *new_data) {
		 statistic_increment(table->in_use->status_var.ha_read_rnd_next_count,
                      &LOCK_status);
		uchar *record_old = table->record[0];
		uchar *record_new = table->record[1];
		typedef std::vector<uchar *> _Saved;
		_Saved saved;

		for (Field **field=table->field ; *field ; field++){// offset to old rec
			Field * f = (*field);
			if(f){//this looks like its working but who knows actually
				saved.push_back(f->ptr);

				f->ptr = (uchar*)(old_data+f->offset(record_old));
			}
		}

		get_tree_table()->erase_row_index(last_resolved);/// remove old index entries

		for (Field **field=table->field ; *field ; field++){
			Field * ff = (*field);
			if(ff){
				if(bitmap_is_set(table->write_set, ff->field_index)){
					ff->ptr = (uchar*)(new_data+ff->offset(record_new));//replace only new data
				}
			}
		}
		get_tree_table()->write(last_resolved, (*this).table); /// write row and create indexes
		/// restore the fields with their original pointers
		_Saved::iterator si = saved.begin();
		for (Field **field=table->field ; *field ; field++){
			if(*field){
				Field * ff = (*field);
				if(ff){
					ff->ptr = (uchar*)(*si);
					++si;
				}
			}
		}
		return 0;
	}

	ha_rows records_in_range
	(	uint inx
	,	key_range *start_key
	,	key_range *end_key)
	{
		ha_rows rows = 0;
		DBUG_ENTER("records_in_range");
		//read_lookups+=2;

		tree_stored::IndexIterator first = get_index_iterator_lower(inx, start_key) ;
		tree_stored::IndexIterator last = get_index_iterator_upper(inx, end_key);
		rows = first.count(last);
		if (rows == 0) {
			rows = 1;
		}
		DBUG_PRINT("info", ("records in range %lld\n", (NS_STORAGE::u64)rows));
		DBUG_RETURN((ha_rows) rows);

	}

	/// indexing functions
	bool readset_covered;
	virtual int index_init(uint keynr, bool sorted){
		handler::active_index = keynr;
		tt = NULL;
		clear_selection(selected);
		selected = get_tree_table()->create_output_selection(table);
		readset_covered = get_tree_table()->read_set_covered_by_index(table, active_index, selected);

		return 0;
	}

	inline tree_stored::_Rid key_to_rid
	(	uint ax
	,	const tree_stored::CompositeStored& input
	)
	{
		return input.row;
	}

	void resolve_selection_from_index(uint ax, const tree_stored::CompositeStored& iinfo){

		using namespace NS_STORAGE;

		tree_stored::_Rid row = key_to_rid(ax, iinfo);
		last_resolved = row;
		for(_Selection::iterator s = selected.begin(); s != selected.end(); ++s){
			tree_stored::selection_tuple & sel = (*s);
			sel.col->seek_retrieve(row,sel.field);
		}

	}
	void resolve_selection_only_from_index(uint ax){

		const tree_stored::CompositeStored& iinfo = index_iterator.get_key();
		resolve_selection_from_index(ax, iinfo);

	}
	void resolve_selection_from_index(uint ax){

		const tree_stored::CompositeStored& iinfo = index_iterator.get_key();
		resolve_selection_from_index(ax, iinfo);

	}

	void resolve_selection_from_index(){
		resolve_selection_from_index(active_index);
	}

	int basic_index_read_idx_map
	(	uchar * buf
	,	uint keynr
	,	const byte * key
	,	key_part_map keypart_map
	,	enum ha_rkey_function find_flag
	){
		int r = HA_ERR_END_OF_FILE;
		table->status = STATUS_NOT_FOUND;
		tree_stored::tree_table::ptr tt =  get_tree_table();

		read_lookups++;
		const tree_stored::CompositeStored *pred = tt->predict_sequential(table, keynr, key, 0xffffff, keypart_map, index_iterator);
		if(pred==NULL){
			tt->temp_lower(table, keynr, key, 0xffffff, keypart_map,index_iterator);
		}
		print_read_lookups();
		if(index_iterator.valid()){

			switch(find_flag){
			case HA_READ_AFTER_KEY:
				index_iterator.next();
				if(index_iterator.invalid()){
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}

				break;
			case HA_READ_BEFORE_KEY:
				index_iterator.previous();
				if(index_iterator.invalid()){
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}

				break;
			case HA_READ_KEY_EXACT:
				{
					const tree_stored::CompositeStored& current = pred == NULL ?index_iterator.get_key(): *pred;
					if(!tt->check_key_match2(current,table,keynr,keypart_map)){
						DBUG_RETURN(HA_ERR_END_OF_FILE);
					}

				}
				break;
			default:

				break;
			}
			table->status = 0;
			r = 0;
			if(pred != NULL)
				resolve_selection_from_index(keynr, *pred);
			else
				resolve_selection_from_index(keynr);
			//if(HA_READ_KEY_EXACT != find_flag)

		}
		DBUG_RETURN(r);
	}
	int index_read_map
	(	uchar * buf
	,	const uchar * key
	,	key_part_map keypart_map
	,	enum ha_rkey_function find_flag)
	{
		int r = basic_index_read_idx_map(buf, active_index, key, keypart_map, find_flag);

		DBUG_RETURN(r);

	}

	int index_read
	(	uchar * buf
	,	const uchar * key
	,	uint key_len
	,	enum ha_rkey_function find_flag
	){
		return index_read_map(buf, key, 0xFFFFFFFF, find_flag);
	}

	int index_read_idx_map
	(	uchar * buf
	,	uint keynr
	,	const byte * key
	,	key_part_map keypart_map
	,	enum ha_rkey_function find_flag
	){
		tree_stored::tree_table::ptr tt =  get_tree_table();
		clear_selection(selected);
		selected = tt->create_output_selection(table);
		int r = basic_index_read_idx_map(buf, keynr, key, keypart_map, find_flag);

		DBUG_RETURN(r);
	}

	int index_read_idx
	(	uchar * buf
	,	uint keynr
	,	const byte * key
	,	key_part_map keypart_map
	,	enum ha_rkey_function find_flag
	){
		return index_read_idx_map(buf, keynr, key, keypart_map, find_flag);
	}
	int index_next(byte * buf) {
		int r = HA_ERR_END_OF_FILE;
		index_iterator.next();
		table->status = STATUS_NOT_FOUND;
		if(index_iterator.valid()){

			resolve_selection_from_index();
			r = 0;
			table->status = 0;
			//index_iterator.next();
		}

		DBUG_RETURN(r);
	}

	int index_prev(byte * buf) {
		index_iterator.previous();
		int r = HA_ERR_END_OF_FILE;
		table->status = STATUS_NOT_FOUND;
		if(index_iterator.valid()){
			r = 0;
			resolve_selection_from_index();
			table->status = 0;

		}
		DBUG_RETURN(r);
	}

	int index_first(byte * buf) {
		int r = HA_ERR_END_OF_FILE;
		DBUG_RETURN(r);
	}

	int index_last(byte * buf) {
		int r = HA_ERR_END_OF_FILE;
		DBUG_RETURN(r);
	}

	int set_index_iterator_lower
	(	uint ax
	,	const uchar *key
	,	uint key_l
	,	uint key_map
	,	enum ha_rkey_function find_flag
	){
		index_iterator = get_tree_table()->compose_query_lower_r(table, ax, key, key_l, key_map);
		return index_iterator.valid() ? 0 : HA_ERR_END_OF_FILE;
	}

	tree_stored::IndexIterator get_index_iterator_lower
	(	uint ax
	,	const uchar *key
	,	uint key_l
	,	uint key_map
	,	enum ha_rkey_function find_flag
	){
		return get_tree_table()->compose_query_lower_r(table, ax, key, key_l, key_map);
	}

	tree_stored::IndexIterator get_index_iterator_upper(uint ax, const key_range *start_key){
		if(start_key != NULL){
			return get_tree_table()->compose_query_upper(table, ax, start_key->key, start_key->length, start_key->keypart_map);
		}else{
			return get_tree_table()->compose_query_upper(NULL, ax, NULL, 0ul, 0ul);
		}

	}

	tree_stored::IndexIterator get_index_iterator_upper(const key_range *start_key){
		return get_index_iterator_upper(active_index, start_key);
	}

	int set_index_iterator_lower(const key_range *start_key){
		if(start_key != NULL)
			return set_index_iterator_lower(active_index, start_key->key, start_key->length, start_key->keypart_map, start_key->flag);
		else
			return set_index_iterator_lower(active_index, NULL, 0, 0, HA_READ_KEY_OR_NEXT);
	}

	tree_stored::IndexIterator get_index_iterator_lower(uint ax, const key_range *bound){
		if(bound != NULL){
			return get_index_iterator_lower(ax, bound->key, bound->length, bound->keypart_map,bound->flag);
		}else{
			return get_index_iterator_lower(ax, NULL, 0ul, 0xFFFFFFFF, HA_READ_AFTER_KEY);
		}
	}

	int iterations;

	tree_stored::_Rid range_iterator;
	virtual int read_range_first
	(	const key_range *start_key
	,	const key_range *end_key
	,	bool eq_range
	,	bool sorted){


		int r = get_tree_table()->row_count() == 0 ?  HA_ERR_END_OF_FILE : 0;
		iterations = 0;
		range_iterator = 0;
		//if(readset_covered){
		read_lookups++;
		r = set_index_iterator_lower(start_key);
		if(r==0){
			table->status = 0;
			resolve_selection_from_index();
			read_lookups++;
			index_iterator.set_end(get_index_iterator_upper(end_key));
			if(index_iterator.valid()){
				index_iterator.next();
			}else
				r = HA_ERR_END_OF_FILE;
		}
		print_read_lookups();
		DBUG_RETURN(r);
	}

	virtual int read_range_next(){
		int r = HA_ERR_END_OF_FILE;


		if(index_iterator.valid()){
			r = 0;
			table->status = 0;
			resolve_selection_from_index();
			index_iterator.next();
		}

		++range_iterator;

		DBUG_RETURN(r);
	}
};


static handler *treestore_create_handler(handlerton *hton,
	TABLE_SHARE *table,
	MEM_ROOT *mem_root)
{
	return new (mem_root) ha_treestore(hton, table);
}
static int treestore_commit(handlerton *hton, THD *thd, bool all){
	int return_val= 0;

	DBUG_PRINT("info", ("error val: %d", return_val));
	DBUG_RETURN(return_val);
}


static int treestore_rollback(handlerton *hton, THD *thd, bool all){
	int return_val= 0;
	DBUG_ENTER("plasticity_rollback");

	DBUG_PRINT("info", ("error val: %d", return_val));
	DBUG_RETURN(return_val);
}

#include "Poco/Logger.h"
#include "Poco/SimpleFileChannel.h"
#include "Poco/AutoPtr.h"
/// example code
void initialize_loggers(){
	using Poco::Logger;
	using Poco::SimpleFileChannel;
	using Poco::AutoPtr;

	AutoPtr<SimpleFileChannel> pChannel(new SimpleFileChannel);
	pChannel->setProperty("path", "treestore_addresses.log");
	pChannel->setProperty("rotation", "50 M");
	AutoPtr<SimpleFileChannel> pBuffers(new SimpleFileChannel);
	pBuffers->setProperty("path", "treestore_buffers.log");
	pBuffers->setProperty("rotation", "50 M");
	Logger& logger = Logger::get("addresses");
	Logger& buffers = Logger::get("buffers");
	logger.setChannel(pChannel);
	buffers.setChannel(pBuffers);

}
int treestore_db_init(void *p)
{
#ifdef _MSC_VER
	LONG  HeapFragValue = 2;
	if(HeapSetInformation((PVOID)_get_heap_handle(),
						HeapCompatibilityInformation,
						&HeapFragValue,
						sizeof(HeapFragValue))
	)
	{
		printf(" *** Tree Store (eyore) starting\n");
	}
#endif
	DBUG_ENTER("treestore_db_init");
	//initialize_loggers();

	Poco::Data::SQLite::Connector::registerConnector();
	handlerton *treestore_hton= (handlerton *)p;
	static_treestore_hton = treestore_hton;
	treestore_hton->state= SHOW_OPTION_YES;
	treestore_hton->db_type = DB_TYPE_DEFAULT;
	treestore_hton->commit = treestore_commit;
	treestore_hton->rollback = treestore_rollback;
	treestore_hton->create = treestore_create_handler;
	treestore_hton->flags= HTON_ALTER_NOT_SUPPORTED | HTON_NO_PARTITION;

	
	//treestore_hton->commit= 0;
	//treestore_hton->rollback= 0;
	DBUG_RETURN(FALSE);
}

int treestore_done(void *p)
{
	return 0;
}
struct st_mysql_storage_engine treestore_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(treestore)
{
	MYSQL_STORAGE_ENGINE_PLUGIN,
		&treestore_storage_engine,
		"TREESTORE",
		"Christiaan Pretorius (c) 2013",
		"TuReeStore MySQL storage engine",
		PLUGIN_LICENSE_GPL,
		treestore_db_init, /* Plugin Init */
		treestore_done, /* Plugin Deinit */
		0x0100 /* 1.0 */,
		NULL,                       /* status variables                */
		NULL,                       /* system variables                */
		NULL,                       /* config options                  */
		0,                          /* flags                           */
}
mysql_declare_plugin_end;
