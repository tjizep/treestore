
#include "myi.h"


/// TREESTORE specific includes

/// instances of config variables
#include "var.cpp"
#include "info.cpp"

typedef uchar byte;
ptrdiff_t MAX_PC_MEM = 1024ll*1024ll*1024ll*4ll;
namespace nst = NS_STORAGE;
nst::u64 pk_lookups = 0;
nst::u64 read_lookups =0;
nst::u64 hash_hits =0;
nst::u64 hash_predictions =0;
nst::u64 last_read_lookups ;
nst::u64 total_cache_size=0;
nst::u64 ltime = 0;
static nst::u64 total_locks = 0;
static Poco::Mutex mut_total_locks;

/// accessors for journal stats
void set_treestore_journal_size(nst::u64 ns){
	treestore_journal_size = ns;
}

nst::u64 get_treestore_journal_size(){
	return treestore_journal_size ;
}
nst::u64 get_treestore_journal_lower_max(){
	return treestore_journal_lower_max ;
}
/// <-journal stats.

static Poco::Mutex plock;
static Poco::Mutex p2_lock;

long long calc_total_use(){
	treestore_current_mem_use =  NS_STORAGE::total_use+btree_totl_used+total_cache_size;
	return treestore_current_mem_use;
}
void print_read_lookups();


typedef std::map<std::string, _FileNames > _Extensions;
typedef std::unordered_map<std::string, int > _LoadingData;


Poco::Mutex tree_stored::tree_table::shared_lock;
Poco::Mutex single_writer_lock;
Poco::Mutex data_loading_lock;

tree_stored::tree_table::_SharedData  tree_stored::tree_table::shared;
_LoadingData loading_data;

collums::_LockedRowData* collums::get_locked_rows(const std::string& name){
	static collums::_RowDataCache rdata;
	return rdata.get_for_table(name);
}
void collums::set_loading_data(const std::string& name, int loading){
	nst::synchronized sl(data_loading_lock);
	loading_data[name] = loading;
}
int collums::get_loading_data(const std::string& name){
	nst::synchronized sl(data_loading_lock);
	return loading_data[name] ;///def false
}

namespace ts_info{
	void perform_active_stats();
	void perform_active_stats(const std::string table);
};
namespace tree_stored{
	class tt{
		int x;
	public:
		tt(int x):x(x){
		}
	};
	typedef std::map<std::string, tree_table::ptr> _Tables;
	class tree_thread{
	protected:
		int locks;
		bool changed;
		Poco::Thread::TID created_tid;
		bool writing;

	public:
		int get_locks() const {
			return locks;
		}
		tree_thread() : locks(0),changed(false),writing(false){
			created_tid = Poco::Thread::currentTid();
			DBUG_PRINT("info",("tree thread %ld created\n", created_tid));
			DBUG_PRINT("info",(" *** Tree Store (eyore) mem use configured to %lld MB\n",treestore_max_mem_use/(1024*1024L)));
		}
		Poco::Thread::TID get_created_tid() const {
			return (*this).created_tid;
		}
		~tree_thread(){
			clear();
			DBUG_PRINT("info",("tree thread removed\n"));

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

		void check_journal(){
			if(get_treestore_journal_size() > (nst::u64)treestore_journal_upper_max){
				synchronized _s(p2_lock);
				if(!locks){
					for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
						(*t).second->rollback();
					}
				}
			}
		}

		tree_table * lock(TABLE *table_arg, bool writer){
			tree_table * result = NULL;
			if(writer){
				single_writer_lock.lock();

			}
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
			bool writer = changed;
			compose_table(table_arg)->unlock(&p2_lock);
			if(1==locks){

				if
				(	changed
				)
					NS_STORAGE::journal::get_instance().synch( ); /// synchronize to storage

				print_read_lookups();

				changed = false;
			}
			--locks;
			if(writer){
				single_writer_lock.unlock();

			}
		}
		public:

		void reduce_col_trees(){
			synchronized _s(p2_lock);
			if(!locks){
				DBUG_PRINT("info", ("reducing idle thread collums %.4g MiB\n",(double)stx::storage::total_use/(1024.0*1024.0)));
				for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
					if((*t).second)
						(*t).second->reduce_use_collum_trees();
					else
						printf("table entry %s is NULL\n", (*t).first.c_str());
				}
			}

		}
		void reduce_col_trees_only(){
			synchronized _s(p2_lock);
			if(!locks){
				DBUG_PRINT("info", ("reducing idle thread collums %.4g MiB\n",(double)stx::storage::total_use/(1024.0*1024.0)));
				for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
					if((*t).second)
						(*t).second->reduce_use_collum_trees_only();
					else
						printf("table entry %s is NULL\n", (*t).first.c_str());
				}
			}

		}
		void remove_table(const std::string name){
			synchronized _s(p2_lock);
			if(tables.count(name)){
				delete tables[name];
				tables.erase(name);
			}
		}
		void reduce_index_trees(){
			synchronized _s(p2_lock);
			if(!locks){
				DBUG_PRINT("info",("reducing idle thread indexes %.4g MiB\n",(double)stx::storage::total_use/(1024.0*1024.0)));
				for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
					if((*t).second)
						(*t).second->reduce_use_indexes();
					else
						printf("table entry %s is NULL\n", (*t).first.c_str());
				}
			}
		}

		void reduce_col_caches(){
			synchronized _s(p2_lock);
			if(!locks){
				DBUG_PRINT("info", ("reducing idle thread collum caches %.4g MiB\n",(double)calc_total_use()/(1024.0*1024.0)));
				typedef std::multimap<nst::u64, tree_stored::tree_table::ptr> _LastUsedTimes;
				_LastUsedTimes lru;
				for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
					if((*t).second){
						DBUG_PRINT("info",("adding table entry %s to LRU at %lld\n", (*t).first.c_str(), (nst::lld)(*t).second->get_last_lock_time()));
						lru.insert(std::make_pair((*t).second->get_last_lock_time(), (*t).second));
					}else
						DBUG_PRINT("info", ("table entry %s is NULL\n", (*t).first.c_str()));
				}
				size_t pos = 0;
				size_t factor =(size_t)(lru.size() *0.55);
				for(_LastUsedTimes::iterator l = lru.begin(); l != lru.end() && pos < factor;++l,++pos){
					(*l).second->reduce_use_collum_caches();

				}
			}

		}
		public:
		void check_use_col_trees(){
			//if(calc_total_use() > treestore_max_mem_use){

				reduce_col_trees();
			//}
		}
		void check_use_col_caches(){

			if(btree_totl_used < (treestore_max_mem_use*0.1)){				
				reduce_col_caches();
			}

		}
		void check_use_indexes(){
			//if(calc_total_use() > treestore_max_mem_use){

				reduce_index_trees();
			//}
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
	void remove_table(const std::string &name){
		NS_STORAGE::syncronized ul(tlock);

		for(_Threads::iterator t = threads.begin();t!=threads.end(); ++t){
			(*t)->remove_table(name);

		}
	}
	void release_thread(tree_stored::tree_thread * tt){
		if(tt==NULL){
			printf("Invalid argument to release thread\n");
			return ;
		}
		NS_STORAGE::syncronized ul(tlock);

		for(_Threads::iterator t = threads.begin();t!=threads.end(); ++t){
			if((*t) == tt){
				return;
			}
		}
		threads.push_back(tt);
		DBUG_PRINT("info",("added {%lld} t resources\n", (NS_STORAGE::u64)threads.size()));
	}

	tree_stored::tree_thread * reuse_thread(){
		NS_STORAGE::syncronized ul(tlock);
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
		/// TODO: this isnt safe yet (reducing another threads memory use)
		// return;
		if(calc_total_use() > treestore_max_mem_use){
			(*this).reduce();
		}

	}
	void reduce_all(){
		(*this).reduce();
	}
	void release_trees(){
		for(_Threads::iterator t = threads.begin(); t != threads.end() && calc_total_use() > treestore_max_mem_use; ++t){
			if((*t)->get_locks()==0)/// this should ignore busy threads
				if ((*t)->get_created_tid() != Poco::Thread::currentTid()){
					(*t)->reduce_col_trees();
					(*t)->reduce_index_trees();

				}
		}
	}
private:
	void reduce(){
		NS_STORAGE::syncronized ul(tlock);
		for(_Threads::iterator t = threads.begin(); t != threads.end() && calc_total_use() > treestore_max_mem_use; ++t){
			if((*t)->get_locks()==0)/// this should ignore busy threads
				if ((*t)->get_created_tid() != Poco::Thread::currentTid())
					(*t)->check_use_col_trees();
		}
		for(_Threads::iterator t = threads.begin(); t != threads.end() && calc_total_use() > treestore_max_mem_use; ++t){
			if((*t)->get_locks()==0)/// this should ignore busy threads
				if ((*t)->get_created_tid() != Poco::Thread::currentTid())
					(*t)->check_use_indexes();
		}
		for(_Threads::iterator t = threads.begin(); t != threads.end() && calc_total_use() > treestore_max_mem_use; ++t){
			if((*t)->get_locks()==0)/// this should ignore busy threads
				if ((*t)->get_created_tid() != Poco::Thread::currentTid())
					(*t)->check_use_col_caches();
		}
		//if(calc_total_use() > treestore_max_mem_use){
			for(_Threads::iterator t = threads.begin(); t != threads.end(); ++t){

				if ((*t)->get_created_tid() == Poco::Thread::currentTid()){
					(*t)->check_use_col_trees();
					(*t)->check_use_col_caches();
					(*t)->check_use_indexes();
					break;
				}
			}
		//}
	}
public:
	void check_journal(){
		nst::synchronized ss(tlock);
		for(_Threads::iterator t = threads.begin(); t != threads.end(); ++t){
			(*t)->check_journal();
		}
	}
};

static static_threads st;

void reduce_thread_usage(){
	if(calc_total_use() > treestore_max_mem_use){
		st.check_use();
		//reduce_info_tables();	
		
		if(calc_total_use() > treestore_max_mem_use){
			//nst::synchronized s2(tt_info_lock);/// the info function may be called in another thread
			
			stx::process_idle_times();	
		}
		//}		
	}
	if(calc_total_use() > treestore_max_mem_use){			
		
		nst::synchronized s(mut_total_locks);				
		if(total_locks==0){
			st.reduce_all();
		}
	}
	if(nst::buffer_use > treestore_max_mem_use*0.75){			
		/// time to reduce some blocks
		stored::reduce_all();
			
	}
	
}

void print_read_lookups(){
	
	if(os::millis()-ltime > 1000){
		
		stx::storage::syncronized ul(plock);
		if(os::millis()-ltime > 1000){
			if(ltime){
				double use = (double) calc_total_use();
				double twitch = 500.0 * units::MB;
				printf
				(   "read_lookups %lld/s hh %lld hp %lld (total: %lld - btt %.4g %s in %lld trees)\n"
				,   (nst::lld)read_lookups-std::min<NS_STORAGE::u64>(read_lookups, last_read_lookups)
				,   (nst::lld)hash_hits
				,   (nst::lld)hash_predictions
				,   (nst::lld)read_lookups
				,   (double)use/(use < twitch ? units::MB : units::GB)
				,	use < twitch ? "MB" : "GB"
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

	static const int TREESTORE_MAX_KEY_LENGTH = stored::DynamicKey::MAX_BYTES;
	std::string path;
	tree_stored::tree_table::ptr tt;
	tree_stored::tree_table::_TableMap::iterator r;
	tree_stored::tree_table::_TableMap::iterator r_stop;
	stored::_Rid row;
	stored::_Rid last_resolved;
	typedef tree_stored::_Selection  _Selection;
	tree_stored::_Selection selected;// direct selection filter

	stored::index_interface * current_index;
	stored::index_iterator_interface * current_index_iterator;
	inline stored::index_iterator_interface& get_index_iterator(){
		return *current_index_iterator;
	}

	tree_stored::tree_thread* get_thread(THD* thd){
		return new_thread_from_thd(thd);
	}
	tree_stored::tree_thread* get_thread(){
		return new_thread_from_thd(ha_thd());
	}
	tree_stored::tree_table::ptr get_tree_table(){
		if(tt==NULL){
			tt = get_thread()->compose_table((*this).table);
		}
		tt->check_load((*this).table);
		return tt;
	}
	void clear_selection(_Selection & selected){

		for(_Selection::iterator s = selected.begin(); s != selected.end(); ++s){
			(*s).restore_ptr();
		}
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
	,	current_index(NULL)
	{

	}

	~ha_treestore()
	{
	}


	int rnd_pos(uchar * buf,uchar * pos){
		memcpy(&row, pos, sizeof(row));
		resolve_selection(row);

		return 0;
	}

	void position(const uchar *){
		memcpy(this->ref, &row, sizeof(row));
	}

	int info(uint which){

		if(get_tree_table() == NULL){
			return 0;
		}
		{
			nst::synchronized s(mut_total_locks);
			++total_locks;
		}
		
		tree_stored::tree_table * ts = get_info_table((*this).table);		
		tree_stored::table_info tt ;
		ts_info::perform_active_stats(table->s->path.str);
		ts->get_calculated_info(tt);

		
		if(which & HA_STATUS_NO_LOCK){// - the handler may use outdated info if it can prevent locking the table shared
			stats.data_file_length = tt.table_size;
			stats.block_size = 1;
			stats.records = tt.row_count;
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
			stats.mrr_length_per_rec= sizeof(stored::_Rid)+sizeof(void*);
			//handler::table->s->keys_in_use;
			//handler::table->s->keys_for_keyread;

		}

		if(which & HA_STATUS_VARIABLE) {// - records, deleted, data_file_length, index_file_length, delete_length, check_time, mean_rec_length
			stats.data_file_length = tt.table_size;
			stats.block_size = 4096;
			stats.records = tt.row_count;
			stats.mean_rec_length =(ha_rows) stats.records ? ( stats.data_file_length / (stats.records+1) ) : 1;

		}
		
		
		for (ulong	i = 0; i < table->s->keys; i++) {
			if(i < tt.calculated.size()){
				for (ulong j = 0; j < table->key_info[i].actual_key_parts; j++) {
					if(j < tt.calculated[i].density.size()){
						table->key_info[i].rec_per_key[j] = tt.calculated[i].density[j];
					}
				}
			}
		}

		if(which & HA_STATUS_ERRKEY) // - status pertaining to last error key (errkey and dupp_ref)
		{}

		if(which & HA_STATUS_AUTO)// - update autoincrement value
		{

			//handler::auto_increment_value = get_tree_table()->row_count();
		}

		
		{
			nst::synchronized s(mut_total_locks);
			--total_locks;
		}
		return 0;
	}

	const char *table_type(void) const{
		return "TREESTORE";
	}

	const char *index_type(uint /*keynr*/) const{
		return("BTREE");
	}

	const char **bas_ext(void) const{
		static const char * exts[] = {TREESTORE_FILE_EXTENSION, NullS};
		return exts;
	}

	ulong index_flags(uint,uint,bool) const{
		return ( HA_READ_ORDER|HA_READ_NEXT | HA_READ_RANGE | HA_READ_PREV ); //
	}
	uint max_supported_record_length() const { return HA_MAX_REC_LENGTH*8; }
	uint max_supported_keys()          const { return MAX_KEY; }
	uint max_supported_key_parts()     const { return MAX_REF_PARTS; }
	uint max_supported_key_length()    const { return TREESTORE_MAX_KEY_LENGTH; }
	uint max_supported_key_part_length() const { return TREESTORE_MAX_KEY_LENGTH; }
	const key_map *keys_to_use_for_scanning() { return &key_map_full; }
	Table_flags table_flags(void) const{
		return (	HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |HA_PRIMARY_KEY_IN_READ_INDEX |HA_TABLE_SCAN_ON_INDEX |//
					 HA_FILE_BASED | HA_STATS_RECORDS_IS_EXACT |
					/*| HA_REC_NOT_IN_SEQ | HA_AUTO_PART_KEY | HA_CAN_INDEX_BLOBS |*/
					/*HA_NO_PREFIX_CHAR_KEYS |HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |*/
					 HA_PRIMARY_KEY_REQUIRED_FOR_DELETE |
					/* HA_NO_TRANSACTIONS   |*/
					HA_PARTIAL_COLUMN_READ | HA_NULL_IN_KEY /*|HA_CAN_REPAIR*/
				);
	}

	double scan_time()
	{
		return((double) (std::max<nst::u64>(1, get_tree_table()->table_size())/16384));/*assume an average page size of 8192 bytes*/
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


	_FileNames extensions_from_table_name(const std::string& name){
		using Poco::StringTokenizer;
		using Poco::Path;
		using Poco::DirectoryIterator;
		StringTokenizer components(name, "\\/");
		std::string path,filestart;

		size_t e = (components.count()-1);
		filestart = components[e];
		filestart += TREESTORE_FILE_SEPERATOR;

		for(size_t s = 0; s < e; ++s){
			path += components[s] ;
			if (s < e-1)
				path += Path::separator();
		}

		_FileNames files;
		for(DirectoryIterator d(path); d != DirectoryIterator();++d){
			DBUG_PRINT("info",("scanning %s\n",d.name().c_str()));
			if((*d).isFile()){
				if(d.name().substr(0,filestart.size())==filestart && d.name() != filestart){
					DBUG_PRINT("info",("found member table %s\n",d.name().c_str()));
					std::string p = ".";
					p += Path::separator();
					p += (*d).path();
					for(size_t pop = 0; pop < strlen(TREESTORE_FILE_EXTENSION);++pop){
						p.pop_back();
					}

					files.push_back(p);
				}
			}
		}
		DBUG_PRINT("info",("Found %lld files\n", (nst::lld)files.size()));
		return files;

	}

	/// from and to are complete relative paths
	int rename_table(const char *_from, const char *_to){

		DBUG_PRINT("info",("renaming files %s\n", name));


		int r = 0;
		std::string from = _from;
		std::string to = _to;
		delete_info_table(_from);
		delete_info_table(_to);/// to avoid potential issues

		_FileNames files = extensions_from_table_name(from);

		for(_FileNames::iterator f = files.begin(); f != files.end(); ++f){
			while(collums::get_loading_data((*f))!=0){
				os::zzzz(100);
			}
		}

		os::zzzz(130);
		st.remove_table(from);

		std::string extenstion = TREESTORE_FILE_EXTENSION;
		for(_FileNames::iterator f = files.begin(); f != files.end(); ++f){


			std::string name = (*f);
			using Poco::File;
			using Poco::Path;
			try{
				std::string next = name + extenstion;
				printf("renaming %s\n",next.c_str());
				File df (next);
				if(df.exists()){
					std::string renamed = to + &next[from.length()];
					df.moveTo(renamed);
				}
			}catch(std::exception& ){
				printf("could not rename table file %s\n", name.c_str());
				r = HA_ERR_NO_SUCH_TABLE;
			}
			///
		}
		try{
			using Poco::File;
			using Poco::Path;
			std::string nxt = from + extenstion;
			printf("renaming %s\n",nxt.c_str());
			File df (nxt);
			if(df.exists()){
				std::string renamed = to + &nxt[from.length()];
				df.moveTo(renamed);
			}
		}catch(std::exception& ){
			printf("could not rename table file %s\n", from.c_str());
			r = HA_ERR_NO_SUCH_TABLE;

		}


		return r;
	}
	int delete_table (const char * name){
		DBUG_PRINT("info",("deleting files %s\n", name));
		int r = 0;
		delete_info_table(name);
		_FileNames files = extensions_from_table_name(name);

		for(_FileNames::iterator f = files.begin(); f != files.end(); ++f){
			while(collums::get_loading_data((*f))!=0){
				os::zzzz(100);
			}
		}

		os::zzzz(130);
		st.remove_table(name);
		std::string extenstion = TREESTORE_FILE_EXTENSION;
		for(_FileNames::iterator f = files.begin(); f != files.end(); ++f){


			std::string name = (*f);
			using Poco::File;
			using Poco::Path;
			try{
				std::string next = name + extenstion;
				printf("deleting %s\n",next.c_str());
				File df (next);
				if(df.exists()){
					df.remove();
				}
			}catch(std::exception& ){
				printf("could not delete table file %s\n", name.c_str());
				r = HA_ERR_NO_SUCH_TABLE;

			}
			///
		}
		try{
			using Poco::File;
			using Poco::Path;
			std::string nxt = name + extenstion;
			printf("deleting %s\n",nxt.c_str());
			File df (nxt);
			if(df.exists()){
				df.remove();
			}
		}catch(std::exception& ){
			printf("could not delete table file %s\n", name);
			r = HA_ERR_NO_SUCH_TABLE;

		}

		//r = handler::delete_table(name);
		return r;
	}
	int close(void){
		DBUG_PRINT("info",("closing tt %s\n", table->alias));
		printf("closing tt %s\n", table->alias);
		clear_selection(selected);
		if(tt!= NULL)
			tt->clear();
		tt = NULL;
		current_index = NULL;
		current_index_iterator = NULL;
		return 0;
	}

	// tscan 1
	THR_LOCK_DATA **store_lock(THD * thd,THR_LOCK_DATA ** to,thr_lock_type lt){
		DBUG_ENTER("ha_treestore::store_lock");

		DBUG_RETURN(to);
	}
	int start_stmt(THD *thd, thr_lock_type lock_type){
		int r= external_lock(thd, lock_type);

		return r;
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
		if(total_locks==0){
			nst::synchronized s(mut_total_locks);
			if(total_locks==0){
				bool high_mem = calc_total_use() > treestore_max_mem_use ;
				
				if
				(	get_treestore_journal_size() > (nst::u64)treestore_journal_upper_max			
				)
				{					
					st.check_journal();/// function releases all idle reading transaction locks
					NS_STORAGE::journal::get_instance().synch( high_mem ); /// synchronize to storage									
				}
				
			}
		}
		if (lock_type == F_RDLCK || lock_type == F_WRLCK || lock_type == F_UNLCK)
			printf
			(	"[%s]l %s m:T%.4g b%.4g c%.4g t%.4g pc%.4g MB\n"
			,	lock_type == F_UNLCK ? "-":"+"
			,	table->s->normalized_path.str
			,	(double)calc_total_use()/units::MB
			,	(double)nst::buffer_use/units::MB
			,	(double)nst::col_use/units::MB
			,	(double)btree_totl_used/units::MB
			,	(double)total_cache_size/units::MB
			);
		if (lock_type == F_RDLCK || lock_type == F_WRLCK){
			{
				nst::synchronized s(mut_total_locks);
				++total_locks;
			}
			if(get_treestore_journal_size() > (nst::u64)treestore_journal_upper_max){

				return HA_ERR_LOCK_TABLE_FULL;
			}
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

				if(treestore_reduce_tree_use_on_unlock==TRUE){
					thread->reduce_col_trees_only();
					if(treestore_reduce_index_tree_use_on_unlock==TRUE)
						thread->reduce_index_trees();
				}
				clear_selection(selected);

				clear_thread(thd);

				st.release_thread(thread);
				/// (*this).tt = 0;
				
				/// for testing
				//st.reduce_all();

				printf
				(	"%s m:T%.4g b%.4g c%.4g t%.4g pc%.4g MB\n"
				,	"transaction complete"
				,	(double)calc_total_use()/units::MB
				,	(double)nst::buffer_use/units::MB
				,	(double)nst::col_use/units::MB
				,	(double)btree_totl_used/units::MB
				,	(double)total_cache_size/units::MB
				);
				DBUG_PRINT("info",("transaction finalized : %s\n",table->alias));

			}
			
			{
				nst::synchronized s(mut_total_locks);
				--total_locks;
					
			}
		}


		DBUG_RETURN(0);
	}


	bool push_func(const Item_func* f,tree_stored::logical_conditional_iterator::ptr parent){
		/// int argc = f->argument_count();
		Item_func::Functype ft = f->functype();
		if(ft==Item_func::COND_OR_FUNC || ft==Item_func::COND_AND_FUNC){
			Item_cond_or* c = (Item_cond_or* )f;
			tree_stored::logical_conditional_iterator::ptr lor ;
			lor = ft==Item_func::COND_OR_FUNC ? get_tree_table()->create_or_condition() : get_tree_table()->create_and_condition();
			if(parent == nullptr){

				get_tree_table()->set_root_condition(lor);
			}else{
				parent->push_condition(lor);
			}

			List_iterator<Item> i = *(c->argument_list());
			for(;;){
				const Item *cc = i++;

				if(NULL == cc)
					break;
				/// Item::Type ct = cc->type();
				if(cc->type() == Item::FUNC_ITEM){
					const Item_func* f = (const Item_func*)cc;
					if(!push_func(f,lor)) return false;
				}else
					return false;
			}
			return true;
		}else if(Item_func::COND_ITEM && f->argument_count() == 2){
			const Item * i0 = f->arguments()[0];
			const Item * val = f->arguments()[1];
			Item::Type t1 = i0->type();
			Item::Type t2 = val->type();

			if(t1 == Item::FIELD_ITEM){
				switch(t2){
				case Item::STRING_ITEM: case Item::INT_ITEM: case Item::REAL_ITEM:case Item::VARBIN_ITEM:case Item::DECIMAL_ITEM:{

					const Item_field * fi = (const Item_field*)i0;

					tree_stored::abstract_conditional_iterator::ptr local = get_tree_table()->create_field_condition(fi, f, val);
					if(parent == nullptr)
						get_tree_table()->set_root_condition(local);
					else
						parent->push_condition(local);
					return true;
								  }
				default:
					break;
				}
			}

		}
		return false;
	}
	bool push_cond(const Item * acon,tree_stored::logical_conditional_iterator::ptr parent){
		Item::Type t = acon->type();
		if(t == Item::FUNC_ITEM){
			const Item_func* f = (const Item_func*)acon;
			return push_func(f,parent);
		}else if(t==Item::COND_ITEM){
			Item_cond* c = (Item_cond* )acon;
			if(c->functype() == Item_func::COND_AND_FUNC || c->functype() == Item_func::COND_OR_FUNC){
				tree_stored::logical_conditional_iterator::ptr lcond ;
				lcond = (c->functype() == Item_func::COND_AND_FUNC) ? get_tree_table()->create_and_condition() : get_tree_table()->create_or_condition();
				if(parent == nullptr){

					get_tree_table()->set_root_condition(lcond);
				}else{
					parent->push_condition(lcond);
				}
				List_iterator<Item> i = *(c->argument_list());
				for(;;){
					const Item *cc = i++;

					if(NULL == cc)
						break;
					/// Item::Type ct = cc->type();
					if(cc->type() == Item::FUNC_ITEM){
						const Item_func* f = (const Item_func*)cc;
						if(!push_func(f,lcond)){
							return false;
						}
					}else{
						if(!push_cond(cc,lcond)){
							return false;
						}
					}
				}
				return true;
			}else
				return false;
		}
		return false;
	}

	/// call by MySQL to advertise push down conditions
	const Item *cond_push(const Item *acon) {
		if(push_cond(acon,nullptr))
			return NULL;
		get_tree_table()->pop_all_conditions();
		return acon;

		/// const char * n = acon->full_name();


	};
	void cond_pop(){
		get_tree_table()->pop_condition();
	}
	// tscan 3
	int rnd_init(bool scan){
		row = 0;
		tt = NULL;
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
			get_tree_table()->pop_all_conditions();
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		last_resolved = (*this).r.key().get_value();
		stored::_Rid c = tree_stored::abstract_conditional_iterator::NoRecord;
		while(c != last_resolved){
			/// get the next record that matches
			/// returns last_resolved if current i.e. first record matches
			c = (*this).get_tree_table()->iterate_conditions(last_resolved);
			/// c is either valid or NoRecord
			if(c == tree_stored::abstract_conditional_iterator::NoRecord){
				/// no record inclusive of last_resolved matched
				get_tree_table()->pop_all_conditions();
				DBUG_RETURN(HA_ERR_END_OF_FILE);
			}

			if(c > last_resolved + 20){

				(*this).r =  get_tree_table()->get_table().lower_bound(c);

			}else{

				while((*this).r.key().get_value() < c ){
					++((*this).r);
					if((*this).r == (*this).r_stop){
						get_tree_table()->pop_all_conditions();
						DBUG_RETURN(HA_ERR_END_OF_FILE);
					}
				}
			}
			/// c == last_resolved
			last_resolved = (*this).r.key().get_value();
		}

		statistic_increment(table->in_use->status_var.ha_read_rnd_next_count, &LOCK_status);

		resolve_selection(last_resolved);

		++((*this).r);
		DBUG_RETURN(0);
	}

	int delete_row(const byte *buf){
		statistic_increment(table->in_use->status_var.ha_delete_count,&LOCK_status);

		get_tree_table()->erase(last_resolved, (*this).table);


		return 0;
	}

	int write_row(byte * buf){
		statistic_increment(table->in_use->status_var.ha_write_count,&LOCK_status);
		/// TODO: auto timestamps

		//if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_INSERT)
		//	table->timestamp_field->set_time();
		if (table->next_number_field && buf == table->record[0])
			update_auto_increment();
		get_tree_table()->write((*this).table);

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

		stored::index_interface*current_index = get_tree_table()->get_index_interface(inx);

		get_index_iterator_lower(*current_index->get_first1(),inx, start_key) ;
		get_index_iterator_upper(*current_index->get_last1(), inx, end_key);
		rows = current_index->get_first1()->count(*current_index->get_last1());
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
		current_index = get_tree_table()->get_index_interface(handler::active_index);
		current_index_iterator = current_index->get_index_iterator();

		selected = get_tree_table()->create_output_selection(table);
		readset_covered = get_tree_table()->read_set_covered_by_index(table, active_index, selected);

		return 0;
	}

	inline stored::_Rid key_to_rid
	(	uint ax
	,	const tree_stored::CompositeStored& input
	)
	{
		return input.row;
	}
	void resolve_selection(stored::_Rid row){
		last_resolved = row;
#ifdef _ROW_CACHE_
		collums::_LockedRowData * lrd = get_tree_table()->get_row_datas();
		if(lrd && row < lrd->rows.size() ){
			collums::_RowData& rd = lrd->rows[row];
			if(rd.empty()){
				nst::synchronized sr(lrd->lock);
				rd.resize(sizeof(nst::u16)*get_tree_table()->get_col_count());
			}

			for(_Selection::iterator s = selected.begin(); s != selected.end(); ++s){
				tree_stored::selection_tuple & sel = (*s);
				sel.col->seek_retrieve(sel.field, row, rd);
			}

		}else
#endif
		{
			_Selection::iterator s = selected.begin();

			for(; s != selected.end(); ++s){
				tree_stored::selection_tuple & sel = (*s);
				sel.col->seek_retrieve(row,sel.field);
			}


		}

	}
	void resolve_selection_from_index(uint ax,  const tree_stored::CompositeStored& iinfo){

		using namespace NS_STORAGE;

		stored::_Rid row = key_to_rid(ax, iinfo);
		last_resolved = row;
		resolve_selection(row);
	}


	void resolve_selection_from_index(uint ax,stored::index_interface * current_index){

		tree_stored::CompositeStored& iinfo = current_index->get_index_iterator()->get_key();
		resolve_selection_from_index(ax, iinfo);

	}

	void resolve_selection_from_index(){
		resolve_selection_from_index(active_index,current_index);
	}

	int basic_index_read_idx_map
	(	uchar * buf
	,	uint keynr
	,	const byte * key
	,	key_part_map keypart_map
	,	enum ha_rkey_function find_flag
	,	stored::index_interface * current_index
	){
		int r = HA_ERR_END_OF_FILE;
		table->status = STATUS_NOT_FOUND;
		stored::index_iterator_interface & current_iterator = *(current_index->get_index_iterator());

		tree_stored::tree_table::ptr tt =  get_tree_table();

		read_lookups++;
		const tree_stored::CompositeStored *pred = tt->predict_sequential(table, keynr, key, 0xffffff, keypart_map,current_iterator);
		if(pred==NULL){
			
			tt->temp_lower(table, keynr, key, 0xffffff, keypart_map,current_iterator);
		}


		if(current_iterator.valid()){

			switch(find_flag){
			case HA_READ_AFTER_KEY:
				current_iterator.next();
				if(current_iterator.invalid()){
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}

				break;
			case HA_READ_BEFORE_KEY:
				current_iterator.previous();
				if(current_iterator.invalid()){
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}

				break;
			case HA_READ_KEY_EXACT:
				{
					const tree_stored::CompositeStored& current = pred == NULL ?current_iterator.get_key(): *pred;
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
				resolve_selection_from_index(keynr,current_index);
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
		int r = basic_index_read_idx_map(buf, active_index, key, keypart_map, find_flag, current_index);

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

		int r = basic_index_read_idx_map(buf, keynr, key, keypart_map, find_flag,get_tree_table()->get_index_interface(keynr));

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
		get_index_iterator().next();
		table->status = STATUS_NOT_FOUND;
		if(get_index_iterator().valid()){
			resolve_selection_from_index();
			r = 0;
			table->status = 0;
			//index_iterator.next();
		}

		DBUG_RETURN(r);
	}

	int index_prev(byte * buf) {
		get_index_iterator().previous();
		int r = HA_ERR_END_OF_FILE;
		table->status = STATUS_NOT_FOUND;
		if(get_index_iterator().valid()){
			r = 0;
			resolve_selection_from_index();
			table->status = 0;

		}
		DBUG_RETURN(r);
	}

	int index_first(byte * buf) {
		int r = HA_ERR_END_OF_FILE;

		DBUG_ENTER("index_first");
		ha_statistic_increment(&SSV::ha_read_first_count);
		stored::index_iterator_interface & current_iterator = *(current_index->get_prepared_index_iterator());
		current_iterator.first();
		table->status = STATUS_NOT_FOUND;
		if(get_index_iterator().valid()){
			resolve_selection_from_index();
			r = 0;
			table->status = 0;			
		}

		DBUG_RETURN(r);
	}

	int index_last(byte * buf) {
		int r = HA_ERR_END_OF_FILE;
		stored::index_iterator_interface & current_iterator = *(current_index->get_prepared_index_iterator());
		current_iterator.last();
		table->status = STATUS_NOT_FOUND;
		if(get_index_iterator().valid()){
			resolve_selection_from_index();
			r = 0;
			table->status = 0;			
		}
		DBUG_RETURN(r);
	}

	int set_index_iterator_lower
	(	uint ax
	,	const uchar *key
	,	uint key_l
	,	uint key_map
	,	enum ha_rkey_function find_flag
	){
		get_tree_table()->compose_query_lower_r(get_index_iterator(),table, ax, key, key_l, key_map);
		return get_index_iterator().valid() ? 0 : HA_ERR_END_OF_FILE;
	}

	void get_index_iterator_lower
	(	stored::index_iterator_interface& out
	,	uint ax
	,	const uchar *key
	,	uint key_l
	,	uint key_map
	,	enum ha_rkey_function find_flag
	){
		return get_tree_table()->compose_query_lower_r(out, table, ax, key, key_l, key_map);
	}

	void get_index_iterator_upper(stored::index_iterator_interface& out, uint ax, const key_range *start_key){
		if(start_key != NULL){
			get_tree_table()->compose_query_upper(out, table, ax, start_key->key, start_key->length, start_key->keypart_map);
		}else{
			get_tree_table()->compose_query_upper(out, NULL, ax, NULL, 0ul, 0ul);
		}

	}

	void get_index_iterator_upper(stored::index_iterator_interface& out,const key_range *start_key){
		get_index_iterator_upper(out, active_index, start_key);
	}

	int set_index_iterator_lower(const key_range *start_key){
		if(start_key != NULL)
			return set_index_iterator_lower(active_index, start_key->key, start_key->length, start_key->keypart_map, start_key->flag);
		else
			return set_index_iterator_lower(active_index, NULL, 0, 0, HA_READ_KEY_OR_NEXT);
	}

	void get_index_iterator_lower(stored::index_iterator_interface& out, uint ax, const key_range *bound){
		if(bound != NULL){
			get_index_iterator_lower(out, ax, bound->key, bound->length, bound->keypart_map,bound->flag);
		}else{
			get_index_iterator_lower(out, ax, NULL, 0ul, 0xFFFFFFFF, HA_READ_AFTER_KEY);
		}
	}

	nst::u64 iterations;

	stored::_Rid range_iterator;
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
			get_index_iterator_upper(*current_index->get_first1(), end_key);
			get_index_iterator().set_end(*current_index->get_first1());
			if(get_index_iterator().valid()){
				get_index_iterator().next();
			}else
				r = HA_ERR_END_OF_FILE;
		}
		
		DBUG_RETURN(r);
	}

	virtual int read_range_next(){
		int r = HA_ERR_END_OF_FILE;


		if(get_index_iterator().valid()){
			r = 0;
			table->status = 0;
			resolve_selection_from_index();
			get_index_iterator().next();
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


namespace storage_workers{

    struct _storage_worker{
        _storage_worker() : w(1){
        }
        _WorkerManager w;
    };

	unsigned int ctr = 0;
	unsigned int get_next_counter(){
		return ++ctr;
	}
	const int MAX_WORKER_MANAGERS = 2;
	extern _WorkerManager & get_threads(unsigned int which){
		static _storage_worker _adders[MAX_WORKER_MANAGERS];
		return _adders[which % MAX_WORKER_MANAGERS].w;
	}
}
/// example code

void initialize_loggers(){
#ifdef POCO_LOGGING
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
#endif
}
extern int pt_test();
extern int linitialize();
void start_cleaning();
void start_calculating();

void test_suffix_array(){
	
	printf("testing suffix array encoding\n");
	std::ifstream t("suffix_test.dat");
	t.seekg(0, std::ios::end);
	size_t size = t.tellg();
	std::string buffer(size, ' ');
	t.seekg(0);
	t.read(&buffer[0], size); 
	suffix_array_encoder senc;
	senc.encode(&buffer[0], buffer.size());
}
void test_signwriter(){
	
	NS_STORAGE::i64 r = 3000000000ll;
	NS_STORAGE::buffer_type buffer(100);
	NS_STORAGE::buffer_type::iterator writer = buffer.begin();
	NS_STORAGE::leb128::write_signed(writer, r);
	NS_STORAGE::i64 t = NS_STORAGE::leb128::read_signed(buffer.begin());
	if(t!=r){
		printf("test failed\n");
	}
}
void test_run(){
	test_signwriter();
	/// test_suffix_array();
	/// pt_test();
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
		printf(" *** Tree Store (eyore) using low fragmentation heap\n");
	}else{
		printf(" *** Tree Store (eyore) NOT using low fragmentation heap\n");
	}
#endif
	printf(" *** Tree Store (eyore) starting memuse configured to %lld MB\n",treestore_max_mem_use/(1024*1024L));
	DBUG_ENTER("treestore_db_init");
	initialize_loggers();
	Poco::Data::SQLite::Connector::registerConnector();
	handlerton *treestore_hton= (handlerton *)p;
	static_treestore_hton = treestore_hton;
	treestore_hton->state= SHOW_OPTION_YES;
	treestore_hton->db_type = DB_TYPE_DEFAULT;
	treestore_hton->commit = treestore_commit;
	treestore_hton->rollback = treestore_rollback;
	treestore_hton->create = treestore_create_handler;
	treestore_hton->flags= HTON_ALTER_NOT_SUPPORTED | HTON_NO_PARTITION;
	
	printf("Start cleaning \n");

	start_cleaning();
	start_calculating();
	test_run();
	DBUG_RETURN(FALSE);
}

int treestore_done(void *p)
{
	clear_info_tables();

	return 0;
}

static struct st_mysql_sys_var* treestore_system_variables[]= {
  MYSQL_SYSVAR(max_mem_use),
  MYSQL_SYSVAR(current_mem_use),
  MYSQL_SYSVAR(journal_size),
  MYSQL_SYSVAR(journal_lower_max),
  MYSQL_SYSVAR(journal_upper_max),
  MYSQL_SYSVAR(efficient_text),
  MYSQL_SYSVAR(column_cache),
  MYSQL_SYSVAR(column_encoded),
  MYSQL_SYSVAR(predictive_hash),
  MYSQL_SYSVAR(reduce_tree_use_on_unlock),
  MYSQL_SYSVAR(reduce_index_tree_use_on_unlock),
  MYSQL_SYSVAR(reduce_storage_use_on_unlock),
  MYSQL_SYSVAR(use_primitive_indexes),
  NULL
};

struct st_mysql_storage_engine treestore_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(treestore)
{
	MYSQL_STORAGE_ENGINE_PLUGIN,
		&treestore_storage_engine,
		"TREESTORE",
		"Christiaan Pretorius (c) 2013,2014",
		"TuReeStore MySQL storage engine",
		PLUGIN_LICENSE_GPL,
		treestore_db_init, /* Plugin Init */
		treestore_done, /* Plugin Deinit */
		0x0100 /* 1.0 */,
		NULL,                       /* status variables                */
		treestore_system_variables,                       /* system variables                */
		NULL,                       /* config options                  */
		0,                          /* flags                           */
}
mysql_declare_plugin_end;

#include "poco.cpp"

int main(int argc, char *argv[]){
    pt_test();
    return 0;
}

namespace ts_cleanup{
	class print_cleanup_worker : public Poco::Runnable{
	public:
		void run(){
			nst::u64 last_print_size = calc_total_use();
			while(Poco::Thread::current()->isRunning()){
				Poco::Thread::sleep(1000);
				if(calc_total_use() > treestore_max_mem_use){
					stx::memory_low_state = true;
					reduce_thread_usage();
				}else{
					stx::memory_low_state = false;
				}
				if(llabs(calc_total_use() - last_print_size) > (last_print_size>>2ull)){
					printf
					(	"[%s]l %s m:T%.4g b%.4g c%.4g t%.4g pc%.4g MB\n"
					,	"oo"
					,	"global"
					,	(double)calc_total_use()/units::MB
					,	(double)nst::buffer_use/units::MB
					,	(double)nst::col_use/units::MB
					,	(double)btree_totl_used/units::MB
					,	(double)total_cache_size/units::MB
					);
					last_print_size = calc_total_use();
				}
			}
		}
	};
	static print_cleanup_worker the_worker;
	static Poco::Thread cleanup_thread("ts:cleanup_thread");
	static void start(){
		try{
			cleanup_thread.start(the_worker);
		}catch(Poco::Exception &e){
			printf("Could not start cleanup thread : %s\n",e.name());
		}
	}
};
void start_cleaning(){
	ts_cleanup::start();
}

namespace ts_info{
	void perform_active_stats(const std::string table){
		{
			nst::synchronized s(mut_total_locks);
			++total_locks;
		}
		/// other threads cant delete while this section is active
		nst::synchronized synch2(tt_info_delete_lock);
		typedef std::vector<tree_stored::tree_table*> _Tables;
		_Tables tables;
				
		{
			nst::synchronized synch(tt_info_lock);
			_InfoTables::iterator i = info_tables.find(table);
			if(i!=info_tables.end()){
				tables.push_back((*i).second);
			}
					
		}
		for(_Tables::iterator i = tables.begin();i!=tables.end();++i){
					
			(*i)->begin(true,false);
			(*i)->calc_density();
			(*i)->reduce_use_collum_trees();
			(*i)->rollback();
					
		}	
		{
			nst::synchronized s(mut_total_locks);
			--total_locks;
		}
	}
	void perform_active_stats(){
		{
			nst::synchronized s(mut_total_locks);
			++total_locks;
		}
		/// other threads cant delete while this section is active
		nst::synchronized synch2(tt_info_delete_lock);
		typedef std::vector<tree_stored::tree_table*> _Tables;
		_Tables tables;
				
		{
			nst::synchronized synch(tt_info_lock);
			for(_InfoTables::iterator i = info_tables.begin();i!=info_tables.end();++i){
				tables.push_back((*i).second);
			}		
		}
		for(_Tables::iterator i = tables.begin();i!=tables.end();++i){
					
			(*i)->begin(true,false);
			(*i)->calc_density();
			(*i)->reduce_use_collum_trees();
			(*i)->rollback();
					
		}	
		{
			nst::synchronized s(mut_total_locks);
			--total_locks;
		}
	}
	class info_worker : public Poco::Runnable{
	public:
		void run(){
			
			while(Poco::Thread::current()->isRunning()){
				Poco::Thread::sleep(25000);
				perform_active_stats();
			}
		}
	};
	static info_worker the_worker;
	static Poco::Thread info_thread("ts:info_thread");
	static void start(){
		try{
			info_thread.start(the_worker);
		}catch(Poco::Exception &e){
			printf("Could not start cleanup thread : %s\n",e.name());
		}
	}
};
void start_calculating(){
	/// ts_info::start();
}

