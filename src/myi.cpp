
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
nst::u64 last_read_lookups ;
nst::u64 total_cache_size=0;
nst::u64 ltime = 0;
bool treestore_print_lox = false;
Poco::AtomicCounter writers ;
static std::atomic<nst::u64> total_locks = 0;
static std::atomic<nst::u64> total_threads_locked = 0;
static Poco::Mutex mut_total_locks;
static double tree_factor = 0.9;

static handlerton *static_treestore_hton = NULL;
static int thread_commit(handlerton *hton, THD *thd);
static bool ends_with(std::string const &fullString, std::string const &ending) {
	if (fullString.length() >= ending.length()) {
		return (0 == fullString.compare (fullString.length() - ending.length(), ending.length(), ending));
	} else {
		return false;
	}
}
extern "C" int get_l1_bs_memory_use();
/// determine factored low memory state
static bool is_memory_low(double tree_factor) {

	return (allocation_pool.get_allocated() > treestore_max_mem_use*tree_factor*0.85);// || allocation_pool.is_depleted();

}
static bool is_memory_mark(double tree_factor) {

	return (allocation_pool.get_allocated() > treestore_max_mem_use*tree_factor*0.60);// || allocation_pool.is_depleted();
}
static bool is_memory_lower(double tree_factor) {

	return (allocation_pool.get_allocated() > treestore_max_mem_use*tree_factor*0.55);// || allocation_pool.is_depleted();
}
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
	///NS_STORAGE::total_use+buffer_allocation_pool.get_total_allocated()
	treestore_current_mem_use =
		//nst::buffer_use +
		//nst::col_use +
		nst::total_use +
		buffer_allocation_pool.get_total_allocated() +
		allocation_pool.get_total_allocated() +
		total_cache_size +
		get_l1_bs_memory_use();
	return treestore_current_mem_use;
}
void print_read_lookups();


typedef std::map<std::string, _FileNames > _Extensions;
typedef rabbit::unordered_map<std::string, int > _LoadingData;


Poco::Mutex tree_stored::tree_table::shared_lock;
Poco::Mutex single_writer_lock;
Poco::Mutex data_loading_lock;

tree_stored::tree_table::_SharedData  tree_stored::tree_table::shared;
_LoadingData loading_data;
/// the atomic clock
#include <atomic>
namespace tree_stored{
	/// the table clock
	std::atomic<nst::u64> table_clock;
}

//collums::_LockedRowData* collums::get_locked_rows(const std::string& name){
	//static collums::_RowDataCache rdata;
//	return rdata.get_for_table(name);
//}
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
	class tree_thread;
	void lock_writer(tree_stored::tree_thread* thread,const std::string &path);
	void commit_writer(tree_stored::tree_thread* thread);

	typedef std::map<std::string, tree_table::ptr> _Tables;
	typedef std::map<nst::u64,tree_table::ptr> _LruTables;
	/// there is(can be) only one writing table ever.
	Poco::Mutex& get_wt_lock(){
		static Poco::Mutex v;
		return v;
	}
	_Tables & get_writing_tables(){
		static _Tables tables;
		return tables;
	}
	void remove_writing_table(const std::string name){
		/// only one table via static
		nst::synchronized s(get_wt_lock());
		_Tables & tables = get_writing_tables();
		if(tables.count(name)){
			tree_table::ptr table = tables[name];
			table->commit();
			table->reset_shared();			
			delete table;							
			tables.erase(name);
		}
	}
	tree_table::ptr get_writing_table(TABLE *table_arg, const std::string& name){
		/// only one table via static
		nst::synchronized s(get_wt_lock());
		_Tables & tables = get_writing_tables();
		tree_table::ptr t = tables[name];
		if(t == NULL){
			/// TABLE_SHARE *share= table_arg->s;
			//uint options= share->db_options_in_use;
			t = new tree_table(table_arg,name,true);
			t->check_load(table_arg);
			tables[name] = t;
		}
		return t;
	}
	class tree_thread{
	protected:
		typedef std::vector<Poco::Mutex *> _WriteLocks;
		typedef rabbit::unordered_map<std::string, bool> _LockTable;

		int locks;

		bool changed;
		bool used;
		bool first_throttled;
		Poco::Thread::TID created_tid;
		_WriteLocks writer_locks;
		_LockTable lock_table;
		_Tables read_tables;
		_Tables tables;

		void remove_from_read_tables(const std::string& name){
			auto i = read_tables.find(name);
			if(i != read_tables.end()){
				tree_table::ptr t = (*i).second;
				t->reset_shared();
				delete t;
				read_tables.erase(name);
			}
		}
		tree_table::ptr get_from_read_tables(TABLE *table_arg, const std::string& name){			
			auto i = read_tables.find(name);
			tree_table::ptr t = nullptr;
			if(i == read_tables.end()){
				t = new tree_table(table_arg,name);
				t->check_load(table_arg);
				read_tables[name] = t;
				get_writing_table(table_arg,name); /// make sure theres a writer for every new reader - so that generated statistics can be saved
			}else{
				t = i->second;
			}
			return t;
		}
	public:
		Poco::Mutex reduce_lock;
		int get_locks() const {
			return locks;
		}
		bool is_first_throttled() const {
			return this->first_throttled;
		}
		void set_first_throttled(bool first_throttled){
			this->first_throttled = first_throttled;
		}

		void add_write_lock(const std::string& , Poco::Mutex * lock){
			lock->lock();
			if(writer_locks.empty()) 
				modify();
			writer_locks.push_back(lock);
		}

		void unlock_writelocks(){
			
			tables.clear(); 
			while(!writer_locks.empty()){
				Poco::Mutex * l = writer_locks.back();
				writer_locks.pop_back();
				if(l!=nullptr)
					l->unlock();
			}
		}
		
		tree_thread()
		:	locks(0)
		,	changed(false)
		,	used(false)		
		,	first_throttled(false)
		{
		    inf_print("create tree thread");
			created_tid = Poco::Thread::currentTid();
			DBUG_PRINT("info",("tree thread %ld created\n", created_tid));
			DBUG_PRINT("info",(" *** Tree Store (eyore) mem use configured to %lld MB\n",treestore_max_mem_use/(1024*1024L)));
		}

		Poco::Thread::TID get_created_tid() const {
			return (*this).created_tid;
		}
		void set_used(){
			(*this).used = true;
		}
		void set_unused(){
			(*this).used = false;
		}
		bool get_used() const{
			return (*this).used;
		}
		void reuse(){
			created_tid = Poco::Thread::currentTid();
		}

		~tree_thread(){
			clear();
			DBUG_PRINT("info",("tree thread removed\n"));

		}
		
		void modify(){
			changed = true;
		}
		bool is_writing() const {
			return changed;
		}
		void commit(){
			for(_Tables::iterator t = tables.begin(); t != tables.end();++t){
				(*t).second->commit();
			}			
			this->changed = false;
		}
		void rollback(){			
			for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
				(*t).second->rollback();				
			}
			tables.clear();			
		}
		void save_stats(){
			_Tables writers;
			{
				nst::synchronized s(get_wt_lock());
				writers = get_writing_tables(); /// lock snapshot 
			}

			for(_Tables::iterator t = writers.begin(); t!= writers.end();++t){
				auto table = (*t).second;

				if(table->get_locks() == 0 && (table->should_save_stats() )){
					///|| table->should_calc()
					auto writing_table = start(table,true);
					if(writing_table!=nullptr){									
						table->save_stats();
						release(table->get_path());
						tree_stored::commit_writer(this);
						break;
					}					
					
				}
			}
			
		}
		tree_table::ptr compose_table(const std::string& name){			
			tree_table::ptr t = tables[name];
			if(t == NULL ){
				err_print("Fatal: used table has not been locked");
			}
			return t;
		}
		tree_table::ptr compose_table(TABLE *table_arg, const std::string& name, bool writer){			
			if(writer && lock_table.count(name)==0){
				wrn_print("table not locked");
			}
			tree_table::ptr t = tables[name];
			if(t == NULL ){
				/// TABLE_SHARE *share= table_arg->s;
				//uint options= share->db_options_in_use;
				if(writer){
					/// TODO: hopefully this is locked by current lock
					t = get_writing_table(table_arg, name);				
				}else{
					t = get_from_read_tables(table_arg,name);
				}
				tables[name] = t;

			}else if(writer != t->is_writing()){
				if(!t->is_writing())
					err_print("requesting table in way which is not supported");
			}

			return t;
		}

		void clear(){
			changed = false;
			tables.clear();
		}

		void check_journal(){
			if(get_treestore_journal_size() > (nst::u64)treestore_journal_upper_max){

				if(!locks){
					for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
						(*t).second->rollback();
					}
				}
			}
		}
		tree_table::ptr start(tree_table::ptr table, bool writer){
			
			tree_table::ptr result = table;
			
			if(result == NULL) return result;
			if(writer != result->is_writing()){				
				err_print("changing of lock type not supported");
				return nullptr;				
			}
			if(writer)
				lock_writer(this,table->get_path());			

			tables[table->get_path()] = result;
			lock_table[table->get_path()] = writer;
			
			result->lock(writer);
			read_lookups = 0;
			++locks;
			return result;
		}
		tree_table::ptr start(TABLE *table_arg, const std::string &part, bool writer){
			std::string path = part;
			tree_table::ptr result = NULL;
			if(writer)
				lock_writer(this,path);
			//synchronized _s(p2_lock);
			result = tables[path];
			lock_table[path] = writer;

			if(result == NULL ){
				result = compose_table(table_arg, path, writer);
			}else if(writer != result->is_writing()){
				if(writer){
					tables.erase(path);
					result = compose_table(table_arg, path,writer);
				}else{
					err_print("downgrading of lock not supported");
					return nullptr;
				}
			}
			
			result->lock(writer);
			read_lookups = 0;
			++locks;
			return result;
		}

		void commit_table(const std::string& path){
			bool locked_as = lock_table[path];
			lock_table.erase(path);
			auto ti = tables.find(path);

			if(ti != tables.end()){
				tree_table::ptr t = (*ti).second;
				if(t->is_writing() != locked_as){
					err_print("table modify type not same as lock");
				}
				if(t->is_writing())
					t->commit();
			}
		}
		void release(const std::string &path){

			if(!locks){
				err_print("no locks to release");
				return;
			}
			if(lock_table.count(path)==0){
				err_print("table not locked in this thread %s",path.c_str());
				return;
			}
			bool locked_as = lock_table[path];			
			
			auto ti = tables.find(path);

			if(ti != tables.end()){
				tree_table::ptr t = ti->second;			
				if(t == NULL ){
					err_print("released table never composed");
				}else{
					if(t->is_writing() != locked_as){
						err_print("table modify type not same as lock");
					}
					if(0 == t->unlock()){
						lock_table.erase(path);
					}
				}				
			}else{
				///table probably deleted
			}

			print_read_lookups();

			--locks;

		}
		void create_lru(_LruTables & ordered){
			ordered.clear();
			//if(!is_memory_mark()) return;
			for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){

				if((*t).second!=nullptr)
					ordered[(*t).second->get_clock()] = (*t).second;
				else
					err_print("table entry %s is NULL", (*t).first.c_str());
			}
		}
		void own_reduce_col_trees(_LruTables & ordered){
			DBUG_PRINT("info", ("reducing idle thread collums %.4g MiB\n",(double)stx::storage::total_use/(1024.0*1024.0)));
			for(_LruTables::iterator t = ordered.begin(); t!= ordered.end();++t){
				//if(!is_memory_mark(tree_factor)) break;
				(*t).second->reduce_use_collum_trees();
			}
		}
		void own_reduce_index_trees(_LruTables & ordered){
			DBUG_PRINT("info",("reducing idle thread indexes %.4g MiB\n",(double)stx::storage::total_use/(1024.0*1024.0)));
			for(_LruTables::iterator t = ordered.begin(); t!= ordered.end();++t){
				//if(!is_memory_mark(tree_factor)) break;
				(*t).second->reduce_use_indexes();
			}
		}
		void own_reduce_col_caches(_LruTables & ordered){
			DBUG_PRINT("info", ("reducing idle thread collum caches %.4g MiB\n",(double)calc_total_use()/(1024.0*1024.0)));
			typedef std::multimap<nst::u64, tree_stored::tree_table::ptr> _LastUsedTimes;
			_LastUsedTimes lru;
			for(_LruTables::iterator t = ordered.begin(); t!= ordered.end();++t){
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
		public:

		void own_reduce(){
			//return;
			_LruTables ordered;
			create_lru(ordered);
			own_reduce_index_trees(ordered);
			own_reduce_col_trees(ordered);

			if(calc_total_use() > treestore_max_mem_use){
				own_reduce_col_caches(ordered);
			}
		}

		void reduce_col_trees(){

			if(!locks){
				_LruTables ordered;
				create_lru(ordered);
				own_reduce_col_trees(ordered);
			}

		}
		void reduce_col_trees_only(){
			return;
			if(!locks){
				_LruTables ordered;
				create_lru(ordered);
				DBUG_PRINT("info", ("reducing idle thread collums %.4g MiB\n",(double)stx::storage::total_use/(1024.0*1024.0)));
				for(_Tables::iterator t = tables.begin(); t!= tables.end();++t){
					if((*t).second)
						(*t).second->reduce_use_collum_trees_only();
					else
						err_print("table entry %s is NULL", (*t).first.c_str());
				}
			}

		}
		void remove_table(const std::string name){
			tables.erase(name);
			remove_from_read_tables(name);
			remove_writing_table(name);
		}
		void reduce_index_trees(){

			if(!locks){
				_LruTables ordered;
				create_lru(ordered);
				own_reduce_index_trees(ordered);
			}
		}

		void reduce_col_caches(){

			if(!locks){
				_LruTables ordered;
				create_lru(ordered);
				own_reduce_col_caches(ordered);
			}

		}

	};
}; // tree_stored


class static_threads{
public:
	
	typedef rabbit::unordered_map<Poco::Thread::TID,tree_stored::tree_thread*> _Writers;
	typedef rabbit::unordered_map<std::string,std::shared_ptr<Poco::Mutex>> _WriterLocks;
private:
	mutable Poco::Mutex tlock;
	mutable Poco::Mutex plock;
	_Writers writers;
	_WriterLocks locks;
	Poco::Mutex* get_write_lock(const std::string &path){
		std::shared_ptr<Poco::Mutex> result = nullptr;
		NS_STORAGE::syncronized s(plock);
		auto w = locks.find(path);
		if(w == locks.end()){
			result = std::make_shared<Poco::Mutex>();
			locks[path] = result;
		}else{
			result = (*w).second;
		}
		return result.get();
	}

	tree_stored::tree_thread* map_thread(Poco::Thread::TID curt){
		tree_stored::tree_thread* result = nullptr;

		auto w = writers.find(curt);
		if(w == writers.end()){
			result = new tree_stored::tree_thread();
			writers[curt] = result;
		}else{
			result = (*w).second;
		}
		return result;
	}
public:
	static_threads() {
	}
	void reduce_idle_writers(){
		//nst::synchronized s(tlock);
		for(_Writers::iterator w = writers.begin(); w != writers.end(); ++w){
			tree_stored::tree_thread* writer = (*w).second;
			if(!writer->get_used()){
				NS_STORAGE::syncronized ul(writer->reduce_lock);
				if(!writer->get_used()){
					//writer->reduce_col_trees();
					//writer->reduce_index_trees();
					writer->own_reduce();
				}
			}
		}
	}
	void save_idle_writers_stats(){
		//nst::synchronized s(tlock);
		
		tree_stored::tree_thread* writer = get_thread();				
		writer->save_stats();								
		
	}
	tree_stored::tree_thread *get_thread(){
		
		Poco::Thread::TID curt = Poco::Thread::currentTid();
		tree_stored::tree_thread* writer = nullptr;
		{
			nst::synchronized s(tlock);
			writer = map_thread(curt);
		}
		nst::synchronized sr(writer->reduce_lock);		
		writer->set_used();
		return writer;
	}	

	void lock_writer(tree_stored::tree_thread* writer, const std::string& path){
		Poco::Mutex* write_lock = get_write_lock(path);
		
		writer->add_write_lock(path,write_lock);		
	}
	size_t get_thread_count()  {
		NS_STORAGE::syncronized ul(tlock);
		return writers.size();
	}
	/// should happen only once when all the locks are discounted
	bool release_writer(tree_stored::tree_thread * w){
		if(w->get_used()){
			w->unlock_writelocks();
			w->set_unused();
			return true;
		}else{
			return false;
		}
		
	}
	void remove_table(const std::string &name){
		_Writers temp;

		{
			NS_STORAGE::syncronized ul(tlock);
			temp = this->writers;
			for(auto w = temp.begin(); w != temp.end(); ++w){
				tree_stored::tree_thread * wt = (*w).second;
				NS_STORAGE::syncronized wl(wt->reduce_lock);
				wt->remove_table(name);
			}
		}
		
	}

	
	void check_use(){
		/// TODO: this isnt safe yet (reducing another threads memory use)

		if(stx::memory_mark_state){

		}

	}
	void release_idle_trees(){

	}


public:
	void check_journal(){
		/// TODO: NB: do it for writers
		//for(_Threads::iterator t = threads.begin(); t != threads.end(); ++t){
		//	(*t)->check_journal();
		//}
	}
	bool commit(tree_stored::tree_thread * thread){
		if(thread != NULL){
			if(thread->get_locks()==0){
				bool writing = thread->is_writing();	
				if(writing){
					thread->commit();		
					
					thread->clear();		
					if(writing){			
						NS_STORAGE::journal::get_instance().synch( ); /// synchronize to storage - second phase						
					}
					release_writer(thread);				
					
				}else{
				
				}				
			}else{
				wrn_print("lock references to tables must be released before committing a thread(thread::release(table))");
			}
		}
		return true;
	}
};

static static_threads st;
namespace tree_stored{
	void lock_writer(tree_stored::tree_thread* thread,const std::string &path){
		st.lock_writer(thread, path);
	}
	void commit_writer(tree_stored::tree_thread* thread){
		st.commit(thread);
	}
};

void print_read_lookups(){
}
tree_stored::tree_thread** thd_to_tree_thread(THD* thd, handlerton* hton){
	return(tree_stored::tree_thread**) thd_ha_data(thd, hton );
}

void clear_thread(THD* thd,handlerton* hton){
	//(*thd_to_tree_thread(thd,hton)) = NULL;	
	 thd_set_ha_data(thd, hton, nullptr);
}

tree_stored::tree_thread * thread_from_thd(THD* thd,handlerton* hton){
	if(thd==NULL) return NULL;
	tree_stored::tree_thread** stpp = thd_to_tree_thread(thd,hton);
	return *stpp;
}

tree_stored::tree_thread * new_thread_from_thd(THD* thd, handlerton* hton){
	tree_stored::tree_thread** stpp = thd_to_tree_thread(thd,hton);
	if(*stpp == NULL)
		*stpp = st.get_thread();
	if(*stpp == NULL){
		err_print("the thread thd is NULL");
	}
	return *stpp;
}

class ha_treestore: public handler{
public:

	static const int TREESTORE_MAX_KEY_LENGTH = stored::StandardDynamicKey::MAX_BYTES;
	std::string path;
	tree_stored::tree_table::ptr tt;
	
	stored::_Rid row;

	typedef tree_stored::_Selection  _Selection;
	typedef tree_stored::_SetFields  _SetFields;
	typedef std::vector<Field*> _FieldMap;
	stored::_Rid counter;

	struct _SelectionState{
		_SelectionState() : last_resolved(0){
		}
		tree_stored::_Selection selected;// direct selection filter
		_FieldMap field_map;	
		_SetFields fields_set;
		stored::_Rid last_resolved;

		void clear_output_selection(){
			for(_Selection::iterator s = this->selected.begin(); s != this->selected.end(); ++s){
				(*s).restore_ptr();
			}
			this->selected.clear();
		}

		void rebase_selection_io(byte* io){
			_Selection::iterator s = this->selected.begin();
			for(; s != this->selected.end(); ++s){
				tree_stored::selection_tuple & sel = (*s);
				if(sel.base_ptr != io){
					sel.restore_ptr();
					nst::u32 findex = sel.field->field_index;
					sel.saved_ptr = sel.field->ptr;
					sel.field->ptr = io + sel.field->offset(sel.base_ptr);
				}else{
					break;
				}
			}
		}
		void init_fields_set(int cnt){
			fields_set.resize(cnt);
		}
		void restore_selection_io(){
			_Selection::iterator s = this->selected.begin();
			for(; s != this->selected.end(); ++s){
				tree_stored::selection_tuple & sel = (*s);
				if(sel.saved_ptr == sel.field->ptr){
					break;
				}
				sel.restore_ptr();
			}
		}
		void clear_fields_set(){
			for(int c= 0; c < fields_set.size(); ++c){
				fields_set[c] = false;
			}
		}
		void set_field(int index, bool f){
			fields_set[index] = f;
		}
		bool is_field_set(int index) const {
			return fields_set[index];
		}
	};
	_SelectionState _selection_state;

	stored::index_interface * current_index;
	stored::index_iterator_interface * current_index_iterator;
	tree_stored::tree_thread * writer_thread; /// set when write lock
	THD* locked_thd;
	inline stored::index_iterator_interface& get_index_iterator(){

		if(current_index_iterator==nullptr)
			current_index_iterator = current_index->get_index_iterator((nst::u64)this);
		return *current_index_iterator;
	}

	tree_stored::tree_thread* get_thread(THD* thd){
		return new_thread_from_thd(thd,ht);
	}

	tree_stored::tree_thread* get_thread(){
		return new_thread_from_thd(ha_thd(),ht);
	}
	tree_stored::tree_table::ptr get_reader_tree_table(){		
		return get_thread()->compose_table((*this).table, this->path, false);
	}
	tree_stored::tree_table::ptr get_tree_table(){
		if(tt==NULL){
			tt = get_thread()->compose_table((*this).table, this->path, false);
			tt->check_load((*this).table);
		}
		
		return tt;
	}
	void check_tree_table(){		
		get_tree_table()->check_load((*this).table);		
	}
	void create_selection_map(_SelectionState &selection_state){
		if(treestore_resolve_values_from_index==TRUE){
			selection_state.field_map.resize(get_tree_table()->get_col_count());
			for(_Selection::iterator s = selection_state.selected.begin(); s != selection_state.selected.end(); ++s){
				selection_state.field_map[(*s).field->field_index] = (*s).field;
			}
		}
	}

	void clear_selection(_SelectionState &selection_state){
		selection_state.clear_output_selection();
	}

	void initialize_selection(_SelectionState &selection_state,uint keynr,size_t col_count){
		counter=0;
		clear_selection(selection_state);
		selection_state.init_fields_set(get_tree_table()->get_col_count());
		selection_state.selected = get_tree_table()->create_output_selection(table);
		if(keynr < get_tree_table()->get_indexes().size()){
			create_selection_map(selection_state);
		}
	}

	void clear_fields_set(_SelectionState& selection_state){
		
		selection_state.clear_fields_set();
	}

	ha_treestore
	(	handlerton *hton
	,	TABLE_SHARE *table_arg
	)
	:	handler(hton, table_arg)
	,	tt(NULL)
	,	row(0)
	,	current_index(NULL)
	,	writer_thread(NULL)
	,	locked_thd(NULL)
	{

	}

	~ha_treestore()
	{
	}


	int rnd_pos(uchar * buf,uchar * pos){
		memcpy(&row, pos, sizeof(row));
		resolve_selection(row,this->_selection_state);

		return 0;
	}

	void position(const uchar *){
		memcpy(this->ref, &row, sizeof(row));
	}

	int info(uint which){

		if(get_tree_table() == NULL){
			return 0;
		}

		++total_locks;

		if (thd_sql_command(ha_thd()) == SQLCOM_TRUNCATE) {
			DBUG_PRINT("info",("the table is being truncated\n"));
		}
		if (which & HA_STATUS_ERRKEY) {
			if(get_tree_table()->error_index!=nullptr)
				errkey = get_tree_table()->error_index->get_ix();			
			if(which == HA_STATUS_ERRKEY)
				return 0;
			//if(which && HA_STATUS_NO_LOCK)
			//	return 0;
		}
		
		tree_stored::tree_thread * thread = st.get_thread();
		
		
		tree_stored::table_info tt ;
		if(thread->is_writing()){
			tree_stored::tree_table::ptr ts = thread->compose_table((*this).table, this->path, thread->is_writing());				
			ts->get_calculated_info(tt);
		}else{
			tree_stored::tree_table::ptr ts = thread->start((*this).table, this->path, thread->is_writing());				
			if(ts->should_calc()){
				
				ts->calc_density();					
				inf_print("calculating stats [%s] table size %lld",this->path.c_str(),(nst::u64)tt.table_size);
			}
			ts->get_calculated_info(tt);
			thread->release(this->path);			
		}
		if (which & HA_STATUS_AUTO)
		{

			/// stats.auto_increment_value =  ts->get_auto_incr() ;

		}


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

			for (ulong	i = 0; i < table->s->keys; i++) {
				if(i < tt.calculated.size()){
					for (ulong j = 0; j < table->key_info[i].actual_key_parts; j++) {
						if(j < tt.calculated[i].density.size()){
							//std::cout << "-[" << table->key_info[i].name << " (" << i << "," << j << ") " << tt.calculated[i].density[j] << "]" << std::endl;
							table->key_info[i].rec_per_key[j] = tt.calculated[i].density[j];
						}
					}
				}
			}
		}

		if(which & HA_STATUS_VARIABLE) {// - records, deleted, data_file_length, index_file_length, delete_length, check_time, mean_rec_length

			stats.data_file_length = tt.table_size;
			stats.block_size = 4096;
			stats.records = tt.row_count;
			stats.mean_rec_length =(ha_rows) stats.records ? ( stats.data_file_length / (stats.records+1) ) : 1;

			for (ulong	i = 0; i < table->s->keys; i++) {
				if(i < tt.calculated.size()){
					for (ulong j = 0; j < table->key_info[i].actual_key_parts; j++) {
						if(j < tt.calculated[i].density.size()){
							//std::cout << "*[" << this->path << "." << table->key_info[i].name << " (" << i << "," << j << ") " << tt.calculated[i].density[j] << "]" << std::endl;
							table->key_info[i].rec_per_key[j] = tt.calculated[i].density[j];
						}
					}
				}
			}
		}



		if(which & HA_STATUS_ERRKEY) // - status pertaining to last error key (errkey and dupp_ref)
		{}

		--total_locks;

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
		return ( HA_READ_ORDER | HA_READ_NEXT | HA_READ_RANGE | HA_KEYREAD_ONLY); //| HA_READ_PREV
	}
	uint max_supported_record_length() const { return HA_MAX_REC_LENGTH*8; }
	uint max_supported_keys()          const { return MAX_KEY; }
	uint max_supported_key_parts()     const { return MAX_REF_PARTS; }
	uint max_supported_key_length()    const { return TREESTORE_MAX_KEY_LENGTH; }
	uint max_supported_key_part_length() const { return TREESTORE_MAX_KEY_LENGTH; }
	const key_map *keys_to_use_for_scanning() { return &key_map_full; }
	Table_flags table_flags(void) const{
		return (	HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |HA_TABLE_SCAN_ON_INDEX |//|HA_PRIMARY_KEY_IN_READ_INDEX
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
		try{
			return((double) (std::max<nst::u64>(1, get_reader_tree_table()->table_size())/2048));/*assume an average page size of 8192 bytes*/
		}catch(tree_stored::InvalidTablePointer&){
			err_print("invalid table pointer");
		}
		return 1.0f;
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
		try{
			time_for_scan = scan_time();

			if ((total_rows =  get_reader_tree_table()->shared_row_count()) < rows) {

				return(time_for_scan);
			}
		}catch(tree_stored::InvalidTablePointer&){
			err_print("invalid table pointer");
		}

		return(ranges + (double) rows / (double) total_rows * time_for_scan);
	}

	int create(const char *n,TABLE *t,HA_CREATE_INFO *create_info){

		int r = delete_table(n);///t->s->path.str
		if(r != 0) return r;

		THD* thd = ha_thd();


		if
		(
			(
				(	create_info->used_fields & HA_CREATE_USED_AUTO
				)
				||	thd_sql_command(thd) == SQLCOM_ALTER_TABLE
				||	thd_sql_command(thd) == SQLCOM_OPTIMIZE
				||	thd_sql_command(thd) == SQLCOM_CREATE_INDEX
			)
		&&	create_info->auto_increment_value > 0
		){
			bool writer = false;
			tree_stored::tree_thread* thread = st.get_thread();
			if(thread==NULL){
				err_print("could not allocate thread");
				DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
			}
			
			tree_stored::tree_table::ptr tt = thread->start(t, n, writer);
			if(tt==NULL){
				err_print("could not lock table");
				DBUG_RETURN(HA_ERR_READ_ONLY_TRANSACTION);
			}
			tt->reset_auto_incr(create_info->auto_increment_value);
			
			thread->release(n);
			thread->rollback();					
			//st.release_writer(thread);
			
			
		}

		this->path = n;
		//path = t->s->path.str;
		return 0;
	}

	int open(const char *n,int,uint){
		this->path = n;
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
		//nst::synchronized swl(single_writer_lock);
	
		int r = 0;
		std::string from = _from;
		std::string to = _to;

		_FileNames files = extensions_from_table_name(from);

		for(_FileNames::iterator f = files.begin(); f != files.end(); ++f){
			while(collums::get_loading_data((*f))!=0){
				os::zzzz(100);
			}
		}

		
		st.remove_table(from);

		NS_STORAGE::journal::get_instance().synch( true ); /// synchronize to storage

		std::string extenstion = TREESTORE_FILE_EXTENSION;
		for(_FileNames::iterator f = files.begin(); f != files.end(); ++f){
			std::string name = (*f);
			using Poco::File;
			using Poco::Path;
			try{
				std::string next = name + extenstion;
				if(stored::erase_abstracted_storage(next)){
					inf_print("renaming %s",next.c_str());
					nst::delete_temp_files_of(next.c_str());
					File df (next);
					if(df.exists()){
						std::string renamed = to + &next[from.length()];
						df.moveTo(renamed);
					}
				}else{
					r = HA_ERR_TABLE_EXIST;
				}
			}catch(std::exception& ){
				err_print("could not rename table file %s", name.c_str());
				r = HA_ERR_NO_SUCH_TABLE;
			}
			///
		}
		try{
			using Poco::File;
			using Poco::Path;
			std::string nxt = from + extenstion;
			if(stored::erase_abstracted_storage(nxt)){

				File df (nxt);
				if(df.exists()){
					std::string renamed = to + &nxt[from.length()];
					inf_print("renaming %s to %s",nxt.c_str(),renamed.c_str());
					df.moveTo(renamed);
				}
			}else{
				r = HA_ERR_TABLE_EXIST;
			}
		}catch(std::exception& ){
			err_print("could not rename table file %s", from.c_str());
			r = HA_ERR_TABLE_EXIST;

		}



		return r;
	}
	int delete_table (const char * name){
		DBUG_PRINT("info",("deleting files %s\n", name));
		///nst::synchronized swl(single_writer_lock);
		int r = 0;
		
		_FileNames files = extensions_from_table_name(name);

		for(_FileNames::iterator f = files.begin(); f != files.end(); ++f){
			while(collums::get_loading_data((*f))!=0){
				os::zzzz(100);
			}
		
		}
		tree_stored::tree_thread * thread = st.get_thread();
		thread->remove_table(name);
		st.remove_table(name);

		NS_STORAGE::journal::get_instance().synch(true); /// synchronize trx log to storage

		std::string extenstion = TREESTORE_FILE_EXTENSION;
		for(_FileNames::iterator f = files.begin(); f != files.end(); ++f){

			std::string name = (*f);
			using Poco::File;
			using Poco::Path;
			try{
				std::string next = name + extenstion;
				if(stored::erase_abstracted_storage(next)){
					inf_print("deleting %s",next.c_str());
					nst::delete_temp_files_of(next.c_str());
					File df (next);
					if(df.exists()){
						df.remove();
					}
				}
			}catch(std::exception& ){
				err_print("could not delete table file %s", name.c_str());
				r = HA_ERR_TABLE_EXIST;

			}
			///
		}
		try{
			using Poco::File;
			using Poco::Path;
			std::string nxt = name + extenstion;
			if(stored::erase_abstracted_storage(nxt)){
				inf_print("deleting %s",nxt.c_str());
				File df (nxt);
				if(df.exists()){
					df.remove();
				}
			}else{
				r = HA_ERR_TABLE_EXIST;
			}
		}catch(std::exception& ){
			err_print("could not delete table file %s", name);
			r = HA_ERR_TABLE_EXIST;

		}
		if(r == 0){
			/// the lock to the deleted table must also be removed

		}
		//r = handler::delete_table(name);
		return r;
	}

	int truncate(){
		std::string todo = (*this).path;
		bool toopen = (tt != NULL);
		if(toopen)
			close();
		int r = delete_table(todo.c_str());
		if(toopen)
			open( todo.c_str() ,0,0);
		return r;
	}
	int delete_all_rows(){
		return truncate();
	}
	void clear_state(){
		clear_selection(this->_selection_state);
		this->tt = nullptr;
		tt = NULL;
		current_index = NULL;
		current_index_iterator = NULL;
		locked_thd = NULL;
	}
	int close(void){
		DBUG_PRINT("info",("closing tt %s\n", table->alias));
		inf_print("closing tt %s", table->alias);
		clear_selection(this->_selection_state);	
		if(tt!= NULL)
			tt->rollback();
		tt = NULL;
		current_index = NULL;
		current_index_iterator = NULL;
		return 0;
	}

	void start_bulk_insert(ha_rows rows){
		DBUG_ENTER("handler::ha_start_bulk_insert");
	
		DBUG_VOID_RETURN;
	}

	THR_LOCK_DATA **store_lock(THD * thd,THR_LOCK_DATA ** to,thr_lock_type lt){
		DBUG_ENTER("ha_treestore::store_lock");

		DBUG_RETURN(to);
	}
	int start_stmt(THD *thd, thr_lock_type lock_type){
		int r= external_lock(thd, lock_type);

		return r;
	}
	void check_own_use(){
		if(stx::memory_mark_state){

			if(writer_thread!=nullptr){
				writer_thread->own_reduce();
			}else{
				get_thread()->own_reduce();
			}

		}
	}
	
	int external_lock(THD *thd, int lock_type){

		DBUG_ENTER("::external_lock");

		if(table == NULL){
			err_print("table cannot be locked - invalid argument");
			return 0;
		}
		bool is_ddl = 
				thd_sql_command(thd) == SQLCOM_DROP_TABLE 
			||	thd_sql_command(thd) == SQLCOM_ALTER_TABLE
			||	thd_sql_command(thd) == SQLCOM_CREATE_INDEX	
			||	thd_sql_command(thd) == SQLCOM_DROP_INDEX;

		bool writer = false;
		if(lock_type == F_WRLCK){
			writer = true;
		}
		bool processed= false;
		bool high_mem = calc_total_use() > treestore_max_mem_use ;
		tree_stored::tree_thread * thread = new_thread_from_thd(thd,ht); /// there can only be one of these per thread
		if(total_locks==0){
			nst::synchronized s(mut_total_locks);
			if(total_locks==0){

				if
				(	get_treestore_journal_size() > (nst::u64)treestore_journal_upper_max
				//||	high_mem
				)
				{
					st.check_journal();/// function releases all idle reading transaction locks
					NS_STORAGE::journal::get_instance().synch( high_mem ); /// synchronize to storage

				}
				if(high_mem){
					//stored::reduce_all();
				}

			}
		}
		if(treestore_print_lox){
			if (lock_type == F_RDLCK || lock_type == F_WRLCK || lock_type == F_UNLCK)
				inf_print
				(	"[%s]l %s m:T%.4g b%.4g c%.4g [s]%.4g t%.4g pc%.4g pool %.4g MB"
				,	lock_type == F_UNLCK ? "-":"+"
				,	table->s->normalized_path.str
				,	(double)calc_total_use()/units::MB
				,	(double)nst::buffer_use/units::MB
				,	(double)nst::col_use/units::MB
				,	(double)nst::stl_use/units::MB
				,	(double)btree_totl_used/units::MB
				,	(double)total_cache_size/units::MB
				,	(double)(buffer_allocation_pool.get_total_allocated() + allocation_pool.get_total_allocated())/units::MB
				);
		}
		
		bool is_autocommit = !thd->in_multi_stmt_transaction_mode(); //thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
		BOOL is_manualcommit = thd->in_multi_stmt_transaction_mode();

		if (lock_type == F_RDLCK || lock_type == F_WRLCK){
			processed = true;
			this->locked_thd = thd;			
			++total_locks;
			if(thread->get_locks()==0){
				
				trans_register_ha(thd, is_manualcommit, ht); /// register the commit callback - finally !!
				++total_threads_locked;
				if(total_threads_locked > treestore_max_thread_concurrency){
					
					DBUG_PRINT("info",("throtling threads"));
					//thread->set_first_throttled(true);

				}
			}
			/// the table structures must be composed
			
			if(get_treestore_journal_size() > (nst::u64)treestore_journal_upper_max){
				NS_STORAGE::journal::get_instance().synch(true);
				if(get_treestore_journal_size() > (nst::u64)treestore_journal_upper_max)
					return HA_ERR_LOCK_TABLE_FULL;
			}
			DBUG_PRINT("info", (" *locking %s->%s \n", table->s->normalized_path.str,this->path.c_str()));
			
			check_tree_table();
			(*this).tt = thread->start(table, this->path, writer);
			if((*this).tt == nullptr){
				DBUG_RETURN(HA_ERR_READ_ONLY_TRANSACTION);
			}
			if(writer)
				(*this).writer_thread = thread;
			else
				(*this).writer_thread = nullptr;

		}else if(lock_type == F_UNLCK){
			processed = true;
			DBUG_PRINT("info", (" -unlocking %s \n", this->path.c_str()));			
			//thread->commit_table(this->path);
			thread->release(this->path);
			if(thread->get_locks()==0){
				
				this->writer_thread = nullptr;
				if(treestore_reduce_tree_use_on_unlock==TRUE){
					thread->reduce_col_trees_only();
					if(treestore_reduce_index_tree_use_on_unlock==TRUE)
						thread->reduce_index_trees();
				}
				
				clear_selection(this->_selection_state);
				if(is_autocommit){
					if(this->locked_thd != thd){
						err_print("locked thd not equal to current thd");
					}else{
						//this->current_index_iterator = NULL;
						thread_commit(ht,thd);
						//this->clear_state();
					}									
				}
				this->locked_thd = NULL;
				--total_threads_locked;
				if(treestore_print_lox){
					inf_print
					(	"%s m:T%.4g b%.4g c%.4g [s]%.4g t%.4g pc%.4g pool %.4g / %.4g MB"
					,	"transaction complete"
					,	(double)calc_total_use()/units::MB
					,	(double)nst::buffer_use/units::MB
					,	(double)nst::col_use/units::MB
					,	(double)nst::stl_use/units::MB
					,	(double)btree_totl_used/units::MB
					,	(double)total_cache_size/units::MB
					,	(double)allocation_pool.get_used()/units::MB
					,	(double)allocation_pool.get_total_allocated()/units::MB
					);
				}
				DBUG_PRINT("info",("transaction finalized : %s\n",table->alias));

			}


			--total_locks;

		}
		if(!processed){
			err_print("lock event not processed");
		}

		DBUG_RETURN(0);
	}

/// TODO: the 5.7 internal parsers have changed so this code may need to be rewritten

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
				case Item::STRING_ITEM: case Item::INT_ITEM:
				case Item::REAL_ITEM:case Item::VARBIN_ITEM:
				case Item::DECIMAL_ITEM:{

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
#if 0
#endif
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
		if(get_tree_table()->has_primary_key()){
			int r = this->index_init(get_tree_table()->get_primary_key(),true);			
			current_index->first(get_index_iterator());
			get_index_iterator().first();
		
			return r;
		}
		row = 0;
		tt = NULL;
		st.check_use();
		/// cond_pop();

		this->_selection_state.last_resolved = 0;

		initialize_selection(this->_selection_state,get_tree_table()->get_indexes().size(),get_tree_table()->get_col_count());
		get_tree_table()->init_iterators();

		return 0;
	}
	// tscan 4

	int rnd_next(byte *buf){
		
		DBUG_ENTER("rnd_next");
		if((counter&255)==0){
			check_own_use();
		}
		++counter;

		if(get_tree_table()->has_primary_key()){
			int r = HA_ERR_END_OF_FILE;
			stored::index_iterator_interface & current_iterator = get_index_iterator();
			if(current_iterator.valid()){
				ha_statistic_increment(&SSV::ha_read_rnd_next_count);
				r = 0;
				table->status = 0;
				
				rebase_selection_io(buf,this->_selection_state);
				resolve_selection_from_index(active_index,this->_selection_state,current_iterator);
				restore_selection_io(this->_selection_state);				
				current_iterator.next();
			
				
			}
			DBUG_RETURN(r);
		}
		
		if(get_tree_table()->is_table_end()){
			get_tree_table()->pop_all_conditions();
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		this->_selection_state.last_resolved = (*this).get_tree_table()->get_r_value();
		stored::_Rid c = tree_stored::abstract_conditional_iterator::NoRecord;
		while(c != this->_selection_state.last_resolved){
			/// get the next record that matches
			/// returns last_resolved if current i.e. first record matches
			c = (*this).get_tree_table()->iterate_conditions(this->_selection_state.last_resolved);
			/// c is either valid or NoRecord
			if(c == tree_stored::abstract_conditional_iterator::NoRecord){
				/// no record inclusive of last_resolved matched
				get_tree_table()->pop_all_conditions();
				DBUG_RETURN(HA_ERR_END_OF_FILE);
			}

			if(c > this->_selection_state.last_resolved + 20){

				get_tree_table()->move_iterator(c);

			}else{

				if(!get_tree_table()->iterate_r_conditions(c)){
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}
			}
			/// c == last_resolved
			this->_selection_state.last_resolved = (*this).get_tree_table()->get_r_value();
		}

		ha_statistic_increment(&SSV::ha_read_rnd_next_count);
		table->status = 0;
		resolve_selection(this->_selection_state.last_resolved,this->_selection_state);
		if((this->_selection_state.last_resolved % 1000000) == 0){
			inf_print("resolved %lld rows in table %s", (long long)this->_selection_state.last_resolved,table->s->path.str);
		}
		get_tree_table()->iterate_r();
		DBUG_RETURN(0);
	}

	int delete_row(const byte *buf){
		ha_statistic_increment(&SSV::ha_delete_count);

		get_tree_table()->erase(this->_selection_state.last_resolved, (*this).table);


		return 0;
	}
	void check_auto_incr(byte * buf){



	}
	int extra(enum ha_extra_function operation){

		return 0;
	}
	nst::u64 writes;
	int write_row(byte * buff){
		if(((writes%1000ll)==0) || stx::memory_mark_state){
			check_own_use();

		}
		++writes;
		ha_statistic_increment(&SSV::ha_write_count);
		/// TODO: auto timestamps

		bool auto_increment_update_required= (table->next_number_field != NULL);
		bool have_auto_increment= table->next_number_field && buff == table->record[0];
		if (have_auto_increment){
			if(auto_increment_update_required){
				int e = update_auto_increment();
				if(e) return e;
				get_tree_table()->set_calc_max_rowid(table->next_number_field->val_int());
			}

		}
		return get_tree_table()->write((*this).table);
	}

	std::vector<bool> change_set;
	inline nst::u32 get_field_offset(const TABLE* table, const Field* f){
		return((nst::u32) (f->ptr - table->record[0]));
	}
	nst::u32
	mach_read_from_1
	(	const byte*	b
	){
		return((nst::u32)(b[0]));
	}
	nst::u32
	mach_read_from_2_little_endian
	(	const byte* buf
	){
		return((nst::u32)(buf[0]) | ((nst::u32)(buf[1]) << 8));
	}
	const byte*
	row_mysql_read_true_varchar
	(	nst::u32* len
	,	const byte*	field
	,	nst::u32 lenlen
	){
		if (lenlen == 2) {
			*len = mach_read_from_2_little_endian(field);

			return(field + 2);
		}

		*len = mach_read_from_1(field);

		return(field + 1);
	}

	nst::u32
	mach_read_from_n_little_endian
	(	const byte* buf
	,	nst::u32 buf_size
	){
		nst::u32	n	= 0;
		const byte*	ptr;

		ptr = buf + buf_size;

		for (;;) {
			ptr--;

			n = n << 8;

			n += (nst::u32)(*ptr);

			if (ptr == buf) {
				break;
			}
		}

		return(n);
	}
	const byte*
	row_mysql_read_blob_ref
	(	nst::u32*		len
	,	const byte*	ref
	,	nst::u32		col_len
	){
		byte*	data;

		*len = mach_read_from_n_little_endian(ref, col_len - 8);

		memcpy(&data, ref + col_len - 8, sizeof data);

		return(data);
	}
	void calculate_change_set(const byte *old_row, byte *new_row){
		nst::u32 invalid_value = std::numeric_limits<nst::u32>::max();

		enum_field_types field_mysql_type;
		nst::u32 n_fields;
		nst::u32 o_len;
		nst::u32 n_len;
		nst::u32 col_pack_len;

		const byte*	o_ptr;
		const byte*	n_ptr;

		nst::u32 col_type;

		n_fields = table->s->fields;
		change_set.resize(n_fields);
		for (nst::u32 i = 0; i < n_fields; i++) {
			Field *field = table->field[i];

			o_ptr = (const byte*) old_row + get_field_offset(table, field);
			n_ptr = (const byte*) new_row + get_field_offset(table, field);

			col_pack_len = field->pack_length();

			o_len = col_pack_len;
			n_len = col_pack_len;

			field_mysql_type = field->type();

			switch (field_mysql_type) {
			case MYSQL_TYPE_BLOB:
				o_ptr = row_mysql_read_blob_ref(&o_len, o_ptr, o_len);
				n_ptr = row_mysql_read_blob_ref(&n_len, n_ptr, n_len);

				break;
			case MYSQL_TYPE_NEWDECIMAL:
				break;
			case MYSQL_TYPE_VARCHAR:
				o_ptr = row_mysql_read_true_varchar(&o_len, o_ptr,(nst::u32)(((Field_varstring*) field)->length_bytes));
				n_ptr = row_mysql_read_true_varchar(&n_len, n_ptr,(nst::u32)(((Field_varstring*) field)->length_bytes));
				break;
			case MYSQL_TYPE_STRING:
				break;
			default:
				;
			}


			if (field->real_maybe_null()) {
				if (field->is_null_in_record(old_row)) {
					o_len = invalid_value;
				}

				if (field->is_null_in_record(new_row)) {
					n_len = invalid_value;
				}
			}
			bool changed =(o_len != n_len || (o_len != invalid_value && 0 != memcmp(o_ptr, n_ptr, o_len)));
			change_set[field->field_index] = changed;


		}
	}
	int update_row(const byte *old_data, byte *new_data) {
		ha_statistic_increment(&SSV::ha_update_count);
		calculate_change_set(old_data, new_data);
		/// TODO: check if any indexes are affected before doing this
		get_tree_table()->erase_row_index(this->_selection_state.last_resolved,change_set);/// remove old index entries
		/// the write set will contain the new values

		get_tree_table()->update(this->_selection_state.last_resolved, (*this).table, change_set); /// write row and create indexes

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

		initialize_selection(this->_selection_state,keynr,get_tree_table()->get_col_count());
		current_index = get_tree_table()->get_index_interface(handler::active_index);
		current_index_iterator = current_index->get_index_iterator((nst::u64)this);
		//readset_covered = get_tree_table()->read_set_covered_by_index(table, active_index, this->_selection_state);


		return 0;
	}

	inline stored::_Rid key_to_rid
	(	uint ax
	,	const tree_stored::CompositeStored& input
	)
	{
		return input.row;
	}
	inline stored::_Rid key_to_rid
	(	uint ax
	,	const stored::BareKey& input
	)
	{
		return input.row;
	}
	void rebase_selection_io(byte* io,_SelectionState& selection_state){
		selection_state.rebase_selection_io(io);
	}
	void restore_selection_io(_SelectionState& selection_state){
		_selection_state.restore_selection_io();

	}
	void resolve_selection_with_index(stored::_Rid row, _SelectionState& selection_state){
		selection_state.last_resolved = row;

		_Selection::const_iterator s = selection_state.selected.begin();
		for(; s != selection_state.selected.end(); ++s){
			const tree_stored::selection_tuple & sel = (*s);
			nst::u32 findex = sel.field->field_index;
			if(!selection_state.is_field_set(findex)){
				sel.col->seek_retrieve(row,sel.field);
			}

		}

	}
	void resolve_selection(stored::_Rid row, _SelectionState &selection_state){
		selection_state.last_resolved = row;

		_Selection::const_iterator s = selection_state.selected.begin();

		for(; s != selection_state.selected.end(); ++s){
			const tree_stored::selection_tuple & sel = (*s);
			//bool flip = !bitmap_is_set(table->write_set,  sel.field->field_index);
			//if(flip)
			//	bitmap_set_bit(table->write_set,  sel.field->field_index);
			nst::u32 findex = sel.field->field_index;
			sel.col->seek_retrieve(row,sel.field);
			//if(flip)
			//	bitmap_flip_bit(table->write_set,  sel.field->field_index);

		}
	}
	template<typename _KeyType>
	void resolve_selection_from_index(uint ax, _SelectionState &selection_state, const _KeyType& iinfo, bool use_index = true){

		using namespace NS_STORAGE;

		stored::_Rid row = key_to_rid(ax, iinfo);
		selection_state.last_resolved = row;

		if(use_index && treestore_resolve_values_from_index==TRUE){
			if( get_tree_table()->read_set_covered_by_index(table, active_index, selection_state.selected) ){
				clear_fields_set(selection_state);
				get_tree_table()->read_index_key_to_fields(selection_state,table,ax,iinfo,selection_state.field_map);
				resolve_selection_with_index(row,selection_state);
				return;
			}
		}
		resolve_selection(row,selection_state);

	}

	void resolve_selection_from_index(uint ax,  _SelectionState& selection_state, stored::index_iterator_interface * current_iterator, bool use_index = true) {
		const stored::StandardDynamicKey &iinfo = current_iterator->get_key();
		if(iinfo.row == 0){
			err_print("invalid key");
		}
		resolve_selection_from_index(ax, selection_state, iinfo, use_index);
	}


	void resolve_selection_from_index(uint ax,_SelectionState &selection_state,stored::index_iterator_interface & current_index_iterator, bool use_index = true) {
		resolve_selection_from_index(ax, selection_state, &current_index_iterator, use_index);
	}

	int basic_index_read_idx_map
	(	uchar * buf
	,	uint keynr
	,	const byte * key
	,	key_part_map keypart_map
	,	enum ha_rkey_function find_flag
	,	stored::index_interface * current_index
	,	_SelectionState &selection_state
	){
		int r = HA_ERR_END_OF_FILE;
		table->status = STATUS_NOT_FOUND;
		rebase_selection_io(buf,selection_state);
		stored::index_iterator_interface & current_iterator = *(current_index->get_index_iterator((nst::u64)this));

		tree_stored::tree_table::ptr tt =  get_tree_table();

		read_lookups++;
		const tree_stored::CompositeStored *pred = tt->predict_sequential(table, keynr, key, 0xffffff, keypart_map,current_iterator);
		if(pred==NULL){

			tt->temp_lower(table, keynr, key, 0xffffff, keypart_map,current_iterator);
		}


		if(current_iterator.valid()){
			const tree_stored::CompositeStored& current_key = current_iterator.get_key();
			switch(find_flag){
			case HA_READ_PREFIX:
			case HA_READ_PREFIX_LAST:

				break;
			case HA_READ_PREFIX_LAST_OR_PREV:
				//if(!tt->is_equal(current_key)){
				//	current_iterator.previous();
				//}
				break;
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
					if(!tt->check_key_match(current,table,keynr,keypart_map)){
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
				resolve_selection_from_index(keynr, selection_state, *pred);
			else
				resolve_selection_from_index(keynr, selection_state, &current_iterator);
			//if(HA_READ_KEY_EXACT != find_flag)

		}
		restore_selection_io(selection_state);
		check_own_use();
		DBUG_RETURN(r);
	}
	int index_read_map
	(	uchar * buf
	,	const uchar * key
	,	key_part_map keypart_map
	,	enum ha_rkey_function find_flag)
	{
		int r = basic_index_read_idx_map(buf, active_index, key, keypart_map, find_flag, current_index, this->_selection_state);

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
		_Selection selected;

		initialize_selection(this->_selection_state,keynr,tt->get_col_count());

		int r = basic_index_read_idx_map(buf, keynr, key, keypart_map, find_flag,get_tree_table()->get_index_interface(keynr),this->_selection_state);

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
		if((counter&1023)==0){
			check_own_use();
		}
		++counter;
		int r = HA_ERR_END_OF_FILE;
		get_index_iterator().next();
		table->status = STATUS_NOT_FOUND;
		if(get_index_iterator().valid()){
			rebase_selection_io(buf,this->_selection_state);
			resolve_selection_from_index(active_index,this->_selection_state,get_index_iterator());
			restore_selection_io(this->_selection_state);
			r = 0;
			table->status = 0;
			//index_iterator.next();
		}

		DBUG_RETURN(r);
	}

	int index_prev(byte * buf) {
		int r = HA_ERR_END_OF_FILE;
		THD *thd = ha_thd();
		table->status = STATUS_NOT_FOUND;
		check_own_use();
		if(get_index_iterator().previous()){
			r = 0;
			rebase_selection_io(buf,this->_selection_state);
			resolve_selection_from_index(active_index,this->_selection_state,get_index_iterator());
			restore_selection_io(this->_selection_state);
			table->status = 0;
		}
		DBUG_RETURN(r);
	}

	int index_first(byte * buf) {
		int r = HA_ERR_END_OF_FILE;

		DBUG_ENTER("index_first");
		ha_statistic_increment(&SSV::ha_read_first_count);
		stored::index_iterator_interface & current_iterator = *(current_index->get_index_iterator((nst::u64)this));
		current_index->first(current_iterator);
		current_iterator.first();
		table->status = STATUS_NOT_FOUND;
		if(current_iterator.valid()){
			rebase_selection_io(buf,this->_selection_state);
			resolve_selection_from_index(active_index,this->_selection_state,current_iterator);
			restore_selection_io(this->_selection_state);
			r = 0;
			table->status = 0;
		}

		DBUG_RETURN(r);
	}

	int index_last(byte * buf) {
		int r = HA_ERR_END_OF_FILE;
		stored::index_iterator_interface & current_iterator = *(current_index->get_index_iterator((nst::u64)this));
		current_index->end(current_iterator);
		current_iterator.last();
		table->status = STATUS_NOT_FOUND;
		if(current_iterator.valid()){
			rebase_selection_io(buf,this->_selection_state);
			resolve_selection_from_index(active_index,this->_selection_state,current_iterator,false);
			restore_selection_io(this->_selection_state);
			r = 0;
			table->status = 0;
			if(table->next_number_field){
				table->next_number_field->ptr += table->s->rec_buff_length;
				table->next_number_field->store(get_tree_table()->get_calc_row_id());
				table->next_number_field->ptr -= table->s->rec_buff_length;
			}
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
		current_index_iterator=nullptr;
		r = set_index_iterator_lower(start_key);
		if(r==0){
			table->status = 0;
			resolve_selection_from_index(active_index,this->_selection_state,get_index_iterator());
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
			resolve_selection_from_index(active_index,this->_selection_state,get_index_iterator());
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
static int thread_commit(handlerton *hton, THD *thd){
	tree_stored::tree_thread* thread = (tree_stored::tree_thread*)thd_get_ha_data(thd, hton);		
	thd_set_ha_data(thd, hton, nullptr);

	if(st.commit(thread)){		
		
	}else{
		err_print("could not find transaction context");
		return -1;
	}
	return 0;

}
static int treestore_commit(handlerton *hton, THD *thd, bool all){
	int return_val= 0;
	if
	(	thd->in_multi_stmt_transaction_mode() 
	||	all
	){
		return_val = thread_commit(hton, thd);
	}
	
	DBUG_PRINT("info", ("error val: %d", return_val));
	DBUG_RETURN(return_val);
}


static int treestore_rollback(handlerton *hton, THD *thd, bool all){
	int return_val= 0;
	DBUG_ENTER("treestore_rollback");
	if
	(	all 
	||	thd->in_multi_stmt_transaction_mode() 
	){
		tree_stored::tree_thread** stpp = thd_to_tree_thread(thd,hton);
		if((*stpp) != NULL){
			(*stpp)->rollback();
			(*stpp)->clear();
			st.release_writer((*stpp));
			thd_set_ha_data(thd, hton, nullptr);
		}
		thd_set_ha_data(thd, hton, nullptr);
	}
	DBUG_PRINT("info", ("error val: %d", return_val));
	DBUG_RETURN(return_val);
}


namespace storage_workers{
	typedef asynchronous::QueueManager<asynchronous::AbstractWorker> _WorkerManager;

    struct _storage_worker{
        _storage_worker() : w(1){
        }
        _WorkerManager w;
    };

	unsigned int ctr = 0;
	unsigned int get_next_counter(){
		return ++ctr;
	}
	const int MAX_WORKER_MANAGERS = 4;
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
void stop_cleaning();
void stop_calculating();

void test_suffix_array(){

	inf_print("testing suffix array encoding");
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
#ifdef _MSC_VER
	NS_STORAGE::i64 r = 3000000000ll;
	NS_STORAGE::buffer_type buffer(100);
	NS_STORAGE::buffer_type::iterator writer = buffer.begin();
	NS_STORAGE::leb128::write_signed(writer, r);
	//NS_STORAGE::i64 t = NS_STORAGE::leb128::read_signed(buffer.begin());
	//if(t!=r){
	//	printf("test failed\n");
	//}
#endif
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
		inf_print("*** Tree Store (eyore) using low fragmentation heap");
	}else{
		inf_print("*** Tree Store (eyore) NOT using low fragmentation heap");
	}
#endif
	inf_print("*** Tree Store (eyore) starting memuse configured to %lld MB",treestore_max_mem_use/(1024*1024L));
	DBUG_ENTER("treestore_db_init");
	initialize_loggers();
	Poco::Data::SQLite::Connector::registerConnector();
	handlerton *treestore_hton= (handlerton *)p;
	static_treestore_hton = treestore_hton;
	treestore_hton->state= SHOW_OPTION_YES;
	treestore_hton->db_type = DB_TYPE_UNKNOWN;
	treestore_hton->commit = treestore_commit;
	treestore_hton->rollback = treestore_rollback;
	treestore_hton->create = treestore_create_handler;
	treestore_hton->flags= HTON_CAN_RECREATE; ///HTON_ALTER_NOT_SUPPORTED | HTON_NO_PARTITION

	inf_print("Start cleaning");

	start_cleaning();
	start_calculating();
	test_run();

	DBUG_RETURN(FALSE);
}

int treestore_done(void *p)
{
	stop_cleaning();
	stop_calculating();
	
	return 0;
}
//MYSQL_SYSVAR(predictive_hash),
//MYSQL_SYSVAR(column_cache),
//MYSQL_SYSVAR(column_encoded),

static struct st_mysql_sys_var* treestore_system_variables[]= {
  MYSQL_SYSVAR(max_mem_use),
  MYSQL_SYSVAR(current_mem_use),
  MYSQL_SYSVAR(journal_size),
  MYSQL_SYSVAR(journal_lower_max),
  MYSQL_SYSVAR(journal_upper_max),
  MYSQL_SYSVAR(efficient_text),
  MYSQL_SYSVAR(column_cache_factor),
  MYSQL_SYSVAR(reduce_tree_use_on_unlock),
  MYSQL_SYSVAR(reduce_index_tree_use_on_unlock),
  MYSQL_SYSVAR(reduce_storage_use_on_unlock),
  MYSQL_SYSVAR(cleanup_time),
  MYSQL_SYSVAR(use_primitive_indexes),
  MYSQL_SYSVAR(block_read_ahead),
  MYSQL_SYSVAR(contact_points),
  MYSQL_SYSVAR(red_address),
  MYSQL_SYSVAR(resolve_values_from_index),
  MYSQL_SYSVAR(use_internal_pool),
  MYSQL_SYSVAR(reset_statistics),
  NULL
};

struct st_mysql_storage_engine treestore_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(treestore)
{
	MYSQL_STORAGE_ENGINE_PLUGIN,
		&treestore_storage_engine,
		"TREESTORE",
		"Christiaan Pretorius (c) 2013,2014,2015",
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

extern void start_red(const nst::u64& id);

namespace ts_cleanup{
	class print_cleanup_worker : public Poco::Runnable{
	private:
		bool started;
		bool stopped;
	public:
		
		print_cleanup_worker():started(false),stopped(true){
		}
		bool is_started() const {
			return started;
		}
		void wait_start(){
			while(!started){
				Poco::Thread::sleep(50);
			}
		}
		void stop(){

			started = false;			
			while(!stopped){
				Poco::Thread::sleep(50);
			}
				
		}
		void run(){
			stopped = false;
			started = true;
			stx::memory_low_state = false;
			start_red(0);
			nst::u64 last_print_size = calc_total_use();
			nst::u64 last_print_time = os::millis();
			/// DEBUG: nst::u64 last_check_size = calc_total_use();
			double min_tree_factor = treestore_column_cache_factor;
			double factor_decr = 0.1;
			double tree_factor = 1 - factor_decr;

			allocation_pool.set_max_pool_size(treestore_max_mem_use*tree_factor);
			buffer_allocation_pool.set_max_pool_size(treestore_max_mem_use*(1-tree_factor));
			buffer_allocation_pool.set_special();
			nst::u64 last_total_encoded = buffer_allocation_pool.get_allocated();

			while(started){
				//if(stx::memory_low_state) Poco::Thread::sleep(50);
				Poco::Thread::sleep(treestore_cleanup_time);
				nst::u64 total_encoded = buffer_allocation_pool.get_allocated();
				nst::u64 total_decoded = allocation_pool.get_allocated();
				nst::u64 target_encoded = (treestore_max_mem_use*(1-tree_factor));
				/// if tree factor becomes smaller the encoded memory becomes more - goes up
				bool up = (tree_factor > factor_decr) && (total_encoded > last_total_encoded) && buffer_allocation_pool.is_near_depleted();
				/// if tree factor becomes larger the encoded memory becomes less - goes down
				bool down = (tree_factor < (1.0 - factor_decr)) && (total_encoded < (last_total_encoded - last_total_encoded/8)) && (total_encoded < (target_encoded-target_encoded/8));
				nst::u64 last_up_encoded = last_total_encoded;
				last_total_encoded = total_encoded;
				if( down || up ){

					nst::i64 sign = up ? 1ll : -1ll;
					double tf = tree_factor;
					total_encoded = std::min<nst::u64>(treestore_max_mem_use,buffer_allocation_pool.get_allocated());
					if(up){
						double up_delta = (double)(2.0*(buffer_allocation_pool.get_allocated() - last_up_encoded))/(double)treestore_max_mem_use;
						inf_print("up delta :%.4g ", up_delta/units::MB);
						tree_factor = std::max<double>(min_tree_factor, tree_factor - up_delta);
					}else{
						tree_factor = std::max<double>(min_tree_factor, 1.0 - ((double)(total_encoded)/(double)treestore_max_mem_use));
					}

					inf_print("set encoded/decoded ratio to:%.4g from: %.4g, direction is %s", tree_factor, tf, up ? "up" : "down");

					allocation_pool.set_max_pool_size(treestore_max_mem_use*tree_factor);
					buffer_allocation_pool.set_max_pool_size(treestore_max_mem_use*(1-tree_factor));
				}

				buffer_allocation_pool.check_overflow();
				allocation_pool.check_overflow();

				stx::memory_mark_state = is_memory_mark(tree_factor);
				stx::memory_low_state = is_memory_low(tree_factor);
				if(!(up || down)){
					if(stx::memory_low_state ){ //|| stx::memory_mark_state
						if(stx::memory_low_state ) inf_print("reduce idle tree use");
						st.reduce_idle_writers();
						st.release_idle_trees();
					}
					if(stx::memory_low_state || stx::memory_mark_state || buffer_allocation_pool.is_near_depleted()){
						stored::reduce_aged();
						if(buffer_allocation_pool.is_near_depleted()){
							stored::reduce_all();
						}
					}
				}
				if(buffer_allocation_pool.is_near_full()){
					//
				}
				/// write statistics for idle writers which have something to save
				st.save_idle_writers_stats();
				if(os::millis() - last_print_time > 5000 ||
					llabs(calc_total_use() - last_print_size) > (last_print_size>>4)){

					inf_print
					(	" m:T%.4g b%.4g c%.4g pc%.4g stl%.4g pl. %.4g/%.4g, %.4g/%.4g MB, tr:%lu f%.4g"
					,	(double)calc_total_use()/units::MB
					,	(double)(nst::buffer_use+buffer_allocation_pool.get_allocated())/units::MB
					,	(double)nst::col_use/units::MB

					,	(double)total_cache_size/units::MB
					,	(double)nst::stl_use/units::MB
					,	(double)(allocation_pool.get_used())/units::MB
					,	(double)(allocation_pool.get_total_allocated())/units::MB
					,	(double)(buffer_allocation_pool.get_used())/units::MB
					,	(double)(buffer_allocation_pool.get_total_allocated())/units::MB
					//,	(double)allocation_pool.get_allocated()/units::MB
					,	(unsigned long)st.get_thread_count()
					,	tree_factor
					);
					last_print_size = calc_total_use();
					last_print_time = os::millis();
				}
			}
			
			inf_print("stopped cleanup worker");
			started = false;
			stopped = true;
		}
	};
	static print_cleanup_worker the_worker;
	static Poco::Thread cleanup_thread("ts:cleanup_thread");
	static void start(){
		try{
			cleanup_thread.start(the_worker);
			//the_worker.wait_start();
			inf_print("started cleanup worker");
		}catch(Poco::Exception &e){
			err_print("Could not start cleanup thread : %s\n",e.name());
		}
	}
	static void stop(){
		the_worker.stop();
		cleanup_thread.join();
	}
};
void start_cleaning(){
	ts_cleanup::start();
}
void stop_cleaning(){
	ts_cleanup::stop();
}
void start_calculating(){
	/// ts_info::start();
}
void stop_calculating(){
	//ts_info::stop();
}
#include <stx/storage/pool.h>

nst::allocation::pool allocation_pool(2*1024ll*1024ll*1024ll);
nst::allocation::pool buffer_allocation_pool(2*1024ll*1024ll*1024ll);

namespace stx{
namespace storage{
namespace allocation{
	Poco::Mutex lock;
	size_t threads = 0;

	thread_local thread_instance* std_instance = nullptr;
	thread_local thread_instance* buff_instance = nullptr;

	size_t get_thread_instance_count(){
		return threads;
	}
	thread_instance* get_variable(thread_instance** variable,pool_shared * shared){
		thread_instance* result = *variable;
		if(*variable==nullptr){
			synchronized inst(lock);
			if(*variable==nullptr){
				threads++;
				*variable = new thread_instance(shared);
				result = *variable;
			}
		}
		return result;
	}
	thread_instance* get_thread_instance(const nst::allocation::inner_pool* src){
		if(src == &(allocation_pool.get_pool())){
			return get_variable(&std_instance, allocation_pool.get_pool().get_shared());
		}else{
			return get_variable(&buff_instance, buffer_allocation_pool.get_pool().get_shared());
		}

	}
};
};
};


