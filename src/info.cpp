
Poco::Mutex tt_info_lock;
Poco::Mutex tt_info_delete_lock;

typedef std::map<std::string, tree_stored::tree_table* > _InfoTables;

static _InfoTables info_tables;

void reduce_info_tables(){
	nst::synchronized synch(tt_info_lock);
	
	for(_InfoTables::iterator i = info_tables.begin();i!=info_tables.end();++i){
		(*i).second->reduce_use();
	}
}
void clear_info_tables(){
	nst::synchronized synch(tt_info_lock);
	
	for(_InfoTables::iterator i = info_tables.begin();i!=info_tables.end();++i){
		delete (*i).second;
	}
	info_tables.clear();
}

void delete_info_table(const char* name){
	nst::synchronized synch(tt_info_lock);
	
	tree_stored::tree_table * result = info_tables[name];
	if(NULL != result){
		result -> rollback();		
		delete result;		
	}
	info_tables.erase(name);
}
tree_stored::tree_table * get_info_table(TABLE* table,const std::string& path){
	nst::synchronized synch(tt_info_lock);
	
	tree_stored::tree_table * result = info_tables[path];
	if(NULL == result){
		result = new tree_stored::tree_table(table,path);
		info_tables[path] = result;
		result->begin(true,false);
		result->calc_rowcount(); // the minimum statistics required
		result->rollback();
	}
	return result;

}

extern Poco::Mutex& get_tt_info_lock(){
	return tt_info_lock;
}
namespace ts_info{
	void perform_active_stats(const std::string table){
		
		//++total_locks;
		
		/// other threads cant delete while this section is active
		
		typedef std::vector<tree_stored::tree_table*> _Tables; ///, sta::tracker<tree_stored::tree_table*> 
		_Tables tables;

		{
			nst::synchronized synch(tt_info_lock);
			
			_InfoTables::iterator i = info_tables.find(table);
			if(i!=info_tables.end()){
				tree_stored::tree_table* t = (*i).second;
				if(t->should_calc()){
					t->begin(true,false);
					t->calc_density();
					t->reduce_use_collum_trees();
					t->rollback();
				}
				//tables.push_back((*i).second);			
			}

			for(_Tables::iterator i = tables.begin();i!=tables.end();++i){

				

			}
		}

		
		
		//--total_locks;		
	}
	void perform_active_stats(){
		//++total_locks;
		
		/// other threads cant delete while this section is active
		
		typedef std::vector<tree_stored::tree_table*> _Tables; ///,sta::tracker<tree_stored::tree_table*> 
		_Tables tables;

		{
			nst::synchronized synch(tt_info_lock);
			
			for(_InfoTables::iterator i = info_tables.begin();i!=info_tables.end();++i){
				tables.push_back((*i).second);
			}

			for(_Tables::iterator i = tables.begin();i!=tables.end();++i){

				(*i)->begin(true,false);
				(*i)->calc_density();
				(*i)->reduce_use_collum_trees();
				(*i)->rollback();

			}

		}
		
		

		//--total_locks;
		
	}
	
};
