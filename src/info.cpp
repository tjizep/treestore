
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
	nst::synchronized synch2(tt_info_delete_lock);
	for(_InfoTables::iterator i = info_tables.begin();i!=info_tables.end();++i){
		delete (*i).second;
	}
	info_tables.clear();
}

void delete_info_table(const char* name){
	nst::synchronized synch(tt_info_lock);
	nst::synchronized synch2(tt_info_delete_lock);
	tree_stored::tree_table * result = info_tables[name];
	if(NULL != result){
		delete result ;
		info_tables.erase(name);
	}
	
}
tree_stored::tree_table * get_info_table(TABLE* table){
	nst::synchronized synch(tt_info_lock);

	tree_stored::tree_table * result = info_tables[table->s->path.str];
	if(NULL == result){
		result = new tree_stored::tree_table(table);
		info_tables[table->s->path.str] = result;
		result->begin(true,false);
		result->calc_rowcount(); // the minimum statistics required
		result->rollback();
	}
	return result;

}

extern Poco::Mutex& get_tt_info_lock(){
	return tt_info_lock;
}
