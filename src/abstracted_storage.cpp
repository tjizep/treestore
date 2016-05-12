#include "abstracted_storage.h"

static Poco::Mutex m;
namespace nst = ::stx::storage;
namespace stored{
	
	static _AlocationsMap instances;
	_Allocations* _get_abstracted_storage(std::string name){
		_Allocations* r = NULL;
		r = instances[name];
		if(r == NULL){
			r = new _Allocations( std::make_shared<_BaseAllocator>( stx::storage::default_name_factory(name.c_str())) );
			instances[name] = r;
		}

		return r;
	}
	bool erase_abstracted_storage(std::string name){
		
		nst::synchronized ll(m);
		_Allocations* r = NULL;
		_AlocationsMap::iterator s = instances.find(name);
		if(s == instances.end()) return true;
		r = (*s).second;
		if(r == nullptr) return true;

		if(r->is_idle()){
			
			r->set_recovery(false);
			delete r;
			instances.erase(name);

			return true;
		}else{
			printf("the storage '%s' is not idle and could not be erased\n",name.c_str());
		}				
		
		return false;
	}
	_Allocations* get_abstracted_storage(std::string name){
		_Allocations* r = NULL;
		{
			nst::synchronized ll(m);
			if(instances.empty())
				nst::journal::get_instance().recover();
			r = instances[name];
			if(r == NULL){
				r = new _Allocations( std::make_shared<_BaseAllocator>( stx::storage::default_name_factory(name.c_str())) );
				///r->set_readahead(true);
				instances[name] = r;
			}
			r->set_recovery(false);
		}
		r->engage();
		return r;
	}
	void reduce_aged(){
		nst::synchronized ll(m);
		nst::u64 reduced = 0;
		for(_AlocationsMap::iterator a = instances.begin(); a != instances.end(); ++a){
			//if(buffer_allocation_pool.is_near_depleted()){ //
				if((*a).second->get_age() > 15000 && (*a).second->transactions_away() <= 1){				
					(*a).second->reduce();
					(*a).second->touch();
					reduced++;
				}
			//}
		}
		inf_print("reduced %lld aged block storages",reduced);
	}
	void reduce_all(){

		nst::synchronized ll(m);
		
		for(_AlocationsMap::iterator a = instances.begin(); a != instances.end(); ++a){
			if(buffer_allocation_pool.is_near_depleted())
				(*a).second->reduce();
		}
	}
}; //stored

#include "Foundation/src/Environment.cpp"
