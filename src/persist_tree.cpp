// persist_tree.cpp : Defines the entry point for the console application.
//

#ifdef _MSC_VER
#define _WIN32_WINNT 0x0400
#include <windows.h>
#endif

#include "example_storage.h"
#include "allocator_test.h"
#include "fields.h"
#include "system_timers.h"
int strings_test(){


	typedef stored::Blobule<16> key_type;
	typedef stored::IntTypeStored<int> value_type;
	typedef stored::abstracted_storage _Storage;
	//typedef buf_type<32> key_type;
	//typedef buf_type<32> value_type;
	//typedef buf_type_storage<key_type> _Storage;
	_Storage storage("strings_test");
	typedef std::set<key_type> _TestSet;
	typedef stx::btree_map<key_type,value_type,_Storage> _IntMap;
	//typedef std::map<key_type,value_type> _IntMap;
	//typedef std::map<int,int> _IntMap;
	printf("sizeof(key_type)=%ld\n",sizeof(key_type));
	typedef std::vector<key_type> _Script;
	static const int MAX_ITEMS = 1000000;
	static const int INTERVAL = MAX_ITEMS/10;
	_IntMap int_map(storage);
	//_IntMap int_map;
	_TestSet against;
	_Script script;
	size_t tt = ::os::millis();
	size_t onek = 1024ull;
	int_map.set_max_use(onek*onek*onek*2ull);
	size_t t = ::os::millis();
	char buf[120];
	int vgen = 0;
	std::string d;

	for(;against.size() < MAX_ITEMS;){//ensure uniqueness of keys
		sprintf(buf, "%08d",rand()*rand());
		key_type k = &buf[0]; //;
		if(against.count(k) == 0){
			against.insert(k);
			script.push_back(k);
			if(against.size() % (MAX_ITEMS/10)==0){
				printf("generated %ld test keys in %ld ms\n", against.size(), ::os::millis()-t);
			}
		}
	}

	if(int_map.empty()){
		//std::sort(script.begin(), script.end());
		printf("%ld items in %ld ms\n", script.size(), ::os::millis()-t);
		t = ::os::millis();

		for(int i=0;i < MAX_ITEMS;++i){
			//int_map[script[i]] = script[i];

			int_map[script[i]]=i;
			if(i % INTERVAL == 0){
				printf("%lld items in %ld ms\n", i, ::os::millis()-t);
				//int_map.reduce_use();
			}
		}

		printf("%ld items in %ld ms\n", script.size(), ::os::millis()-t);
		//int_map.flush();

		t = ::os::millis();
		int iter = 0;
		int ok = 0;
		_IntMap::iterator the_end = int_map.end();
		key_type val;
		for(_IntMap::iterator j=int_map.begin(); j != the_end;++j){
			++iter;
			if(val != j.key()){
				ok++;
			}

			if(iter % INTERVAL == 0){
				printf("iterated %ld items in %lld ms\n", iter, ::os::millis()-t);

			}
		}
		printf("iterated %ld items in %lld ms\n", int_map.size(), ::os::millis()-t);

	}
	if(true){

		size_t t = ::os::millis();
		_IntMap::iterator f;
		for(int i=0;i < MAX_ITEMS;++i){
			f = int_map.find(script[i]);
			if(f==int_map.end() ){//
				printf("tree structure or insert error(-1)\n");
				return -1;
			}
			if(f.key() != script[i]){//
				printf("tree structure or insert error(1. %ld!=%ld)\n", f.data().get_value(),script[i].get_value());
				return -1;
			}

			/*if (int_map.count(script[i])==0 ){
				printf("tree structure or insert error(0)\n");
				return -1;
			}*/


			if(i % INTERVAL == 0){
				printf("read %ld items in %lld ms\n", i, ::os::millis()-t);
				//int_map.reduce_use();
			}

		}
		printf("read %ld items in %lld ms\n", script.size(), ::os::millis()-t);
		t = ::os::millis();
		for(int i=0;i < MAX_ITEMS/2;++i){
			int_map.erase(script[i]);
			if(i % INTERVAL == 0){
				printf("erased %ld items in %lld ms\n", i, ::os::millis()-t);
				//int_map.reduce_use();
			}
		}
		t = ::os::millis();
		for(int i=0;i < MAX_ITEMS/2;++i){
			f = int_map.find(script[i]);
			if(f!=int_map.end() ){//
				printf("tree structure or iterator error\n");
				return -1;
			}
			if(i % INTERVAL == 0){
				printf("tested %ld erased items in %lld ms\n", i, ::os::millis()-t);
				//int_map.reduce_use();
			}
		}
		for(int i=MAX_ITEMS/2;i < MAX_ITEMS;++i){
			f = int_map.find(script[i]);
			if(f==int_map.end() ){//
				printf("tree structure or erasure error\n");
				return -1;
			}
			if(i % INTERVAL == 0){
				printf("tested %ld items in %lld ms\n", i, ::os::millis()-t);
				//int_map.reduce_use();
			}
		}
		printf("tree ok %lld ms\n", ::os::millis()-tt);
	}
	return 0;
}

int test()
{
	Poco::Data::SQLite::Connector::registerConnector();
	return strings_test();

	allocator_test::test_allocators();

	{
		typedef int key_type;
		typedef int value_type;
		example_storage<key_type, value_type> storage;
		typedef std::unordered_set<key_type> _TestSet;
		typedef stx::btree_map<key_type,value_type,example_storage<key_type,value_type>> _IntMap;
		//typedef std::map<int,int> _IntMap;

		typedef std::vector<key_type> _Script;
		static const int MAX_ITEMS = 1000000;
		static const int INTERVAL = MAX_ITEMS/10;
		_IntMap int_map(storage);
		_TestSet against;
		_Script script;
		size_t tt = ::os::millis();
		size_t onek = 1024ull;
		int_map.set_max_use(onek*onek*onek*2ull);
		size_t t = ::os::millis();
		int vgen = 0;
		for(;against.size() < MAX_ITEMS;){//ensure uniqueness of keys
			key_type k = rand()*rand();//vgen++;
			if(against.count(k) == 0){
				against.insert(k);
				script.push_back(k);
				if(against.size() % (MAX_ITEMS/10)==0){
					printf("generated %ld test keys in %lld ms\n", against.size(), ::os::millis()-t);
				}
			}
		}

		if(int_map.empty()){

			//std::sort(script.begin(), script.end());
			printf("%ld items in %lld ms\n", script.size(), ::os::millis()-t);
			t = ::os::millis();

			for(int i=0;i < MAX_ITEMS;++i){
				int_map[script[i]] = script[i];
				//int_map.insert2(script[i], i);
				if(i % INTERVAL == 0){
					printf("%ld items in %lld ms\n", i, ::os::millis()-t);
					//int_map.reduce_use();
				}
			}

			printf("%ld items in %lld ms\n", script.size(), ::os::millis()-t);
			int_map.flush();



			t = ::os::millis();
			int iter = 0;
			_IntMap::iterator the_end = int_map.end();
			for(_IntMap::iterator j=int_map.begin(); j != the_end;++j){
				++iter;
				int i = j.key();

				if(iter % INTERVAL == 0){
					printf("iterated %ld items in %ld ms\n", iter, ::os::millis()-t);

				}
			}
			printf("iterated %ld items in %ld ms\n", int_map.size(), ::os::millis()-t);

		}
		if(true){

			size_t t = ::os::millis();
			_IntMap::iterator f;
			for(int i=0;i < MAX_ITEMS;++i){
				f = int_map.find(script[i]);
				if(f==int_map.end() ){//
					printf("tree structure or insert error(-1)\n");
					return -1;
				}
				/*if (int_map.count(script[i])==0 ){
					printf("tree structure or insert error(0)\n");
					return -1;
				}*/

				if(f.data() != script[i]){//
					printf("tree structure or insert error(1. %ld!=%ld)\n", f.data(), script[i]);
					return -1;
				}
				if(i % INTERVAL == 0){
					printf("read %ld items in %ld ms\n", i, ::os::millis()-t);
					//int_map.reduce_use();
				}

			}
			printf("read %ld items in %ld ms\n", script.size(), ::os::millis()-t);
			t = ::os::millis();
			for(int i=0;i < MAX_ITEMS/2;++i){
				int_map.erase(script[i]);
				if(i % INTERVAL == 0){
					printf("erased %ld items in %ld ms\n", i, ::os::millis-t);
					//int_map.reduce_use();
				}
			}
			t = ::os::millis();
			for(int i=0;i < MAX_ITEMS/2;++i){
				f = int_map.find(script[i]);
				if(f!=int_map.end() ){//
					printf("tree structure or iterator error\n");
					return -1;
				}
				if(i % INTERVAL == 0){
					printf("tested %ld erased items in %ld ms\n", i, ::os::millis()-t);
					//int_map.reduce_use();
				}
			}
			for(int i=MAX_ITEMS/2;i < MAX_ITEMS;++i){
				f = int_map.find(script[i]);
				if(f==int_map.end() ){//
					printf("tree structure or erasure error\n");
					return -1;
				}
				if(i % INTERVAL == 0){
					printf("tested %ld items in %ld ms\n", i, ::os::millis()-t);
					///int_map.reduce_use();
				}
			}
			printf("tree ok %ld ms\n", ::os::millis()-tt);
		}

	}

	return 0;


}

