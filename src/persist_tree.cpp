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
	printf("sizeof(key_type)=%li\n",(long int)sizeof(key_type));
	typedef std::vector<key_type> _Script;
	static const size_t MAX_ITEMS = 1000000;
	static const size_t INTERVAL = MAX_ITEMS/10;
	_IntMap int_map(storage);
	//_IntMap int_map;
	_TestSet against;
	_Script script;
	size_t tt = ::os::millis();
	size_t onek = 1024ull;
	int_map.set_max_use(onek*onek*onek*2ull);
	size_t t = ::os::millis();
	char buf[120];

	std::string d;

	for(;against.size() < MAX_ITEMS;){//ensure uniqueness of keys
		sprintf(buf, "%08d",rand()*rand());
		key_type k = &buf[0]; //;
		if(against.count(k) == 0){
			against.insert(k);
			script.push_back(k);
			if(against.size() % (MAX_ITEMS/10)==0){
				printf("generated %li test keys in %li ms\n", (long int)against.size(), (long int)::os::millis()-t);
			}
		}
	}

	if(int_map.empty()){
		//std::sort(script.begin(), script.end());
		printf("%li items in %li ms\n", (long int)script.size(), (long int)(::os::millis()-t));
		t = ::os::millis();

		for(size_t i=0;i < MAX_ITEMS;++i){
			//int_map[script[i]] = script[i];

			int_map[script[i]]=i;
			if(i % INTERVAL == 0){
				printf("%li items in %li ms\n", (long int)i, (long int)(::os::millis()-t));
				//int_map.reduce_use();
			}
		}

		printf("%li items in %li ms\n", (long int)script.size(), (long int)(::os::millis()-t));
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
				printf("iterated %li items in %li ms\n", (long int)iter, (long int)(::os::millis()-t));

			}
		}
		printf("iterated %li items in %li ms\n",(long int) int_map.size(), (long int)(::os::millis()-t));

	}
	if(true){

		size_t t = ::os::millis();
		_IntMap::iterator f;
		for(size_t i=0;i < MAX_ITEMS;++i){
			f = int_map.find(script[i]);
			if(f==int_map.end() ){//
				printf("tree structure or insert error(-1)\n");
				return -1;
			}
			if(f.key() != script[i]){//
				printf("tree structure or insert error(1. %li!=%li)\n", (long int)f.data().get_value(),(long int)script[i].get_value());
				return -1;
			}

			/*if (int_map.count(script[i])==0 ){
				printf("tree structure or insert error(0)\n");
				return -1;
			}*/


			if(i % INTERVAL == 0){
				printf("read %li items in %li ms\n", (long int) i, (long int)(::os::millis()-t));
				//int_map.reduce_use();
			}

		}
		printf("read %li items in %li ms\n", (long int)script.size(), (long int)(::os::millis()-t));
		t = ::os::millis();
		for(size_t i=0;i < MAX_ITEMS/2;++i){
			int_map.erase(script[i]);
			if(i % INTERVAL == 0){
				printf("erased %li items in %li ms\n", (long int)i, (long int)(::os::millis()-t));
				//int_map.reduce_use();
			}
		}
		t = ::os::millis();
		for(size_t i=0;i < MAX_ITEMS/2;++i){
			f = int_map.find(script[i]);
			if(f!=int_map.end() ){//
				printf("tree structure or iterator error\n");
				return -1;
			}
			if(i % INTERVAL == 0){
				printf("tested %li erased items in %li ms\n", (long int)i, (long int)(::os::millis()-t));
				//int_map.reduce_use();
			}
		}
		for(size_t i=MAX_ITEMS/2;i < MAX_ITEMS;++i){
			f = int_map.find(script[i]);
			if(f==int_map.end() ){//
				printf("tree structure or erasure error\n");
				return -1;
			}
			if(i % INTERVAL == 0){
				printf("tested %li items in %li ms\n", (long int)i, (long int)::os::millis()-t);
				//int_map.reduce_use();
			}
		}
		printf("tree ok %li ms\n", (long int)(::os::millis()-tt));
	}
	return 0;
}
#ifndef _MSC_VER
void getch(){
}
#endif

int pt_test()
{
	printf("press a key to continue\n");
	getch();
	//Poco::Data::SQLite::registerConnector();
	//return strings_test();

	allocator_test::test_allocators();

	{
		typedef int key_type;
		typedef int value_type;
		example_storage<key_type, value_type> storage("test_data");
		typedef std::unordered_set<key_type> _TestSet;
		typedef stx::btree_map<key_type,value_type,example_storage<key_type,value_type>> _IntMap;
		//typedef std::map<int,int> _IntMap;

		typedef std::vector<key_type> _Script;
		static const size_t MAX_ITEMS = 10000000;
		static const size_t INTERVAL = MAX_ITEMS/10;
		_IntMap int_map(storage);
		_TestSet against;
		_Script script;
		size_t tt = ::os::millis();
		size_t onek = 1024ull;
		int_map.set_max_use(onek*onek*onek*2ull);
		size_t t = ::os::millis();

		for(;(size_t)against.size() < MAX_ITEMS;){//ensure uniqueness of keys
			key_type k = rand()*rand();//vgen++;
			if(against.count(k) == 0){
				against.insert(k);
				script.push_back(k);
				if(against.size() % (MAX_ITEMS/10)==0){
					printf("generated %li test keys in %li ms\n", (long int)against.size(), (long int)::os::millis()-t);
				}
			}
		}

		if(int_map.empty()){

			//std::sort(script.begin(), script.end());
			printf("%li items in %li ms\n", (long int)script.size(), (long int)::os::millis()-t);
			t = ::os::millis();

			for(size_t i=0;i < MAX_ITEMS;++i){
				int_map[script[i]] = script[i];
				//int_map.insert2(script[i], i);
				if(i % INTERVAL == 0){
					printf("%li items in %li ms\n", (long int)i, (long int)::os::millis()-t);
					//int_map.reduce_use();
				}
			}

			printf("%li items in %li ms\n", (long int)script.size(), (long int)::os::millis()-t);
			//int_map.flush();

			t = ::os::millis();
			int iter = 0;
			_IntMap::iterator the_end = int_map.end();
			for(_IntMap::iterator j=int_map.begin(); j != the_end;++j){
				++iter;
				/// int i = j.key();

				if(iter % INTERVAL == 0){
					printf("iterated %li items in %li ms\n", (long int)iter, (long int)::os::millis()-t);

				}
			}
			printf("iterated %li items in %li ms\n", (long int)int_map.size(), (long int)::os::millis()-t);

		}
		if(true){
			size_t t = ::os::millis();
			_IntMap::iterator f;
			{
				t = ::os::millis();

				for(size_t i=0;i < MAX_ITEMS;++i){
					f = int_map.find(script[i]);
					if(f==int_map.end() ){//
						printf("tree structure or insert error(-1)\n");
						return -1;
					}
					if(f.key() != script[i]){
						/*if (int_map.count(script[i])==0 ){*/
						printf("tree structure or insert error(0)\n");
						return -1;
					}

					if(f.data() != script[i]){//
						//// key_type k = f.key();
						printf("tree structure or insert error(1. %li!=%li)\n",(long int) f.data(), (long int)script[i]);
						return -1;
					}
					if(i % INTERVAL == 0){
						printf("read %li items in %li ms\n",(long int) i, (long int)::os::millis()-t);
						//int_map.reduce_use();
					}

				}
				printf("read %li items in %li ms\n", (long int)script.size(), (long int)::os::millis()-t);
			}
			{
				t = ::os::millis();
				for(size_t i=0;i < MAX_ITEMS;++i){
					f = int_map.find(script[i]);
					if(f==int_map.end() ){//
						printf("tree structure or insert error(-1)\n");
						return -1;
					}
					if(f.key() != script[i]){
						/*if (int_map.count(script[i])==0 ){*/
						printf("tree structure or insert error(0)\n");
						return -1;
					}

					if(f.data() != script[i]){//
						/// key_type k = f.key();
						printf("tree structure or insert error(1. %li!=%li)\n",(long int) f.data(), (long int)script[i]);
						return -1;
					}
					if(i % INTERVAL == 0){
						printf("read %li items in %li ms\n", (long int)i, (long int)::os::millis()-t);
						//int_map.reduce_use();
					}

				}
				printf("read %li items in %li ms\n", (long int)script.size(), (long int)::os::millis()-t);
			}

			t = ::os::millis();
			for(size_t i=0;i < MAX_ITEMS/2;++i){
				int_map.erase(script[i]);
				if(i % INTERVAL == 0){
					printf("erased %li items in %li ms\n",(long int) i, (long int)::os::millis()-t);
					//int_map.reduce_use();
				}
			}
			t = ::os::millis();
			for(size_t i=0;i < MAX_ITEMS/2;++i){
				f = int_map.find(script[i]);
				if(f!=int_map.end() ){//
					printf("tree structure or iterator error\n");
					return -1;
				}
				if(i % INTERVAL == 0){
					printf("tested %li erased items in %li ms\n", (long int)i, (long int)::os::millis()-t);
					//int_map.reduce_use();
				}
			}
			for(size_t i=MAX_ITEMS/2;i < MAX_ITEMS;++i){
				f = int_map.find(script[i]);
				if(f==int_map.end() ){//
					printf("tree structure or erasure error\n");
					return -1;
				}
				if(i % INTERVAL == 0){
					printf("tested %li items in %li ms\n", (long int)i, (long int)::os::millis()-t);
					///int_map.reduce_use();
				}
			}
			printf("tree ok %li ms\n", (long int) ::os::millis()-tt);
		}

	}

	return 0;


}

