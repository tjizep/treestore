#ifndef _ALLOCATOR_TEST_CEP_2013_
#define _ALLOCATOR_TEST_CEP_2013_
#include "simple_storage.h"
namespace allocator_test{
	int test_allocators(){
		typedef stx::storage::sqlite_allocator<NS_STORAGE::stream_address, NS_STORAGE::buffer_type> test_alloc_type;
		typedef std::shared_ptr<test_alloc_type> test_alloc_type_ptr;
		typedef stx::storage::mvcc_coordinator<test_alloc_type> mvcc_coordinator_type;
		test_alloc_type_ptr test_the_alloc = std::make_shared<test_alloc_type>(NS_STORAGE::default_name_factory("test_allocator"));
		mvcc_coordinator_type mvcc(test_the_alloc);

		NS_STORAGE::stream_address sa =0;
		test_the_alloc->allocate(sa,NS_STORAGE::create);
		mvcc_coordinator_type::version_storage_type* version =  mvcc.begin(false);

		version->allocate(sa,NS_STORAGE::read);
		mvcc.commit(version);

		mvcc_coordinator_type::version_storage_type* version2 =  mvcc.begin(true);
		mvcc.commit(version2);

		mvcc_coordinator_type::version_storage_type* version3 =  mvcc.begin(false);
		mvcc.discard(version3);
		//mvcc.commit(version3);
		printf("allocators tested\n");
		return 0;
	}
};
#endif
