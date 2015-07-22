#include "btree.h"
bool stx::memory_low_state = false;
stx::_idle_processors stx::idle_processors;

#include "stx/storage/pool.h"
namespace stx{
namespace storage{
namespace allocation{
	
	//Poco::ThreadLocal<unlocked_pool> pool::sp;
	//unlocked_pool* pool::sp = new unlocked_pool(1024ll*1024ll*1024ll);
};
};
};