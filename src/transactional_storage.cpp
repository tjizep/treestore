#include <windows.h>
#include "Poco/Mutex.h"
typedef Poco::ScopedLockWithUnlock<Poco::Mutex> scoped_ulock;
static Poco::Mutex _c_lock;
namespace stx{
namespace storage{
	long long total_use = 0;
	unsigned int ptime = ::GetTickCount() ;
	long long last_flush_time = ::GetTickCount() ;
	extern void add_total_use(long long added){
		scoped_ulock l(_c_lock);
		total_use += added;
	}
	extern void remove_total_use(long long removed){
		scoped_ulock l(_c_lock);
		total_use -= removed;
	}

	
}
}
