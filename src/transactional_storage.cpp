#include <windows.h>
#include "Poco/Mutex.h"
#include "system_timers.h"
typedef Poco::ScopedLockWithUnlock<Poco::Mutex> scoped_ulock;
static Poco::Mutex _c_lock;
namespace stx{
namespace storage{
	Poco::Int64 total_use = 0;
	Poco::UInt64 ptime = os::millis() ;
	Poco::UInt64 last_flush_time = os::millis() ;
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
