
#include "Poco/Mutex.h"
#include "system_timers.h"
typedef Poco::ScopedLockWithUnlock<Poco::Mutex> syncronized;
static Poco::Mutex _c_lock;
namespace stx{
namespace storage{
	Poco::Int64 total_use = 0;
	Poco::UInt64 ptime = os::millis() ;
	Poco::UInt64 last_flush_time = os::millis() ;
	extern void add_total_use(long long added){
		syncronized l(_c_lock);
		if(added < 0){
			printf("adding neg val\n");
		}
		total_use += added;
	}
	extern void remove_total_use(long long removed){
		syncronized l(_c_lock);
		if(removed > total_use){
			printf("removing more than added\n");
		}
		total_use -= removed;
	}


}
}
