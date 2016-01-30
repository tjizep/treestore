#include "Poco/Mutex.h"
#include "system_timers.h"

typedef Poco::ScopedLockWithUnlock<Poco::Mutex> syncronized;
static Poco::Mutex& get_stats_lock(){
	static Poco::Mutex _c_lock;
	return _c_lock;
}
namespace stx{
namespace storage{
	Poco::Mutex& get_single_writer_lock(){
		static Poco::Mutex swl;
		return swl;
	}
	
	long long total_use = 0;
	
	long long buffer_use = 0;

	long long col_use = 0;

	long long stl_use = 0;

	Poco::UInt64 ptime = os::millis() ;
	Poco::UInt64 last_flush_time = os::millis() ;
	extern void add_total_use(long long added){
		syncronized l(get_stats_lock());
		if(added < 0){
			printf("adding neg val\n");
		}
		total_use += added;
	}
	extern void remove_total_use(long long removed){
		syncronized l(get_stats_lock());
		if(removed > total_use){
			printf("removing more than added\n");
		}
		total_use -= removed;
	}

	extern void add_buffer_use(long long added){
		if(added==0) return;
		syncronized l(get_stats_lock());
		total_use += added;
		buffer_use += added;
	}
	
	extern void remove_buffer_use(long long removed){
		syncronized l(get_stats_lock());
		total_use -= removed;
		buffer_use -= removed;
	}
	
	extern void add_col_use(long long added){
		syncronized l(get_stats_lock());
		total_use += added;
		col_use += added;
	}
	
	extern void remove_col_use(long long removed){
		syncronized l(get_stats_lock());
		total_use -= removed;
		col_use -= removed;
	}
	extern void add_stl_use(long long added){
		syncronized l(get_stats_lock());
		total_use += added;
		stl_use += added;
	}
	
	extern void remove_stl_use(long long removed){
		syncronized l(get_stats_lock());
		total_use -= removed;
		stl_use -= removed;
	}

}
}
