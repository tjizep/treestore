#include "Poco/Mutex.h"
#include "system_timers.h"
#include <stx/storage/types.h>


typedef Poco::ScopedLockWithUnlock<Poco::Mutex> syncronized;
static Poco::Mutex& get_stats_lock(){
	static Poco::Mutex _c_lock;
	return _c_lock;
}
namespace stx{
namespace storage{
	class low_resource_timer{
		class timer_worker : public Poco::Runnable{
		private:
			bool started;
			bool stopped;
			u64 timer_val;
		public:
			timer_worker() : timer_val(0),started(false),stopped(true){
			}
			void cancel(){
				started = false;
			}
			bool is_started() const {
				return started;
			}
			void wait_start(){
				while(!started){
					try{
						Poco::Thread::sleep(50);
					}catch(Poco::Exception &){
						inf_print("stop interrupted");
					}
				}
			}
			void stop(){
				cancel();
				while(!stopped){
					try{
						Poco::Thread::sleep(50);
					}catch(Poco::Exception &){
						inf_print("stop interrupted");
					}
				}
			}
			void run(){
				stopped = false;
				started = true;

				timer_val = os::millis();
				try{
					while(is_started()){
						Poco::Thread::sleep(50);
						timer_val = os::millis();
					}
				}catch(Poco::Exception &){
					inf_print("timer interrupted");
				}
				inf_print("timer is canceled");
				stopped = true;
			}
			u64 get_timer() const {
				return this->timer_val;
			}
		};
	private:
		Poco::Thread timer_thread;
		timer_worker worker;
	public:
		low_resource_timer() : timer_thread("ts:timer_thread"){
			try{
				timer_thread.start(worker);
				//worker.wait_start();
			}catch(Poco::Exception &e){
				err_print("Could not start timer thread : %s\n",e.name());
			}
		}
		u64 get_timer() const {
			return this->worker.get_timer();
		}
		~low_resource_timer(){
			try{
				if(worker.is_started()){
					worker.stop();
				}

			}catch(Poco::Exception &e){
				err_print("Could not stop timer thread : %s\n",e.name());
			}
		}
	};
	static low_resource_timer lrt;
	u64 get_lr_timer(){
		return lrt.get_timer();
	}
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
			err_print("adding neg val");
		}
		total_use += added;
	}
	extern void remove_total_use(long long removed){
		syncronized l(get_stats_lock());
		if(removed > total_use){
			err_print("removing more than added");
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
