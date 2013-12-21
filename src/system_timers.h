#ifndef _SYSTEM_TIMERS_CEP_20131108_
#define _SYSTEM_TIMERS_CEP_20131108_
#include <Poco/Timestamp.h>
#include <Poco/Thread.h>
namespace os{
	inline Poco::UInt64 millis(){
		Poco::Timestamp now;
		return now.epochMicroseconds()/1000;
	}
	inline Poco::UInt64 micros(){
		Poco::Timestamp now;
		return now.epochMicroseconds();
	}
	
	/// zzzz because ms compilers have some strange unhealthy fascination with the word 'sleep'

	inline void zzzz(Poco::UInt32 millis){
#ifdef _MSC_VER
				Poco::Thread::__sleep(millis);
#else
				Poco::Thread::sleep(millis);
#endif
	}
}
#endif