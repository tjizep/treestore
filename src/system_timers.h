#ifndef _SYSTEM_TIMERS_CEP_20131108_
#define _SYSTEM_TIMERS_CEP_20131108_
#include <Poco/Timestamp.h>
namespace os{
	inline Poco::UInt64 millis(){
		Poco::Timestamp now;
		return now.epochMicroseconds()/1000;
	}
	inline Poco::UInt64 micros(){
		Poco::Timestamp now;
		return now.epochMicroseconds();
	}
}
#endif