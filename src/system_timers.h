#ifndef _SYSTEM_TIMERS_CEP_20131108_
#define _SYSTEM_TIMERS_CEP_20131108_
#include <Poco/Timestamp.h>
namespace os{
	inline Poco::UInt64 millis(){
		return Poco::Timestamp().epochMicroseconds()/1000;
	}
	inline Poco::UInt64 micros(){
		return Poco::Timestamp().epochMicroseconds()/1000;
	}
}
#endif