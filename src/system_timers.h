#ifndef _SYSTEM_TIMERS_CEP_20131108_
#define _SYSTEM_TIMERS_CEP_20131108_
#include <fstream>
#include <Poco/Timestamp.h>
#include <Poco/Thread.h>
#include <Poco/BinaryWriter.h>
#include <Poco/BinaryReader.h>
#include <Poco/File.h>

namespace os{
	inline void read_ahead(std::string name){
		std::ifstream data_istr(name, std::ios::binary);
		Poco::BinaryReader reader(data_istr);
		char buffer[16384];
		Poco::UInt64 bc = 0;
		while(reader.good()){
			reader.readRaw(buffer,sizeof(buffer));
			bc++;			
		}
	}
	inline Poco::UInt64 millis(){
		Poco::Timestamp now;
		return now.epochMicroseconds()/1000;
	}
	inline Poco::UInt64 micros(){
		Poco::Timestamp now;
		return now.epochMicroseconds();
	}
	
	/// zzzz because mysql redefines 'sleep' as a macro

	inline void zzzz(Poco::UInt32 millis){
#ifdef _MSC_VER
				Poco::Thread::sleep(millis);
#else
				Poco::Thread::sleep(millis);
#endif
	}
}
#endif