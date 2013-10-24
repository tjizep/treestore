#include <Windows.h>
#include <Psapi.h>
ptrdiff_t MAX_EXT_MEM = 1024ll*1024ll*1024ll*8ll;
static HANDLE process_handle(){
	static HANDLE result = ::GetCurrentProcess();
	return result;
}

	ptrdiff_t  _reported_memory_size(){
		
		static ptrdiff_t _last = ::GetTickCount();
		static ptrdiff_t result = 0;
		if(::GetTickCount() - _last > 100){
			HANDLE process = process_handle();
			if (!process)   {
				return result;
			}
		
			PROCESS_MEMORY_COUNTERS counters;
			if(GetProcessMemoryInfo(process, &counters, sizeof(counters))){
				result = counters.WorkingSetSize;
			}		
			
			_last = ::GetTickCount();
		}
		return result;
}
