// dllmain.cpp : Defines the entry point for the DLL application.
#include <windows.h>
#include "Poco/Mutex.h"
typedef Poco::ScopedLockWithUnlock<Poco::Mutex> scoped_ulock;
static Poco::Mutex _c_lock;

ptrdiff_t btree_totl_used = 0;
ptrdiff_t btree_totl_instances = 0;

void add_btree_totl_used(ptrdiff_t added){
	scoped_ulock l(_c_lock);
	btree_totl_used += added;
}
void remove_btree_totl_used(ptrdiff_t removed){
	scoped_ulock l(_c_lock);
	btree_totl_used -= removed;
}

BOOL APIENTRY DllMain( HMODULE hModule,
                       DWORD  ul_reason_for_call,
                       LPVOID lpReserved
					 )
{
	switch (ul_reason_for_call)
	{
	case DLL_PROCESS_ATTACH:
		btree_totl_used = 0;
		btree_totl_instances = 0;
	case DLL_THREAD_ATTACH:
	case DLL_THREAD_DETACH:
	case DLL_PROCESS_DETACH:
		break;
	}
	return TRUE;
}

