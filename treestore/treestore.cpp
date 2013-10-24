// treestore.cpp : Defines the exported functions for the DLL application.
//


#include "treestore.h"


// This is an example of an exported variable
TREESTORE_API int ntreestore=0;

// This is an example of an exported function.
TREESTORE_API int fntreestore(void)
{
	return 42;
}

// This is the constructor of a class that has been exported.
// see treestore.h for the class definition
Ctreestore::Ctreestore()
{
	return;
}
