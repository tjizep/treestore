// The following ifdef block is the standard way of creating macros which make exporting 
// from a DLL simpler. All files within this DLL are compiled with the TREESTORE_EXPORTS
// symbol defined on the command line. This symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see 
// TREESTORE_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
#ifdef TREESTORE_EXPORTS
#define TREESTORE_API __declspec(dllexport)
#else
#define TREESTORE_API __declspec(dllimport)
#endif

// This class is exported from the treestore.dll
class TREESTORE_API Ctreestore {
public:
	Ctreestore(void);
	// TODO: add your methods here.
};

extern TREESTORE_API int ntreestore;

TREESTORE_API int fntreestore(void);
