//
// Connector.h
//
// $Id: //poco/1.4/Data/SQLite/include/Poco/Data/SQLite/Connector.h#1 $
//
// Library: Data/SQLite
// Package: SQLite
// Module:  Connector
//
// Definition of the Connector class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// Permission is hereby granted, free of charge, to any person or organization
// obtaining a copy of the software and accompanying documentation covered by
// this license (the "Software") to use, reproduce, display, distribute,
// execute, and transmit the Software, and to prepare derivative works of the
// Software, and to permit third-parties to whom the Software is furnished to
// do so, all subject to the following:
// 
// The copyright notices in the Software and this entire statement, including
// the above license grant, this restriction and the following disclaimer,
// must be included in all copies of the Software, in whole or in part, and
// all derivative works of the Software, unless such copies or derivative
// works are solely in the form of machine-executable object code generated by
// a source language processor.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE, TITLE AND NON-INFRINGEMENT. IN NO EVENT
// SHALL THE COPYRIGHT HOLDERS OR ANYONE DISTRIBUTING THE SOFTWARE BE LIABLE
// FOR ANY DAMAGES OR OTHER LIABILITY, WHETHER IN CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//


#ifndef DataConnectors_SQLite_Connector_INCLUDED
#define DataConnectors_SQLite_Connector_INCLUDED


#include "Poco/Data/SQLite/SQLite.h"
#include "Poco/Data/Connector.h"


namespace Poco {
namespace Data {
namespace SQLite {


class SQLite_API Connector: public Poco::Data::Connector
	/// Connector instantiates SqLite SessionImpl objects.
{
public:
	static const std::string KEY;
		/// Keyword for creating SQLite sessions ("SQLite").

	Connector();
		/// Creates the Connector.

	~Connector();
	/// Destroys the Connector.

	Poco::AutoPtr<Poco::Data::SessionImpl> createSession(const std::string& connectionString);
		/// Creates a SQLite SessionImpl object and initializes it with the given connectionString.

	static void registerConnector();
		/// Registers the Connector under the Keyword Connector::KEY at the Poco::Data::SessionFactory.
	
	static void unregisterConnector();
		/// Unregisters the Connector under the Keyword Connector::KEY at the Poco::Data::SessionFactory.

	static void enableSharedCache(bool flag = true);
		/// Enables or disables SQlite shared cache mode
		/// (see http://www.sqlite.org/sharedcache.html for a discussion).

	static void enableSoftHeapLimit(int limit);
		/// Sets a soft upper limit to the amount of memory allocated
		/// by SQLite. For more information, please see the SQLite
		/// sqlite_soft_heap_limit() function (http://www.sqlite.org/c3ref/soft_heap_limit.html).
};


} } } // namespace Poco::Data::SQLite


#endif // DataConnectors_SQLite_Connector_INCLUDED
