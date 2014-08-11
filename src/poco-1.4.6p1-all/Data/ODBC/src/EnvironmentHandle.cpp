//
// EnvironmentHandle.cpp
//
// $Id: //poco/1.4/Data/ODBC/src/EnvironmentHandle.cpp#1 $
//
// Library: Data/ODBC
// Package: ODBC
// Module:  EnvironmentHandle
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


#include "Poco/Data/ODBC/EnvironmentHandle.h"
#include "Poco/Data/ODBC/Utility.h"
#include "Poco/Data/ODBC/ODBCException.h"


namespace Poco {
namespace Data {
namespace ODBC {


EnvironmentHandle::EnvironmentHandle(): _henv(SQL_NULL_HENV)
{
	if (Utility::isError(SQLAllocHandle(SQL_HANDLE_ENV, 
			SQL_NULL_HANDLE, 
			&_henv)) ||
		Utility::isError(SQLSetEnvAttr(_henv, 
			SQL_ATTR_ODBC_VERSION, 
			(SQLPOINTER) SQL_OV_ODBC3, 
			0)))
	{
		throw ODBCException("Could not initialize environment.");
	}
}


EnvironmentHandle::~EnvironmentHandle()
{
	SQLRETURN rc = SQLFreeHandle(SQL_HANDLE_ENV, _henv);
	poco_assert (!Utility::isError(rc));
}


} } } // namespace Poco::Data::ODBC
