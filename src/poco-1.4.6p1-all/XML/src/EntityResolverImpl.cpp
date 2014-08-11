//
// EntityResolverImpl.cpp
//
// $Id: //poco/1.4/XML/src/EntityResolverImpl.cpp#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
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


#include "Poco/SAX/EntityResolverImpl.h"
#include "Poco/SAX/InputSource.h"
#include "Poco/XML/XMLString.h"
#include "Poco/URI.h"
#include "Poco/Path.h"
#include "Poco/Exception.h"


using Poco::URIStreamOpener;
using Poco::URI;
using Poco::Path;
using Poco::Exception;
using Poco::IOException;
using Poco::OpenFileException;


namespace Poco {
namespace XML {


EntityResolverImpl::EntityResolverImpl():
	_opener(URIStreamOpener::defaultOpener())
{
}


EntityResolverImpl::EntityResolverImpl(const URIStreamOpener& opener):
	_opener(opener)
{
}


EntityResolverImpl::~EntityResolverImpl()
{
}


InputSource* EntityResolverImpl::resolveEntity(const XMLString* publicId, const XMLString& systemId)
{
	std::istream* pIstr = resolveSystemId(systemId);
	InputSource* pInputSource = new InputSource(systemId);
	if (publicId) pInputSource->setPublicId(*publicId);
	pInputSource->setByteStream(*pIstr);
	return pInputSource;
}

		
void EntityResolverImpl::releaseInputSource(InputSource* pSource)
{
	poco_check_ptr (pSource);

	delete pSource->getByteStream();
	delete pSource;
}


std::istream* EntityResolverImpl::resolveSystemId(const XMLString& systemId)
{
	std::string sid = fromXMLString(systemId);
	return _opener.open(sid);
}


} } // namespace Poco::XML
