//
// CipherKeyImpl.cpp
//
// $Id: //poco/1.4/Crypto/src/CipherKeyImpl.cpp#1 $
//
// Library: Crypto
// Package: Cipher
// Module:  CipherKeyImpl
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
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


#include "Poco/Crypto/CipherKeyImpl.h"
#include "Poco/Crypto/CryptoTransform.h"
#include "Poco/Crypto/CipherFactory.h"
#include "Poco/Exception.h"
#include "Poco/RandomStream.h"
#include <openssl/err.h>
#include <openssl/evp.h>


namespace Poco {
namespace Crypto {


CipherKeyImpl::CipherKeyImpl(const std::string& name, 
	const std::string& passphrase, 
	const std::string& salt,
	int iterationCount):
	_pCipher(0),
	_name(name),
	_key(),
	_iv()
{
	// dummy access to Cipherfactory so that the EVP lib is initilaized
	CipherFactory::defaultFactory();
	_pCipher = EVP_get_cipherbyname(name.c_str());

	if (!_pCipher)
		throw Poco::NotFoundException("Cipher " + name + " was not found");
	_key = ByteVec(keySize());
	_iv = ByteVec(ivSize());
	generateKey(passphrase, salt, iterationCount);
}


CipherKeyImpl::CipherKeyImpl(const std::string& name, 
	const ByteVec& key, 
	const ByteVec& iv):
	_pCipher(0),
	_name(name),
	_key(key),
	_iv(iv)
{
	// dummy access to Cipherfactory so that the EVP lib is initilaized
	CipherFactory::defaultFactory();
	_pCipher = EVP_get_cipherbyname(name.c_str());

	if (!_pCipher)
		throw Poco::NotFoundException("Cipher " + name + " was not found");
}

	
CipherKeyImpl::CipherKeyImpl(const std::string& name):
	_pCipher(0),
	_name(name),
	_key(),
	_iv()
{
	// dummy access to Cipherfactory so that the EVP lib is initilaized
	CipherFactory::defaultFactory();
	_pCipher = EVP_get_cipherbyname(name.c_str());

	if (!_pCipher)
		throw Poco::NotFoundException("Cipher " + name + " was not found");
	_key = ByteVec(keySize());
	_iv = ByteVec(ivSize());
	generateKey();
}


CipherKeyImpl::~CipherKeyImpl()
{
}


CipherKeyImpl::Mode CipherKeyImpl::mode() const
{
	switch (EVP_CIPHER_mode(_pCipher))
	{
	case EVP_CIPH_STREAM_CIPHER:
		return MODE_STREAM_CIPHER;

	case EVP_CIPH_ECB_MODE:
		return MODE_ECB;

	case EVP_CIPH_CBC_MODE:
		return MODE_CBC;

	case EVP_CIPH_CFB_MODE:
		return MODE_CFB;

	case EVP_CIPH_OFB_MODE:
		return MODE_OFB;
	}
	throw Poco::IllegalStateException("Unexpected value of EVP_CIPHER_mode()");
}


void CipherKeyImpl::generateKey()
{
	ByteVec vec;

	getRandomBytes(vec, keySize());
	setKey(vec);
	
	getRandomBytes(vec, ivSize());
	setIV(vec);
}


void CipherKeyImpl::getRandomBytes(ByteVec& vec, std::size_t count)
{
	Poco::RandomInputStream random;
	
	vec.clear();
	vec.reserve(count);

	for (int i = 0; i < count; ++i)
		vec.push_back(static_cast<unsigned char>(random.get()));
}


void CipherKeyImpl::generateKey(
	const std::string& password,
	const std::string& salt,
	int iterationCount)
{
	unsigned char keyBytes[EVP_MAX_KEY_LENGTH];
	unsigned char ivBytes[EVP_MAX_IV_LENGTH];

	// OpenSSL documentation specifies that the salt must be an 8-byte array.
	unsigned char saltBytes[8];

	if (!salt.empty())
	{
		int len = static_cast<int>(salt.size());
		// Create the salt array from the salt string
		for (int i = 0; i < 8; ++i)
			saltBytes[i] = salt.at(i % len);
		for (int i = 8; i < len; ++i)
			saltBytes[i % 8] ^= salt.at(i);
	}

	// Now create the key and IV, using the MD5 digest algorithm.
	int keySize = EVP_BytesToKey(
		_pCipher,
		EVP_md5(),
		(salt.empty() ? 0 : saltBytes),
		reinterpret_cast<const unsigned char*>(password.data()),
		static_cast<int>(password.size()),
		iterationCount,
		keyBytes,
		ivBytes);

	// Copy the buffers to our member byte vectors.
	_key.assign(keyBytes, keyBytes + keySize);

	if (ivSize() == 0)
		_iv.clear();
	else
		_iv.assign(ivBytes, ivBytes + ivSize());
}


int CipherKeyImpl::keySize() const
{
	return EVP_CIPHER_key_length(_pCipher);
}


int CipherKeyImpl::blockSize() const
{
	return EVP_CIPHER_block_size(_pCipher);
}


int CipherKeyImpl::ivSize() const
{
	return EVP_CIPHER_iv_length(_pCipher);
}


} } // namespace Poco::Crypto
