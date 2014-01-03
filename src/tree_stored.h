/*****************************************************************************

Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
Copyright (c) 2008, 2009 Google Inc.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2012, Facebook Inc.
Copyright (c) 2013, Christiaan Pretorius

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA

*****************************************************************************/
#ifndef _TREE_STORED_H_CEP2013_
#define _TREE_STORED_H_CEP2013_
#include "collumn.h"
namespace tree_stored{
	typedef collums::_Rid _Rid;
	static const _Rid MAX_ROWS = collums::MAX_ROWS;
	typedef collums::col_index ColIndex;
	typedef ColIndex::IndexIterator IndexIterator ;
	typedef ColIndex::index_key CompositeStored;
	typedef ColIndex::iterator_type BasicIterator;
	typedef std::vector<int> _Parts;

	typedef collums::FloatStored FloatStored ;
	typedef collums::DoubleStored DoubleStored ;
	typedef collums::ShortStored ShortStored;
	typedef collums::UShortStored UShortStored;
	typedef collums::CharStored CharStored;
	typedef collums::UCharStored UCharStored;
	typedef collums::IntStored IntStored;
	typedef collums::UIntStored UIntStored;
	typedef collums::LongIntStored LongIntStored;
	typedef collums::ULongIntStored ULongIntStored;
	typedef collums::BlobStored BlobStored;
	typedef collums::VarCharStored VarCharStored;
};
#endif