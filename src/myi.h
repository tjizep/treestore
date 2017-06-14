/*
Copyright (c) 2012,2013,2014 Christiaan Pretorius. All rights reserved.
MySQL TreeStore Storage Engine
myi.cpp" - MySQL Storage Engine Interfaces
*/

#pragma once
#ifdef _MSC_VER
#pragma warning(disable:4800)
#pragma warning(disable:4267)
#endif
//#define ENABLED_DEBUG_SYNC
//#define NDEBUG
#define DBUG_OFF

#define MYSQL_DYNAMIC_PLUGIN

#define MYSQL_SERVER 1

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation                          // gcc: Class implementation
#endif


#define MYSQL_SERVER 1

#ifndef _MSC_VER
#include <cmath>
//#define isfinite std::isfinite
#endif
#include <algorithm>
//#include "sql_priv.h"
//#include "probes_mysql.h"
#include "key.h"                                // key_copy
#include "sql_plugin.h"
#include <m_ctype.h>
#include <my_bit.h>
#include <stdarg.h>

#include "sql_table.h"                          // tablename_to_filename
#include "sql_class.h"                          // THD

#include <limits>
#include <map>
#include <string>
#include "collumn.h"
#include "tree_stored.h"
#include "conversions.h"
#include "tree_index.h"
#include "tree_table.h"
