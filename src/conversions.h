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
#ifndef _CONVERSIONS_H_CEP2013_
#define _CONVERSIONS_H_CEP2013_

#ifndef _MSC_VER
#include <cmath>
#define isfinite std::isfinite
#endif

#include "sql_class.h"           // SSV

#include <mysql.h>
#include <mysql/plugin.h>
#ifdef _MSC_VER
#define CONVERSION_NOINLINE_ //_declspec(noinline) 
#else
#define CONVERSION_NOINLINE_ 
#endif
namespace tree_stored{


	//typedef ColIndex::index_key CompositeStored;
	
	class conversions{
	public:
		typedef _Rid _StorageType;
		static const _StorageType INVALID_STORAGE = (_StorageType)-1;
		struct _RowLength{
			_RowLength() : length(0), storage(INVALID_STORAGE){

			}
			nst::u16 length;
			_StorageType storage;
		};
		typedef std::vector<_RowLength> _DataMapper;
		typedef std::vector<uchar> _Data;

		enum{
			f_use_var_header = 1
		};
	private:
		static const bool _CONVERSION_CACHE_ = true;
		static const nst::u64 MAX_CONVERT_CACHE = 1024*1024*64;
		String  attribute;
		String *r;
		

		
	private:
		
		
	public:	
		conversions() : attribute(32768) {
			attribute.set_charset(system_charset_info);
		}
		
		~conversions(){
		}
		
		void grow_row(_Rid row){
		
		}
		
		

		/// setters ts->mysql
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const FloatStored &fs){
			
			f->store(fs.get_value());
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const DoubleStored &ds){
			
			f->store(ds.get_value());
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const ShortStored& s){
			
			f->store(s.get_value(),false);
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const UShortStored& us){
			
			f->store(us.get_value(),true);
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const CharStored& c){
			
			f->store(c.get_value(),false);
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const UCharStored& uc ){
			
			f->store(uc.get_value());
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * fl, const IntStored& i){
			Field_long * f = (Field_long*)fl;
			//f->store(i.get_value(),false);
			
			*(long*)f->ptr = i.value;
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const UIntStored& ui){
		
			f->store(ui.get_value(),true);
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const LongIntStored& li){
			
			f->store(li.get_value(),false);
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const ULongIntStored& uli){
		
			f->store(uli.get_value(),true);
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field_new_decimal * f, const BlobStored& b){
			memcpy(f->ptr, b.chars(), b.get_size());
			f->bin_size = b.get_size();
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const BlobStored& b){
			
			char * ptr = (char *)f->ptr;
			if(f->type() == MYSQL_TYPE_VARCHAR){
				Field_varstring * fv = (Field_varstring*)f;
				uint lb = fv->length_bytes;
				uint fl = fv->field_length;
				uint cl = (uint)b.get_size();
				if(fl <= cl) cl = fl;

				memcpy(ptr + lb,b.chars(),cl);
				if (lb == 1)
					*ptr= (uchar) cl;
				else
					int2store(ptr, cl);
			}else if(f->type()==MYSQL_TYPE_NEWDECIMAL){
				fset(row, (Field_new_decimal*)f, b);
			}else
				f->store(b.chars(), (uint)b.get_size(), &my_charset_bin);
			
		}
		CONVERSION_NOINLINE_ 
		void fset(_Rid row, Field * f, const VarCharStored& b){
			
			enum_field_types et = f->type();
			char * ptr = (char *)f->ptr;
			if(et == MYSQL_TYPE_VARCHAR){
				Field_varstring * fv = (Field_varstring*)f;
				uint lb = fv->length_bytes;
				uint fl = fv->field_length;
				uint cl = (uint)b.get_size() - 1;
				if(fl <= cl) cl = fl;

				memcpy(ptr + lb,b.chars(),cl);
				if (lb == 1)
					*ptr= (uchar) cl;
				else
					int2store(ptr, cl);
			}else if(et == MYSQL_TYPE_STRING){
				Field_string * fv = (Field_string*)f;
				uint lb = fv->field_length;
				uint cl = (uint)b.get_size() - 1;
				if(cl < lb){
					memset(ptr + cl, ' ', lb-cl);
					memcpy(ptr, b.chars(), cl);
				}else{
					memcpy(ptr, b.chars(), lb);
				}

			}else f->store(b.chars(), (uint)b.get_size()-1, &my_charset_bin);
			
		}
		/// getters mysql->ts
		inline void fget(FloatStored &fs, Field * f, const uchar*, uint flags){
			fs.set_value((float)f->val_real());
		}
		inline void fget(DoubleStored &ds, Field * f, const uchar*, uint flags){
			ds.set_value(f->val_real());
		}
		inline void fget(ShortStored& s, Field * f, const uchar*, uint flags){
			s.set_value((short)f->val_int());
		}
		inline void fget(UShortStored& us, Field * f, const uchar*, uint flags){
			us.set_value((unsigned short)f->val_int());
		}
		inline void fget(CharStored& c, Field * f, const uchar*, uint flags){
			c.set_value((char)f->val_int());
		}
		inline void fget(UCharStored& uc , Field * f, const uchar*, uint flags){
			uc.set_value((unsigned char)f->val_int());
		}
		inline void fget(IntStored& i, Field * f, const uchar*, uint flags){
			i.set_value((int)f->val_int());
		}
		inline void fget(UIntStored& ui, Field * f, const uchar*, uint flags){
			ui.set_value((unsigned int)f->val_int());
		}
		inline void fget(LongIntStored& li, Field * f, const uchar*, uint flags){
			li.set_value(f->val_int());
		}
		inline void fget(ULongIntStored& uli, Field * f, const uchar*, uint flags){
			uli.set_value(f->val_int());
		}
		inline void fget(BlobStored& b, Field_new_decimal * f, const uchar*n_ptr, uint flags){
			b.set((const char *)f->ptr,f->bin_size);
		}
		inline void fget(BlobStored& b, Field * f, const uchar*n_ptr, uint flags){
			if(f->type()==MYSQL_TYPE_NEWDECIMAL){
				fget(b, dynamic_cast<Field_new_decimal*>(f),n_ptr, flags);		
				return;
			}
			if(flags & f_use_var_header){
				String varchar;
				uint var_length= uint2korr(n_ptr);
				varchar.set_quick((char*) n_ptr+HA_KEY_BLOB_LENGTH, var_length, &my_charset_bin);
				b.set(varchar.ptr(), varchar.length());//, varchar.length()
				return;
			}

			r = n_ptr==NULL ? f->val_str(&attribute): f->val_str(&attribute,n_ptr);
			b.set(r->ptr(),r->length());//
		}
		inline void fget(VarCharStored& b, Field * f, const uchar*n_ptr, uint flags){
			if(flags & f_use_var_header){
				String varchar;
				uint var_length= uint2korr(n_ptr);
				varchar.set_quick((char*) n_ptr+HA_KEY_BLOB_LENGTH, var_length, &my_charset_bin);
				b.setterm(varchar.ptr(), varchar.length());//, varchar.length()
				return;
			}

			r = n_ptr==NULL ? f->val_str(&attribute): f->val_str(&attribute,n_ptr);
			b.setterm(r->ptr(),r->length());//
		}

		/// direct getters into the composite type for indexes

		inline void fadd(CompositeStored& to, FloatStored &fs, Field * f, const uchar*, uint flags){
			to.addf4((float)f->val_real());

		}
		inline void fadd(CompositeStored& to, DoubleStored &ds, Field * f, const uchar*, uint flags){
			to.addf8(f->val_real());

		}
		inline void fadd(CompositeStored& to, ShortStored& s, Field * f, const uchar*, uint flags){
			to.add2((short)f->val_int());

		}
		inline void fadd(CompositeStored& to, UShortStored& us, Field * f, const uchar*, uint flags){
			to.addu2((unsigned short)f->val_int());

		}
		inline void fadd(CompositeStored& to, CharStored& c, Field * f, const uchar*, uint flags){
			to.add1((char)f->val_int());

		}
		inline void fadd(CompositeStored& to, UCharStored& uc , Field * f, const uchar*, uint flags){
			to.addu1((unsigned char)f->val_int());

		}
		inline void fadd(CompositeStored& to, IntStored& i, Field * f, const uchar*, uint flags){
			to.add4((int)f->val_int());

		}
		inline void fadd(CompositeStored& to, UIntStored& ui, Field * f, const uchar*, uint flags){
			to.addu4((unsigned int)f->val_int());

		}

		inline void fadd(CompositeStored& to, LongIntStored& li, Field * f, const uchar*, uint flags){
			to.add8(f->val_int());

		}

		inline void fadd(CompositeStored& to, ULongIntStored& uli, Field * f, const uchar*, uint flags){
			to.addu8(f->val_int());

		}

		inline void fadd(CompositeStored& to, BlobStored& b, Field * f, const uchar*n_ptr, uint flags){
			if(flags & f_use_var_header){
				String varchar;
				uint var_length= uint2korr(n_ptr);
				varchar.set_quick((char*) n_ptr+HA_KEY_BLOB_LENGTH, var_length, &my_charset_bin);
				to.add(varchar.ptr(), varchar.length());//, varchar.length()
				return;
			}


			r = n_ptr==NULL ? f->val_str(&attribute): f->val_str(&attribute,n_ptr);
			to.add(r->ptr(),r->length());//
		}

		inline void fadd(CompositeStored& to, VarCharStored& b, Field * f, const uchar*n_ptr, uint flags){
			if(flags & f_use_var_header){
				String varchar;
				uint var_length= uint2korr(n_ptr);
				varchar.set_quick((char*) n_ptr+HA_KEY_BLOB_LENGTH, var_length, &my_charset_bin);
				to.addTerm(varchar.ptr(),varchar.length());//, varchar.length()
				return;
			}
			if(n_ptr != NULL){

				r = f->val_str(&attribute,n_ptr);
				to.addTerm(r->ptr(),r->length());
				return;
			}
			r =  f->val_str(&attribute);
			to.addTerm(r->ptr(),r->length());
			return;
		}
	};
};
#endif
