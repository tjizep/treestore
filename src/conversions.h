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
#include "fields.h"
#ifdef _MSC_VER
#define CONVERSION_NOINLINE_ _declspec(noinline)
#else
#define CONVERSION_NOINLINE_
#endif
namespace units{
	static const double MB = 1024.0*1024.0;
	static const double GB = 1024.0*1024.0*1024.0;
}
namespace tree_stored{


	//typedef ColIndex::index_key CompositeStored;

	class conversions{
	public:

		enum{
			f_use_var_header = 1
		};
	private:
		/// conversion buffers
		String  attribute;
		String *r;

		my_decimal md_buffer;

	private:

	public:
		conversions() : attribute(32768) {
			attribute.set_charset(system_charset_info);
		}

		~conversions(){
		}


		/// setters ts->mysql
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::FloatStored &fs){

			f->store(fs.get_value());

		}
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::DoubleStored &ds){

			f->store(ds.get_value());

		}
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::ShortStored& s){

			f->store(s.get_value(),false);

		}
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::UShortStored& us){

			f->store(us.get_value(),true);

		}
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::CharStored& c){

			f->store(c.get_value(),false);

		}
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::UCharStored& uc ){

			f->store(uc.get_value());

		}

		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * fl, const stored::IntStored& i){
			Field_long * f = (Field_long*)fl;
			longstore(f->ptr, i.value);
		}

		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::UIntStored& ui){

			f->store(ui.get_value(),true);

		}
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::LongIntStored& li){

			f->store(li.get_value(),false);

		}
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::ULongIntStored& uli){

			f->store(uli.get_value(),true);

		}
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field_new_decimal * f, const stored::BlobStored& b){
			memcpy(f->ptr, b.chars(), b.get_size());
			f->bin_size = b.get_size();
		}
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::BlobStored& b){

			char * ptr = (char *)f->ptr;
			if(f->type() == MYSQL_TYPE_VARCHAR){
				Field_varstring * fv =static_cast< Field_varstring*>(f);
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
				fset(row, static_cast<Field_new_decimal*>(f), b); /// dynamic cast has really bad runtime performance
			}else
				f->store(b.chars(), (uint)b.get_size(), &my_charset_bin);

		}
		CONVERSION_NOINLINE_
		void fset(stored::_Rid row, Field * f, const stored::VarCharStored& b){

			enum_field_types et = f->type();
			char * ptr = (char *)f->ptr;
			if(et == MYSQL_TYPE_VARCHAR){
				Field_varstring * fv = static_cast< Field_varstring*>(f);
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
				Field_string * fv = static_cast<Field_string*>(f);
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
		inline void fget(stored::FloatStored &fs, Field * f, const uchar*, uint flags){
			fs.set_value((float)f->val_real());
		}
		inline void fget(stored::DoubleStored &ds, Field * f, const uchar*, uint flags){
			ds.set_value(f->val_real());
		}
		inline void fget(stored::ShortStored& s, Field * f, const uchar*, uint flags){
			s.set_value((int16)f->val_int());
		}
		inline void fget(stored::UShortStored& us, Field * f, const uchar*, uint flags){
			us.set_value((uint16)f->val_int());
		}
		inline void fget(stored::CharStored& c, Field * f, const uchar*, uint flags){
			c.set_value((int8)f->val_int());
		}
		inline void fget(stored::UCharStored& uc , Field * f, const uchar*, uint flags){
			uc.set_value((uint8)f->val_int());
		}
		inline void fget(stored::IntStored& i, Field * f, const uchar*, uint flags){
			i.set_value((int32)f->val_int());
		}
		inline void fget(stored::UIntStored& ui, Field * f, const uchar*, uint flags){
			ui.set_value((uint32)f->val_int());
		}
		inline void fget(stored::LongIntStored& li, Field * f, const uchar*, uint flags){
			li.set_value(f->val_int());
		}
		inline void fget(stored::ULongIntStored& uli, Field * f, const uchar*, uint flags){
			uli.set_value(f->val_int());
		}
		inline void fget(stored::BlobStored& b, Field_new_decimal * f, const uchar*n_ptr, uint flags){
			b.set((const char *)f->ptr,f->bin_size);
		}
		inline void fget(stored::BlobStored& b, Field * f, const uchar*n_ptr, uint flags){
			if(f->type()==MYSQL_TYPE_NEWDECIMAL){
				fget(b, static_cast<Field_new_decimal*>(f),n_ptr, flags);
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
		inline void fget(stored::VarCharStored& b, Field * f, const uchar*n_ptr, uint flags){
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
        typedef stored::DynamicKey CompositeStored;
		inline void fadd(CompositeStored& to, stored::FloatStored &fs, Field * f, const uchar*, uint flags){
			to.addf4((float)f->val_real());

		}
		inline void fadd(CompositeStored& to, stored::DoubleStored &ds, Field * f, const uchar*, uint flags){
			to.addf8(f->val_real());

		}
		inline void fadd(CompositeStored& to, stored::ShortStored& s, Field * f, const uchar*, uint flags){
			to.add2((nst::i16)f->val_int());

		}
		inline void fadd(CompositeStored& to, stored::UShortStored& us, Field * f, const uchar*, uint flags){
			to.addu2((nst::u16)f->val_int());

		}
		inline void fadd(CompositeStored& to, stored::CharStored& c, Field * f, const uchar*, uint flags){
			to.add1((nst::i8)f->val_int());

		}
		inline void fadd(CompositeStored& to, stored::UCharStored& uc , Field * f, const uchar*, uint flags){
			to.addu1((nst::u8)f->val_int());

		}
		inline void fadd(CompositeStored& to, stored::IntStored& i, Field * f, const uchar*, uint flags){
			to.add4((nst::i32)f->val_int());

		}
		inline void fadd(CompositeStored& to, stored::UIntStored& ui, Field * f, const uchar*, uint flags){
			to.addu4((nst::u32)f->val_int());

		}

		inline void fadd(CompositeStored& to, stored::LongIntStored& li, Field * f, const uchar*, uint flags){
			to.add8(f->val_int());

		}

		inline void fadd(CompositeStored& to, stored::ULongIntStored& uli, Field * f, const uchar*, uint flags){
			to.addu8(f->val_int());

		}

		inline void fadd(CompositeStored& to, stored::BlobStored& b, Field * f, const uchar*n_ptr, uint flags){
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

		inline void fadd(CompositeStored& to, stored::VarCharStored& b, Field * f, const uchar*n_ptr, uint flags){
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
		/// item conversions
		template<typename _IntType>
		inline void make_int_item_val(_IntType& i,const Item* val){
			Item::Type t = val->type();
			switch(t){
				case Item::INT_ITEM:
					i.set_value((typename _IntType::value_type)((const Item_int*)val)->value);
					break;
				case Item::REAL_ITEM:
					i.set_value((typename _IntType::value_type)((const Item_float*)val)->value);
					break;
				case Item::DECIMAL_ITEM:
					i.set_value((typename _IntType::value_type)((Item_decimal*)val)->val_int());
					break;
				case Item::STRING_ITEM:

					i.set_value((typename _IntType::value_type)((Item_string*)val)->val_int());
					break;
				default:
					i.set_value((typename _IntType::value_type)((Item*)val)->val_int());
					break;
			};
		}

		template<typename _FloatType>
		inline void make_float_item_val(_FloatType& i,const Item* val){
			switch(val->type()){
				case Item::INT_ITEM:
					i.set_value((typename _FloatType::value_type)((const Item_int*)val)->value);
					break;
				case Item::REAL_ITEM:
					i.set_value((typename _FloatType::value_type)((const Item_float*)val)->value);
					break;
				case Item::DECIMAL_ITEM:
					i.set_value((typename _FloatType::value_type)((Item_decimal*)val)->val_real());
					break;
				case Item::STRING_ITEM:

					i.set_value((typename _FloatType::value_type)((Item_string*)val)->val_real());
					break;
				default:
					i.set_value((typename _FloatType::value_type)((Item*)val)->val_real());
					break;
			};
		}

		template<typename _BinaryType>
		inline void make_binary_item_val(_BinaryType& b, Field* conversion, const Item* val){
			#ifdef _MSC_VER
			String *sv;
			switch(val->type()){
				case Item::INT_ITEM:
					b.add(((const Item_int*)val)->value);
					break;
				case Item::REAL_ITEM:
					b.add(((const Item_float*)val)->value);
					break;
				case Item::DECIMAL_ITEM:
					conversion->store_decimal(((Item_decimal*)val)->val_decimal(&md_buffer));
					fget(b,conversion,NULL,0);

					break;
				case Item::STRING_ITEM:
					sv = ((Item_string*)val)->val_str(&attribute);
					/// truncate query strings which may be longer than the field - if another kind of error happens the item will copy an empty string
					conversion->store(sv->ptr(), std::min<uint>(conversion->data_length(), sv->length()),sv->charset());
					fget(b,conversion,NULL,0);
					break;
				default:
					break;
			};
			#endif
		}

		inline void make_item_val(stored::FloatStored &fs, Field* , const Item* val){
			make_float_item_val(fs, val);
		}

		inline void make_item_val(stored::DoubleStored &ds, Field* , const Item* val){
			make_float_item_val(ds, val);
		}

		inline void make_item_val(stored::ShortStored& s, Field* , const Item* val){
			make_int_item_val(s, val);
		}

		inline void make_item_val(stored::UShortStored& us, Field* , const Item* val){
			make_int_item_val(us, val);
		}

		inline void make_item_val(stored::CharStored& c, Field* , const Item* val){
			make_int_item_val(c, val);
		}

		inline void make_item_val(stored::UCharStored& uc, Field*  , const Item* val){
			make_int_item_val(uc, val);
		}

		inline void make_item_val(stored::IntStored& i, Field* , const Item* val){
			make_int_item_val(i, val);

		}

		inline void make_item_val(stored::UIntStored& ui, Field* , const Item* val){
			make_int_item_val(ui, val);
		}

		inline void make_item_val(stored::LongIntStored& li, Field*, const Item* val){
			make_int_item_val(li, val);
		}

		inline void make_item_val(stored::ULongIntStored& uli, Field*, const Item* val){
			make_int_item_val(uli, val);
		}

		inline void make_item_val(stored::BlobStored& b, Field* conversion, const Item* val){
			make_binary_item_val(b, conversion, val);
		}


		inline void make_item_val(stored::VarCharStored& b, Field* conversion, const Item* val){
			make_binary_item_val(b, conversion, val);
		}

	};
};
#endif
