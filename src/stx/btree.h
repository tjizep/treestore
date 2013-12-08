// $Id: btree.h 130 2011-05-18 08:24:25Z tb $ -*- fill-column: 79 -*-
/** \file btree.h
* Contains the main B+ tree implementation template class btree.
*/

/*
* STX B+ Tree Storage Template Classes v s.9.0
*/
/*****************************************************************************

Copyright (c) 2000, 2013, Oracle and/or its affiliates. All rights reserved.
Copyright (c) 2008, 2009 Google Inc.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2012, Facebook Inc.
Copyright (C) 2008-2011 Timo Bingmann
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
///
/// TODO: There are some const violations inherent to a smart pointer
/// implementation of persisted pages. These are limited to 4 such cases
/// found and explained in the proxy smart pointer template type.
///
/// NOTE: NB: for purposes of sanity the STL operator*() and -> dereference
/// operators on the iterator has been removed so that inter tree concurrent
/// leaf page sharing can be done efficiently. only the key() and data()
/// members remain to access keys and values seperately

#ifndef _STX_BTREE_H_
#define _STX_BTREE_H_



// *** Required Headers from the STL

#include <algorithm>
#include <functional>

#include <istream>
#include <ostream>
#include <memory>
#include <cstddef>

#include <assert.h>
#include <vector>
#include <set>
#include <map>
#include <unordered_map>
#include <stx/storage/basic_storage.h>
#include <Poco/AtomicCounter.h>
#include <Poco/Mutex.h>
#include <Poco/Thread.h>
#include <Poco/ThreadPool.h>
#include <Poco/TaskManager.h>
#include <Poco/Task.h>
#include <Poco/Timestamp.h>

#include "NotificationQueueWorker.h"
// *** Debugging Macros

#ifdef BTREE_DEBUG

#include <iostream>

/// Print out debug information to std::cout if BTREE_DEBUG is defined.
#define BTREE_PRINT(x,...)          do { if (debug) (sprintf(stderr, x, __VA_ARGS__)); } while(0)

/// Assertion only if BTREE_DEBUG is defined. This is not used in verify().
#define BTREE_ASSERT(x)         do { assert(x); } while(0)

#else

/// Print out debug information to std::cout if BTREE_DEBUG is defined.
#define BTREE_PRINT(x,...)          do { } while(0)

/// Assertion only if BTREE_DEBUG is defined. This is not used in verify().
#define BTREE_ASSERT(x)         do { } while(0)

#endif

/// std::max function does not work for initializing static const limiters
#ifndef max_const
#define max_const(a,b)            (((a) > (b)) ? (a) : (b))
#endif

// * define an os related realtime clock

#define OS_CLOCK Poco::Timestamp().epochMicroseconds


/// Compiler options for optimization
#ifdef _MSC_VER
#pragma warning(disable:4503)
#endif
#ifdef _MSC_VER
#define FORCE_INLINE __forceinline
#define NO_INLINE _declspec(noinline)
#else
#define FORCE_INLINE
#define NO_INLINE
#endif
/// STX - Some Template Extensions namespace

extern ptrdiff_t btree_totl_used ;
extern ptrdiff_t btree_totl_instances ;
extern void add_btree_totl_used(ptrdiff_t added);
extern void remove_btree_totl_used(ptrdiff_t added);
class malformed_page_exception : public std::exception{
public:
    malformed_page_exception() throw(){};
};

namespace nst = stx::storage;
namespace stx
{

	template<typename _KeyType>
	struct interpolator{

		inline bool encoded( bool ) const {
			return false;
		}

		inline void encode(nst::buffer_type::iterator&, const _KeyType*, nst::u16) const {
			
		}
		inline void decode(nst::buffer_type::const_iterator&, _KeyType*, nst::u16) const {	
		}

		int encoded_size(const _KeyType*, nst::u16){
			return 0;
		}
	};



	struct def_p_traits /// persist traits
	{
		typedef storage::stream_address stream_address;
		typedef unsigned long relative_address;

	};

	/** Generates default traits for a B+ tree used as a set. It estimates surface and
	* interior node sizes by assuming a cache line size of config::bytes_per_page bytes. */
	struct btree_traits
	{
		enum
		{
			bytes_per_page = 4096, /// this isnt currently used but could be 
			max_scan = 3,
			interior_mul = 1,
			keys_per_page = 512
		};
	};

	template <typename _Key, typename _PersistTraits >
	struct btree_default_set_traits
	{
		

		/// If true, the tree will self verify it's invariants after each insert()
		/// or erase(). The header must have been compiled with BTREE_DEBUG defined.
		static const bool   selfverify = false;

		/// If true, the tree will print out debug information and a tree dump
		/// during insert() or erase() operation. The header must have been
		/// compiled with BTREE_DEBUG defined and key_type must be std::ostream
		/// printable.
		static const bool   debug = false;
		
		/// persistence related addressing
		typedef def_p_traits persist_traits;

		typedef _Key key_proxy;

		/// Number of slots in each surface of the tree. Estimated so that each node
		/// has a size of about btree_traits::bytes_per_page bytes.
		static const int    surfaces = btree_traits::keys_per_page; //max_const( 8l, btree_traits::bytes_per_page / (sizeof(key_proxy)) );

		/// Number of slots in each interior node of the tree. Estimated so that each node
		/// has a size of about btree_traits::bytes_per_page bytes.
		static const int    interiorslots = btree_traits::keys_per_page; //max_const( 8l, btree_traits::bytes_per_page / (sizeof(key_proxy) + sizeof(void*)) );

		///
		/// the max scan value for the hybrid lower bound that allows bigger pages
		static const int    max_scan = btree_traits::max_scan;

	};

	/** Generates default traits for a B+ tree used as a map. It estimates surface and
	* interior node sizes by assuming a cache line size of btree_traits::bytes_per_page bytes. */
	template <typename _Key, typename _Data, typename _PersistTraits >
	struct bt_def_map_t /// default map traits
	{

		/// If true, the tree will self verify it's invariants after each insert()
		/// or erase(). The header must have been compiled with BTREE_DEBUG defined.
		static const bool   selfverify = false;

		/// If true, the tree will print out debug information and a tree dump
		/// during insert() or erase() operation. The header must have been
		/// compiled with BTREE_DEBUG defined and key_type must be std::ostream
		/// printable.
		static const bool debug = false;

		/// traits defining persistence mechanisms and types

		typedef _PersistTraits persist_traits;

		/// the persist proxy's and context if required
		/// usually these are synonymous with the _Key and _Data types

		typedef _Key key_proxy;

		typedef _Data data_proxy;


		/// Number of slots in each surface of the tree. A page has a size of about btree_traits::bytes_per_page bytes.

		static const int    surfaces = btree_traits::keys_per_page;//max_const( 8l, (btree_traits::bytes_per_page) / (sizeof(key_proxy) + sizeof(data_proxy)) ); //

		/// Number of slots in each interior node of the tree. a Page has a size of about btree_traits::bytes_per_page bytes.

		static const int    interiorslots = btree_traits::keys_per_page;//max_const( 8l,  (btree_traits::bytes_per_page*btree_traits::interior_mul) / (sizeof(key_proxy) + sizeof(void*)) );//
		///
		/// the max scan value for the hybrid lower bound that allows bigger pages
		static const int    max_scan = btree_traits::max_scan;

	};

	enum states
	{
		initial = 1,
		created,
		unloaded,
		loaded,
		changed
	};

	struct node_ref{
		/// references for eviction
		int refs;
		states s;


		node_ref() : refs(0), s(initial)//, shared(false)
		{
		}
	};
	/// as a test the null ref was actually defined as a real address
	/// static node_ref null_ref;
	/// static node_ref * NULL_REF = &null_ref;

	/**
	* The base implementation of a stored B+ tree. Pages can exist in encoded or
	* decoded states. Insert is optimized using the stl::sort which is a hybrid
	* sort
	*
	* This class is specialized into btree_set, btree_multiset, btree_map and
	* btree_multimap using default template parameters and facade functions.
	*/



	#define NULL_REF NULL


	template <typename _Key, typename _Data, typename _Storage,
		typename _Value = std::pair<_Key, _Data>,
		typename _Compare = std::less<_Key>,
		typename _Iterpolator = stx::interpolator<_Key> ,
		typename _Traits = bt_def_map_t<_Key, _Data, def_p_traits>,
		bool _Duplicates = false,
		typename _Alloc = std::allocator<_Value> >
	class btree
	{
	public:
		// *** Template Parameter Types

		/// Fifth template parameter: Traits object used to define more parameters
		/// of the B+ tree
		typedef _Traits traits;

		/// First template parameter: The key type of the B+ tree. This is stored
		/// in interior nodes and leaves
		typedef typename traits::key_proxy  key_type;

		/// Second template parameter: The data type associated with each
		/// key. Stored in the B+ tree's leaves
		typedef _Data                       data_type;

		/// Third template parameter: Composition pair of key and data types, this
		/// is required by the STL standard. The B+ tree does not store key and
		/// data together. If value_type == key_type then the B+ tree implements a
		/// set.
		typedef _Value                      value_type;

		/// Fourth template parameter: Key comparison function object
		/// NOTE: this comparator is on the 'default type' of the proxy
		typedef _Compare					key_compare;

		/// Fourth template parameter: interpolator if applicable
		typedef _Iterpolator				key_interpolator;

		/// Fifth template parameter: Allow duplicate keys in the B+ tree. Used to
		/// implement multiset and multimap.
		static const bool                   allow_duplicates = _Duplicates;

		/// Seventh template parameter: STL allocator for tree nodes
		typedef _Alloc                      allocator_type;

		/// the persistent or not storage type
		typedef _Storage                    storage_type;

		/// the persistent buffer type for encoding pages
		typedef storage::buffer_type		buffer_type;

		typedef typename _Traits::persist_traits persist_traits;

		typedef typename persist_traits::stream_address stream_address;
	public:
		// *** Constructed Types

		/// Typedef of our own type
		typedef btree<key_type, data_type,  storage_type, value_type, key_compare,
			key_interpolator, traits, allow_duplicates, allocator_type> btree_self;

		/// Size type used to count keys
		typedef size_t                              size_type;

		/// The pair of key_type and data_type, this may be different from value_type.
		typedef std::pair<key_type, data_type>      pair_type;

	public:
		// *** Static Constant Options and Values of the B+ Tree

		/// Base B+ tree parameter: The number of key/data slots in each surface
		static const unsigned short         surfaceslotmax =  traits::surfaces;

		/// Base B+ tree parameter: The number of key slots in each interior node,
		/// this can differ from slots in each surface.
		static const unsigned short         interiorslotmax =  traits::interiorslots;

		/// Computed B+ tree parameter: The minimum number of key/data slots used
		/// in a surface. If fewer slots are used, the surface will be merged or slots
		/// shifted from it's siblings.
		static const unsigned short minsurfaces = (surfaceslotmax / 2);

		/// Computed B+ tree parameter: The minimum number of key slots used
		/// in an interior node. If fewer slots are used, the interior node will be
		/// merged or slots shifted from it's siblings.
		static const unsigned short mininteriorslots = (interiorslotmax / 2);

		/// Debug parameter: Enables expensive and thorough checking of the B+ tree
		/// invariants after each insert/erase operation.
		static const bool                   selfverify = traits::selfverify;

		/// Debug parameter: Prints out lots of debug information about how the
		/// algorithms change the tree. Requires the header file to be compiled
		/// with BTREE_DEBUG and the key type must be std::ostream printable.
		static const bool                   debug = traits::debug;

		/// forward decl.

		class tree_stats;
	private:
		// *** Node Classes for In-Memory and Stored Nodes

		/// proxy and interceptor class for reference counted pointers
		/// and automatic loading

		struct mini_pointer{
			//node_ref * ptr;
			stream_address w;

		};
		class base_proxy
		{
		protected:

			btree * context;
		public:
			node_ref * ptr;
			stream_address w;

			void make_mini(mini_pointer& m) const {
				/*if(ptr != NULL_REF && ptr->shared){
					m.ptr = (*this).ptr;
				}else{
					m.ptr = NULL_REF;
				}*/
				m.w = (*this).w;
			}
			inline void set_where(stream_address w){

				(*this).w = w;

			}
			inline stream_address get_where() const {
				return (*this).w;
			}

		public:

			inline states get_state() const
			{
				if((*this).ptr != NULL_REF){
					return (*this).ptr->s;
				}
				return w ? unloaded : initial;
			}

			inline void set_state(states s){
				if((*this).ptr != NULL_REF){
					(*this).ptr->s = s;
				}
			}

			void set_loaded(){
				set_state(loaded);
			}

			inline bool has_context() const {
				return (*this).context!=NULL;
			}


			inline btree * get_context() const {
				return context;
			}

			storage_type* get_storage() const {
				return get_context()->get_storage();
			}

			inline bool is_loaded() const{
				return (*this).ptr != NULL_REF;
			}

			inline void set_context(btree * context){
				if((*this).context == context) return;
				if((*this).context != NULL && context != NULL){
					printf("suspicious context transfer\n");
				}
				(*this).context = context;
			}

			base_proxy():context(NULL),ptr(NULL_REF),w(0)
			{

			}

			base_proxy(const node_ref * ptr)
			:	context(NULL),ptr((node_ref *)ptr),w(0){

			}

			base_proxy(const node_ref * ptr, stream_address w)
			:   context(NULL),ptr((node_ref *)ptr),w(0){

			}

			~base_proxy()
			{
			}
			inline bool not_equal(const base_proxy& left) const{

				return left.w != (*this).w;

			}
			inline bool equals(const base_proxy& left) const{

				return left.w == (*this).w;

			}

		};

		template<typename _Loaded>
		class pointer_proxy : public base_proxy
		{

		private:

			typedef base_proxy super;

			inline void clock_synch(){
				//if((*this).ptr)
				//	static_cast<_Loaded*>((*this).ptr)->cc = ++stx::cgen;
			}
			inline void unref(typename btree::surface_node* ptr){
				if(ptr != NULL_REF){
					if(ptr->shared)
						--(ptr->a_refs);
					else
						ptr->refs--;
				}
			}
			inline void ref(typename btree::surface_node* ptr){
				if(ptr != NULL_REF){
					if(ptr->shared)
						++(ptr->a_refs);
					else
						ptr->refs++;
				}
			}

			inline void unref(typename btree::interior_node* ptr){
				if(ptr != NULL_REF){
					ptr->refs--;
				}
			}
			inline void ref(typename btree::interior_node* ptr){
				if(ptr != NULL_REF){
					ptr->refs++;
				}
			}
			inline void unref(typename btree::node* ptr){
				if(ptr != NULL_REF)
				{
					if(ptr->issurfacenode())
					{
						unref(static_cast<surface_node*>(ptr));

					}else
					{
						unref(static_cast<interior_node*>(ptr));
					}
				}
			}
			inline void ref(typename btree::node* ptr){
				if(ptr != NULL_REF){
					if(ptr->issurfacenode()){
						ref(static_cast<surface_node*>(ptr));
					}else{
						ref(static_cast<interior_node*>(ptr));
					}
				}
			}
		public:
			inline void realize(const mini_pointer& m,btree * context){
				//if(m.ptr!=NULL_REF && m.ptr->shared){
				//	(*this).ptr = m.ptr;
				//}else
				//	(*this).ptr = NULL_REF;
				(*this).w = m.w;
				(*this).context = context;
				if(context==NULL&&(*this).w){
					printf("this is NULL\n");
				}

				ref();
			}
			inline bool has_context() const {
				return super::has_context();
			}

			btree* get_context() const {
				return super::get_context();
			}

			void set_context(btree* context){
				super::set_context(context);
			}

			/// adds a reference for page management
			inline void ref(){


					ref(static_cast<_Loaded*>(super::ptr));
			}

			/// removes a reference for page management
			inline void unref(){

					unref(static_cast<_Loaded*>(super::ptr));

			}

			/// called to set the state and members correcly when member ptr is marked as created
			/// requires non zero w (wHere it gets stored) parameter

			void create(btree * context, stream_address w){

				super::w = w;
				set_context( context );

				if(!w)
				{
					if(context->get_storage()->is_readonly()){

						printf("allocating new page in readonly mode\n");
						throw std::exception();
					}
					context->get_storage()->allocate(super::w, stx::storage::create);
					context->get_storage()->complete();
				}
				(*this).set_state( created );
			}


			/// load the persist proxy and set its state to changed
			void change(){

				load();
				switch((*this).get_state()){
				case created:
					break;
				case changed:
					break;
				default:
					(*this).set_state(changed);

				}
				if(rget()->shared){
					throw std::exception();//"cannot change state of shared node"
				}

			}

			void _save(btree & context){

				switch((*this).get_state())
				{
				case created:
				case changed:

						/// exit with a valid state
						context.save(*this);

				break;
				case loaded:
				case initial:
				case unloaded:
				break;
				}

			}

			void save(btree & context){
				_save(context);

			}

			/// update the linked list when pages/nodes are evicted so that the
			/// the current ptr next and preceding members are unloaded
			/// removing any extra refrences to ptr

			void update_links(typename btree::node* l){
				if(rget()->issurfacenode()){
					update_links(static_cast<surface_node*>(rget()));
				}

			}

			void update_links(typename btree::interior_node* ){
			}

			void update_links(typename btree::surface_node* l){
				if(rget()->issurfacenode()){
					if(l->preceding.is_loaded()){
						surface_node * p = static_cast<surface_node*>(l->preceding.rget()->next.rget());
						if(p && p != l){
							BTREE_PRINT("invalid link or version\n");
						}
						l->preceding.rget()->next.unload();
						l->preceding.unload();
					}
					if(l->next.is_loaded()){
						surface_node * n = static_cast<surface_node*>(l->next.rget()->preceding.rget());
						if(n && n != l){
							BTREE_PRINT("invalid link or version\n");
						}
						l->next.rget()->preceding.unload();
						l->next.unload();
					}
				}else{
					BTREE_PRINT("surface node reports its not a surface node\n");
				}
			}

			/// if the state is set to loaded and a valid wHere is set the proxy will change state to unloaded

			void unload(){
				save(*get_context());
				if((*this).get_state()==loaded && super::w){
					unref();
					get_context()->free_node(static_cast<_Loaded*>((*this).ptr),(*this).get_where());
					(*this).ptr = NULL_REF;

				}
			}

			void flush(btree & b)
			{
				(*this).context = &b;
				(*this).save(b);
				if((*this).ptr != NULL_REF && (*this).get_state() == loaded) {
					update_links(static_cast<_Loaded*>(rget()));

					unload();
				}

			}

			/// test the ptr next and preceding members to establish that they are the same pointer as the ptr member itself
			/// used for analyzing correctness emperically
			void test_btree() const {
				if((*this).has_context() && super::w){
					if(super::ptr){
						stream_address sa = super::w;
						const _Loaded*o = static_cast<const _Loaded*>((*this).get_context()->get_loaded(super::w));
						const _Loaded*r = static_cast<const _Loaded*>(super::ptr);
						if(o && r != o){
							BTREE_PRINT("different pointer to same resource\n");
						}
					}
				}
			}

			/// used to hide things from compiler optimizers which may inline to aggresively
			NO_INLINE void load_this(pointer_proxy* p) {//
				/// this is hidden from the MSVC inline optimizer which seems to be overactive
				(*p) = (*p).get_context()->load(((super*)p)->w);

			}

			/// determines if the page should be loaded loads it and change state to loaded.
			/// The initial state can be any state
			void load() {
				if((*this).ptr == NULL_REF){
					if(super::w){
						/// (*this) = (*this).get_context()->load(super::w);
						/// replaced by
						load_this(this);

					}
				}
			}

			/// const version of above function - un-consting it for eveel hackery
			FORCE_INLINE void load() const
			{
				pointer_proxy* p = const_cast<pointer_proxy*>(this);
				p->load();
			}

			/// lets the pointer go relinquishing any states or members held
			/// and returning the relinquished resource
			_Loaded* release()
			{
				_Loaded*r = static_cast<_Loaded*>(super::ptr);
				super::w = 0;
				super::ptr = 0;

				return r;
			}

			pointer_proxy(const _Loaded * ptr)
			:	super(ptr)
			{
				ref();


			}
			/// constructs using given state and referencing the pointer if its not NULL_REF
			pointer_proxy(const _Loaded * ptr, stream_address w,states s)
            :	super(ptr,w,s)
            {
				ref();
			}

			/// installs pointer with clocksynch for LRU eviction
			pointer_proxy(const base_proxy & left)
            :   super(NULL_REF)
            {
				*this = left;
				clock_synch();
			}

			pointer_proxy(const pointer_proxy & left)
            :   super(NULL_REF)
			{
				*this = left;
				clock_synch();
			}

			pointer_proxy()
            :   super(NULL_REF)
			{

			}

			~pointer_proxy()
			{
				unref();
			}

			pointer_proxy& operator=(const base_proxy &left)
			{
				/// dont back propagate a context
				if(!has_context()){
					set_context(left.get_context());
				}
				if(super::ptr != left.ptr)
				{
					unref();
					super::ptr = left.ptr;
					ref();
				}
				super::w = left.w;
				return *this;
			}

			pointer_proxy& operator=(const pointer_proxy &left)
			{

				(*this) = static_cast<const base_proxy&>(left);
				return *this;
			}
			/// reference counted assingment of raw pointers used usually at creation time
			pointer_proxy& operator=(_Loaded* left)
			{
				if(super::ptr != left)
				{
					unref();
					super::ptr = left;
					ref();

				}
				if(!left){
					(*this).ptr = NULL_REF;
					super::w = 0;
				}
				return *this;
			}

			/// in-equality defined by virtual stream address
			/// to speed up equality tests each referenced
			/// instance has a virtual address preventing any
			/// extra and possibly invalid comparisons
			/// this also means that the virtual address is
			/// allocated instantiation time to ensure uniqueness


			inline bool operator!=(const super& left) const
			{

				return super::not_equal(left);
			}

			/// equality defined by virtual stream address

			inline bool operator==(const super& left) const
			{
				return super::equals(left);
			}

			/// just return the refount

			int get_refs() const {
				if((*this).ptr != NULL_REF){
					return (*this).ptr->refs;
				}
				return 0;
			}

			/// return the raw member ptr without loading
			/// just the non const version

			inline _Loaded * rget()
			{
				return static_cast<_Loaded*>(super::ptr);
			}

			/// return the raw member ptr without loading

			inline const _Loaded * rget() const
			{
				return static_cast<_Loaded*>(super::ptr);
			}

			/// 'natural' ref operator returns the pointer with loading

			_Loaded * operator->()
			{
				load();
				return static_cast<_Loaded*>(super::ptr);
			}

			/// 'natural' deref operator returns the pointer with loading

			_Loaded& operator*()
			{
				load();
				return *rget();
			}

			/// 'natural' ref operator returns the pointer with loading - through const violation

			inline const _Loaded * operator->() const
			{
				load();
				return rget();
			}

			/// 'natural' deref operator returns the pointer with loading - through const violation

			const _Loaded& operator*() const
			{
				load();
				return *rget();
			}

		};

		/// The header structure of each node in-memory. This structure is extended
		/// by interior_node or surface_node.
		/// TODO: convert node into 2 level mini b-tree to optimize CPU cache
		/// effects on large keys AND more importantly increase the page size
		/// without sacrificing random insert and read performance. An increased page size
		/// will result in improved LZ like compression on the storage end. will also
		/// result in a flatter b-tree with less page pointers resulting in less
		/// allocations and fragmentation hopefully
		/// Note: it seems on newer intel architecures the l2 <-> l1 bandwith is
		/// very high so the optimization may not be relevant
		struct node : public node_ref
		{
		private:
			/// Number of key occupants, so number of valid nodes or data
			/// pointers

			/// occupants: since all pages are the same size - its like a block of flats
			/// where the occupant count varies over time the size stays the same

			storage::u16  occupants;
			mutable storage::i32  llb;

		public:
			bool shared;

			void inc_occupants(){
				++occupants;
			}
			void dec_occupants(){
				--occupants;
			}

			/// return the key value pair count

			storage::u16 get_occupants() const {
				return occupants;
			}

			/// set the key value pair count

			void set_occupants(storage::u16 o) {
				occupants = o;
			}

			/// Level in the b-tree, if level == 0 -> surface node

			storage::u16  level;

			/// clock counter for lru a.k.a le roux

			//storage::u32 cc;

			/// Delayed initialisation of constructed node

			inline void initialize(const unsigned short l)
			{
				level = l;
				occupants = 0;
				shared = false;
				llb = 0;

			}
			bool is_modified() const {
				return s != loaded;
			}

			/// True if this is a surface node

			inline bool issurfacenode() const
			{
				return (level == 0);
			}

			/// persisted reference type

			typedef pointer_proxy<node> ptr;

			inline bool key_lessequal(key_compare key_less, const key_type &a, const key_type& b) const
			{
				return !key_less(b, a);
			}

			inline bool key_equal(key_compare key_less, const key_type &a, const key_type& b) const
			{
				return !key_less(a, b) && !key_less(b, a);
			}

			/// multiple search type lower bound template function
			/// performs a lower bound mapping using a couple of techniques simultaneously

			template<typename key_compare, typename key_interpolator >
			inline int find_lower(key_compare key_less,key_interpolator interp, const key_type* keys, const key_type& key, bool do_llb = true) const {
				int o = get_occupants() ;
				if (o  == 0) return 0;

				register unsigned int l = 0, ll=llb, h = o;

				/// multiple search type lower bound function
				if(do_llb){
					/// history optimized linear search
					unsigned int llo = std::min<unsigned int>(o,llb+3);
					while (ll < llo && key_less(keys[ll],key)) ++ll;
					if(ll > llb && ll < llo){
						llb = ll;
						return ll;
					}
				}
				/// truncated binary search
				while(h-l > traits::max_scan) { //		(l < h) {  //(h-l > traits::max_scan) { //
					int m = (l + h) >> 1;
					if (key_lessequal(key_less, key, keys[m])) {
						h = m;
					}else {
						l = m + 1;
					}
				}
				/// residual linear search
				while (l < h && key_less(keys[l],key)) ++l;
				llb = l;

				return l;
			}
		};

		/// a decoded interior node in-memory. Contains keys and
		/// pointers to other nodes but no data. These pointers
		/// may refer to encoded or decoded nodes.

		struct interior_node : public node
		{

			/// Define a related allocator for the interior_node structs.
			typedef typename _Alloc::template rebind<interior_node>::other alloc_type;

			/// persisted reference type providing unobtrusive page management
			typedef pointer_proxy<interior_node> ptr;

			/// Keys of children or data pointers
			key_type        keys[interiorslotmax];

			/// Pointers to sub trees
			typename node::ptr           childid[interiorslotmax+1];

			/// Set variables to initial values
			inline void initialize(const unsigned short l)
			{
				node::initialize(l);
			}

			/// True if the node's slots are full
			inline bool isfull() const
			{
				return (node::get_occupants() == interiorslotmax);
			}

			/// True if few used entries, less than half full
			inline bool isfew() const
			{
				return (node::get_occupants() <= mininteriorslots);
			}

			/// True if node has too few entries
			inline bool isunderflow() const
			{
				return (node::get_occupants() < mininteriorslots);
			}

			template<typename key_compare,typename key_interpolator >
			inline int find_lower(tree_stats& s, key_compare key_less, key_interpolator interp, const key_type& key) const
			{
				return node::find_lower(key_less, interp, keys, key);

			}
			/// decodes a page from given storage and buffer and puts it in slots
			/// the buffer type is expected to be some sort of vector although no strict
			/// checking is performed

			void load(btree * context, storage_type & storage,const buffer_type& buffer)
			{
			    using namespace stx::storage;
				buffer_type::const_iterator reader = buffer.begin();
				(*this).set_occupants (leb128::read_signed(reader));
				(*this).level = leb128::read_signed(reader);

				for(u16 k = 0; k < (*this).get_occupants();++k){
					storage.retrieve(reader, keys[k]);
				}
				for(u16 k = 0; k <= (*this).get_occupants();++k){
					stream_address sa =leb128::read_signed(reader);
					childid[k].set_context(context);
					childid[k].set_where(sa);
				}
			}

			/// encodes a node into storage page and resizes the output buffer accordingly
			/// TODO: check for any failure conditions particularly out of memory
			/// and throw an exception

			void save(storage_type &storage, buffer_type& buffer) const{
				using namespace stx::storage;
				u32 storage_use = leb128::signed_size((*this).get_occupants())+leb128::signed_size((*this).level);
				for(u16 k = 0; k < (*this).get_occupants();++k){
					storage_use += storage.store_size(keys[k]);
					storage_use += leb128::signed_size(childid[k].get_where());
				}
				storage_use += leb128::signed_size(childid[(*this).get_occupants()].get_where());

				buffer.resize(storage_use);
				buffer_type::iterator writer = buffer.begin();

				writer = leb128::write_signed(writer, (*this).get_occupants());
				writer = leb128::write_signed(writer, (*this).level);

				for(u16 k = 0; k < (*this).get_occupants();++k){
					storage.store(writer, keys[k]);
				}
				ptrdiff_t d = writer - buffer.begin();
				for(u16 k = 0; k <= (*this).get_occupants();++k){
					writer = leb128::write_signed(writer, childid[k].get_where());
				}
				d = writer - buffer.begin();
				buffer.resize(d); /// TODO: use swap
			}
		};


		/// Extended structure of a surface node in memory. Contains pairs of keys and
		/// data items. Key and data slots are kept in separate arrays, because the
		/// key array is traversed very often compared to accessing the data items.

		struct surface_node : public node
		{
			/// smart pointer for unobtrusive page storage management
			typedef pointer_proxy<surface_node> ptr;

			/// Define a related allocator for the surface_node structs.
			typedef typename _Alloc::template rebind<surface_node>::other alloc_type;

			/// Double linked list pointers to traverse the leaves
			typename surface_node::ptr      preceding;

			/// Double linked list pointers to traverse the leaves
			typename surface_node::ptr		next;

			/// Keys of children or data pointers
			key_type        keys[surfaceslotmax];

			/// Array of data
			data_type       values[surfaceslotmax];

			/// Is the node sorted or not
			int sorted;

			/// shared counter used when page is shared, only surfaces can be shared
			Poco::AtomicCounter a_refs;

			/// Set variables to initial values
			inline void initialize()
			{
				node::initialize(0);
				sorted = 0;
				(*this).shared = false;
				a_refs = 0;
				preceding = next = NULL_REF;

			}
			void set_shared(){
				(*this).shared = true;
				(*this).a_refs = (*this).refs;
			}
			bool unshare(){
				if((*this).shared && a_refs == 1){
					(*this).shared = false;
					(*this).refs = (*this).a_refs ;
					(*this).a_refs = 0;
					return true;
				}
				return false;
			}
			/// True if the node is sorted

			inline bool issorted(){
				return (sorted > 0);
			}
			/// True if the node's slots are full

			inline bool isfull() const
			{
				return (node::get_occupants() == surfaceslotmax);
			}

			/// True if few used entries, less or equal to half full

			inline bool isfew() const
			{
				return (node::get_occupants() <= minsurfaces);
			}

			/// True if node has too few entries

			inline bool isunderflow() const
			{
				return (node::get_occupants() < minsurfaces);
			}

			template< typename key_compare, typename key_interpolator >
			inline int find_lower(tree_stats& stats, key_compare key_less, key_interpolator interp, const key_type& key) const
			{
				this->sort(stats, key_less);

				return node::find_lower(key_less, interp, keys, key, !(*this).shared);//a_refs < 4

			}

			/// assignment
			surface_node& operator=(const surface_node& right){
				std::copy(right.keys, &right.keys[right.get_occupants()], keys);
				std::copy(right.values, &right.values[right.get_occupants()], values);
				set_occupants(right.get_occupants());
				(*this).level = right.level;
				(*this).sorted = right.sorted;
				(*this).next = right.next;
				(*this).preceding = right.preceding;
			}
			/// sorts the page if it hasn't been

			template< typename key_compare >
			int sort(tree_stats& stats, key_compare key_less) const {
				return ((surface_node*)this)->sort(stats, key_less);
			}
			template< typename key_compare >
			struct key_data{
				key_compare l;
				key_type key;
				data_type value;
				inline bool operator<(const key_data& left) const {
					return l(key, left.key);
				}
			};

			/// sort the keys and move values accordingly

			template< typename key_compare >
			inline bool key_equal(key_compare key_less, const key_type &a, const key_type &b) const
			{
				return !key_less(a, b) && !key_less(b, a);
			}

			/// sort the page if its sort level is less than the occupancy

			template< typename key_compare >
			int sort(tree_stats& stats, key_compare key_less){
				int r = 0;
				if(sorted < node::get_occupants()){
					bool unsorted = false;
					for(int i = 1; i < node::get_occupants(); ++i){
						if(keys[i-1] < keys[i]){
						}else{
							unsorted = true;
							break;
						}
					}
					if(unsorted)
					{
						key_data<key_compare> unsorted[surfaceslotmax];

						for(int i = 0; i < node::get_occupants(); ++i)
						{
							unsorted[i].key = keys[i];
							unsorted[i].value = values[i];
						}

						std::sort(unsorted, unsorted + node::get_occupants() );

						if(btree::allow_duplicates)
						{
							for(int i = 0; i < node::get_occupants(); ++i)
							{

								keys[i] = unsorted[i].key ;
								values[i] = unsorted[i].value ;

							}
						}else
						{
							int i = 0, s = 0, p = 0;
							keys[i] = unsorted[i].key;

							++i;++s;

							for(; i < node::get_occupants(); ++i)
							{
								/// because std::sort is stable the inserted order amongst equals prevails
								if(key_equal(key_less, keys[p], unsorted[i].key))
								{
									values[p] = unsorted[i].value; /// overwrite with the latest one

								}else
								{
									keys[s] = unsorted[i].key ;
									values[s] = unsorted[i].value ;
									p = s;
									s++;
								}

							}
							stats.tree_size += (int)s - (int)node::get_occupants();
							node::set_occupants(s);

						}
					}
					sorted = node::get_occupants();
				}
				return r;
			}
			// insert a key and value pair at
			void insert(int at, const key_type &key,const data_type& value){
				keys[at] = key;
				values[at] = value;
				(*this).inc_occupants();

			}

			/// decodes a page into a exterior node using the provided buffer and storage instance/context
			/// TODO: throw an exception if checks fail

			void load(btree* context, storage_type & storage,const buffer_type& buffer, key_interpolator interp){

			    using namespace stx::storage;

				/// size_t bs = buffer.size();
				buffer_type::const_iterator reader = buffer.begin();
				(*this).set_occupants(leb128::read_signed(reader));
				(*this).level = leb128::read_signed(reader);

				stream_address sa = leb128::read_signed(reader);
				preceding.set_context(context);
				preceding.set_where(sa);
				sa = leb128::read_signed(reader);
				next.set_context(context);
				next.set_where(sa);
				if(interp.encoded(btree::allow_duplicates)){
					interp.decode(reader, keys, (*this).get_occupants());
					
				}else{
					for(u16 k = 0; k < (*this).get_occupants();++k){
						storage.retrieve(reader, keys[k]);
					}
				}

				for(u16 k = 0; k < (*this).get_occupants();++k){
					storage.retrieve(reader, values[k]);
				}

				size_t d = reader - buffer.begin();

				if(d != buffer.size()){
					BTREE_ASSERT(d == buffer.size());
				}
				sorted = (*this).get_occupants();
			}

			/// Encode a stored page from input buffer to node instance
			/// TODO: throw an exception if read iteration extends beyond
			/// buffer size

			void save(key_interpolator interp, storage_type &storage, buffer_type& buffer) const {
				using namespace stx::storage;

				ptrdiff_t storage_use = leb128::signed_size((*this).get_occupants());
				storage_use += leb128::signed_size((*this).level);
				storage_use += leb128::signed_size(preceding.get_where());
				storage_use += leb128::signed_size(next.get_where());
				if(interp.encoded(btree::allow_duplicates)){
					storage_use += interp.encoded_size(keys, (*this).get_occupants());
					for(u16 k = 0; k < (*this).get_occupants();++k){
						storage_use += storage.store_size(values[k]);
					}
				}else{
					for(u16 k = 0; k < (*this).get_occupants();++k){
						storage_use += storage.store_size(keys[k]);
						storage_use += storage.store_size(values[k]);
					}
				}
				buffer.resize(storage_use);
				buffer_type::iterator writer = buffer.begin();
				writer = leb128::write_signed(writer, (*this).get_occupants());
				writer = leb128::write_signed(writer, (*this).level);

				writer = leb128::write_signed(writer, preceding.get_where());
				writer = leb128::write_signed(writer, next.get_where());
				if(interp.encoded(btree::allow_duplicates)){
					interp.encode(writer, keys, (*this).get_occupants());
				}else{
					for(u16 k = 0; k < (*this).get_occupants();++k){
						storage.store(writer, keys[k]);
					}
				}
				for(u16 k = 0; k < (*this).get_occupants();++k){
					storage.store(writer, values[k]);
				}
				ptrdiff_t d = writer - buffer.begin();
				if(d > storage_use){
					BTREE_ASSERT(d <= storage_use);
				}
				buffer.resize(d);

			}
		};

	private:

		// ***Smart pointer based dynamic loading
		/// each node is uniquely identified by a 'virtual'
		/// address even empty ones.
		/// Each address is mapped to physical media
		/// i.e. disk, ram, pigeons etc. Allows the storage
		/// and information structure optimization domains to
		/// be decoupled.

		typedef std::unordered_map<stream_address, node*> _AddressedNodes;
		typedef std::pair<stream_address, stx::storage::version_type> _AddressPair;
		typedef std::map<_AddressPair, node*> _AddressedVersionNodes;
		class _Shared{
		public:
			struct _Addr{
				_Addr() : p(NULL){};
				_AddressedVersionNodes* p;
			};/// resolves decorated name length warn C4503 in ms vc

			_Shared() : nodes(NULL){
			}
			Poco::Mutex & get_named_mutex(){
				static Poco::Mutex m;
				return m;
			}

			_AddressedVersionNodes* get_shared_nodes(std::string name){
				typedef std::unordered_map<std::string, _Addr> _NamedSharedNodes;
				static _NamedSharedNodes named;
				nst::synchronized s(get_named_mutex());
				_AddressedVersionNodes* r =  named[name].p;

				if(r == nullptr){
					r = new _AddressedVersionNodes();
					named[name].p = r;
				}
				return r;
			}
			/**/
			void share(std::string name){
				nodes = get_shared_nodes(name);
			}
			_AddressedVersionNodes* nodes;
		};
		
		/// used to consistently report nodes loaded memory use;
		nst::u64 _nodes_loaded_mem_reported;
		void report_nodes_loaded_mem(){
			remove_btree_totl_used (_nodes_loaded_mem_reported);
			_nodes_loaded_mem_reported = nodes_loaded.size()*32;
			add_btree_totl_used (_nodes_loaded_mem_reported);
		}
		/// provides register for currently loaded/decoded nodes
		/// used to prevent reinstantiation of existing nodes
		/// therefore providing consistency to updates to any
		/// node
		
		_AddressedNodes nodes_loaded;
		_Shared shared;
		///	returns NULL if a node with given storage address is not currently
		/// loaded. otherwise returns the currently loaded node

		const node* get_loaded(stream_address w) const {
			typename _AddressedNodes::const_iterator i = nodes_loaded.find(w);
			if(i!=nodes_loaded.end())
				return (*i).second;
			else
				return NULL_REF;
		}
		_AddressPair make_ap(stream_address w){
			get_storage()->allocate(w, stx::storage::read);
			nst::version_type version = get_storage()->get_allocated_version();
			_AddressPair ap = std::make_pair(w, version);
			get_storage()->complete();
			return ap;
		}
		/// called to direct the saving of a inferior node to storage through instance
		/// management routines in the b-tree

		void save(interior_node* n, stream_address& w){
			if(shared.nodes) return;
			if(get_storage()->is_readonly()){
				return;
			}
			if(n->is_modified()){
				//printf("[B-TREE SAVE] i-node  %lld  ->  %s ver. %lld\n", (long long)w, get_storage()->get_name().c_str(), (long long)get_storage()->get_version());
				using namespace stx::storage;
				buffer_type &buffer = get_storage()->allocate(w, stx::storage::create);
				n->save(*get_storage(), buffer);
				if(lz4){
					inplace_compress_lz4(buffer);
				}else{
					inplace_compress_zlib(buffer);
				}
				n->s = loaded;
				//n->set_modified(false);
				get_storage()->complete();
				stats.changes++;

			}

		}

		/// called to direct the saving of a node to storage through instance
		/// management routines in the b-tree

		void save(typename interior_node::ptr &n){
			stream_address w = n.get_where();
			save(n.rget(),w);
			n.set_where(w);

		}

		/// called to direct the saving of a surface node to storage through instance
		/// management routines in the b-tree

		void save(surface_node* n, stream_address& w){
			if(shared.nodes) return;
			if(get_storage()->is_readonly()){

				return;
			}
			if(n->is_modified()){
				//printf("[B-TREE SAVE] s-node %lld  ->  %s ver. %lld\n", (long long)w, get_storage()->get_name().c_str(), (long long)get_storage()->get_version());
				using namespace stx::storage;
				buffer_type &buffer = get_storage()->allocate(w,stx::storage::create);
				n->sort(stats, key_less);
				n->save(key_interpolator(), *get_storage(), buffer);
				if(lz4){
					inplace_compress_lz4(buffer);
				}else{
					inplace_compress_zlib(buffer);
				}
				n->s = loaded;
				get_storage()->complete();
				stats.changes++;
			}

		}

		/// called to direct the saving of a surface node to storage through instance
		/// management routines in the b-tree

		void save(typename surface_node::ptr &n){
			stream_address w = n.get_where();
			save(n.rget(),w);
			n.set_where(w);

		}

		/// called to route the saving of a interior or exterior node to storage through instance
		/// management routines in the b-tree

		void save(typename node::ptr &n){
			if(get_storage()->is_readonly()){
				return;
			}
			if(n->issurfacenode()){
				typename surface_node::ptr s= n;
				save(s);
				n = s;
			}else{
				typename interior_node::ptr s= n;
				save(s);
				n = s;
			}

		}

		/// called to route the loading of a interior or exterior node to storage through instance
		/// management routines in the b-tree

		typename node::ptr load(stream_address w) {
			using namespace stx::storage;
			//if( nodes_loaded.count(w) != 0 ){
			typename btree::node * nt = nodes_loaded[w];
			if(nt != NULL){
				typename node::ptr ns = nt;
				ns.set_where(w);
				ns.set_context(this);

				return ns;
			}

			nodes_loaded.erase(w);
			synchronized synched(shared.get_named_mutex());

			i32 level = 0;
			/// TODO: NB! double mutex
			buffer_type& dangling_buffer = get_storage()->allocate(w, stx::storage::read);
			if(get_storage()->is_end(dangling_buffer) || dangling_buffer.size() == 0){
				printf("bad allocation at %lld in %s\n",(long long)w, get_storage()->get_name().c_str());
			}
			nst::version_type version = get_storage()->get_allocated_version();
			_AddressPair ap = std::make_pair(w, version);
			if(shared.nodes){
				//printf("[%s] %lld at v. %lld\n", get_storage()->get_name().c_str(), (long long)w, (long long)version);
				nt = (*shared.nodes)[ap];
				if(nt != NULL){
					get_storage()->complete();
					typename node::ptr ns = nt;
					ns.set_where(w);
					ns.set_context(this);
					return ns;
				}
			}
			buffer_type buffer = dangling_buffer ;
			get_storage()->complete();
			if(lz4){
				inplace_decompress_lz4(buffer);
			}else{
				inplace_decompress_zlib(buffer);
			}
			buffer_type::iterator reader = buffer.begin();
			leb128::read_signed(reader);
			level = leb128::read_signed(reader);

			if(level==0){ // its a surface
				typename surface_node::ptr s ;
				s = allocate_surface(w);
				s->load(this, *(get_storage()), buffer, key_interpolator());
				s.set_state(loaded);
				s.set_where(w);
				nodes_loaded[w] = s.rget();
				if(shared.nodes != NULL){
					s.rget()->refs++;
					s.rget()->set_shared();
					(*shared.nodes)[ap] = s.rget();
					/// only share surface nodes
				}
				return s;
			}else{
				typename interior_node::ptr s;
				s = allocate_interior(level,w);
				s->load(this, *(get_storage()), buffer);
				s.set_state(loaded);
				s.set_where(w);
				nodes_loaded[w] = s.rget();

				return s;
			}
			typename interior_node::ptr s;
			return s;

		}

		// *** Template Magic to Convert a pair or key/data types to a value_type

		/// For sets the second pair_type is an empty struct, so the value_type
		/// should only be the first.
		template <typename value_type, typename pair_type>
		struct btree_pair_to_value
		{
			/// Convert a fake pair type to just the first component
			inline value_type operator()(pair_type& p) const
			{
				return p.first;
			}
			/// Convert a fake pair type to just the first component
			inline value_type operator()(const pair_type& p) const
			{
				return p.first;
			}
		};

		/// For maps value_type is the same as the pair_type
		template <typename value_type>
		struct btree_pair_to_value<value_type, value_type>
		{
			/// Identity "convert" a real pair type to just the first component
			inline value_type operator()(pair_type& p) const
			{
				return p;
			}
			/// Identity "convert" a real pair type to just the first component
			inline value_type operator()(const pair_type& p) const
			{
				return p;
			}
		};

		/// Using template specialization select the correct converter used by the
		/// iterators
		typedef btree_pair_to_value<value_type, pair_type> pair_to_value_type;

	public:
		// *** Iterators and Reverse Iterators

		class iterator;
		class const_iterator;
		class reverse_iterator;
		class const_reverse_iterator;

		/// STL-like iterator object for B+ tree items. The iterator points to a
		/// specific slot number in a surface.
		class iterator
		{
		public:
			// *** Types

			/// The key type of the btree. Returned by key().
			typedef typename btree::key_type                key_type;

			/// The data type of the btree. Returned by data().
			typedef typename btree::data_type               data_type;

			/// The value type of the btree. Returned by operator*().
			typedef typename btree::value_type              value_type;

			/// The pair type of the btree.
			typedef typename btree::pair_type               pair_type;

			/// Reference to the value_type. STL required.
			typedef value_type&             reference;

			/// Pointer to the value_type. STL required.
			typedef value_type*             pointer;

			/// STL-magic iterator category
			typedef std::bidirectional_iterator_tag iterator_category;

			/// STL-magic
			typedef ptrdiff_t               difference_type;

			/// Our own type
			typedef iterator                self;

			/// an iterator intializer pair
			typedef std::pair<mini_pointer, unsigned short> initializer_pair;
		private:
			// *** Members

			/// The currently referenced surface node of the tree
			typename btree::surface_node::ptr      currnode;

			/// Current key/data slot referenced
			unsigned short          current_slot;

			/// Friendly to the const_iterator, so it may access the two data items directly.
			friend class const_iterator;

			/// Also friendly to the reverse_iterator, so it may access the two data items directly.
			friend class reverse_iterator;

			/// Also friendly to the const_reverse_iterator, so it may access the two data items directly.
			friend class const_reverse_iterator;

			/// Also friendly to the base btree class, because erase_iter() needs
			/// to read the currnode and current_slot values directly.
			friend class btree<key_type, data_type, storage_type, value_type, key_compare, key_interpolator, traits, allow_duplicates>;

			/// Evil! A temporary value_type to STL-correctly deliver operator* and
			/// operator->
			//  mutable value_type              temp_value;

		public:
			// *** Methods

			/// Default-Constructor of a mutable iterator
			inline iterator()
				: currnode(NULL_REF), current_slot(0)
			{ }

			/// Initializing-Constructor of a mutable iterator
			inline iterator(typename btree::surface_node::ptr l, unsigned short s)
				: currnode(l), current_slot(s)
			{ }
			/// Initializing-Constructor-pair of a mutable iterator
			inline iterator(const initializer_pair& initializer)
				: currnode(initializer.first), current_slot(initializer.second)
			{ }
			/// Copy-constructor from a reverse iterator
			inline iterator(const reverse_iterator &it)
				: currnode(it.currnode), current_slot(it.current_slot)
			{ }

			/// Dereference the iterator, this is not a value_type& because key and
			/// value are not stored together
			/*inline reference operator*() const
			{
				temp_value = pair_to_value_type()( pair_type(currnode->keys[current_slot],
					currnode->values[current_slot]) );
				return temp_value;
			}*/

			/// Dereference the iterator. Do not use this if possible, use key()
			/// and data() instead. The B+ tree does not store key and data
			/// together.
			/*inline pointer operator->() const
			{
				temp_value = pair_to_value_type()( pair_type(currnode->keys[current_slot],
					currnode->values[current_slot]) );
				return &temp_value;
			}*/

			/// Key of the current slot
			inline const key_type& key() const
			{
				return currnode->keys[current_slot];
			}
			inline key_type& key()
			{
				return currnode->keys[current_slot];
			}

			/// Writable reference to the current data object
			inline data_type& data()
			{
				return currnode->values[current_slot];
			}

			/// return true if the iterator is valid
			inline bool loadable() const {
				return currnode.has_context();
			}
			/// iterator construction pair
			inline initializer_pair construction() const {
				mini_pointer m;
				currnode.make_mini(m);
				return std::make_pair(m,current_slot);
			}

			/// iterator constructing pair
			template<typename _MapType>
			inline void from_initializer(_MapType& context, const initializer_pair& init)  {
				currnode.realize(init.first,&context);
				current_slot = init.second;
			}

			inline void from_initializer(const initializer_pair& init)  {
				currnode.realize(init.first,currnode.get_context());
				current_slot = init.second;
			}
			inline iterator& operator= (const initializer_pair& init)  {
				currnode.realize(init.first,currnode.get_context());
				current_slot = init.second;
				return *this;
			}
			/// returns true if the iterator is invalid
			inline bool invalid() const {
				return (!currnode.has_context() || currnode.get_where()==0);
			}

			/// clear context references
			inline void clear() {
				current_slot = 0;
				currnode.set_context(NULL);
			}

			/// returns true if the iterator is valid
			inline bool valid() const {
				return (currnode.has_context() && currnode.get_where()!=0);
			}

			/// ++Prefix advance the iterator to the next slot
			inline self& operator++()
			{
				if (current_slot + 1 < currnode->get_occupants())
				{
					++current_slot;
				}
				else if (currnode->next != NULL_REF)
				{
					currnode = currnode->next;
					current_slot = 0;
				}
				else
				{
					// this is end()
					current_slot = currnode->get_occupants();
				}

				return *this;
			}

			/// mechanism to quickly count the keys between this and another iterator
			/// providing the other iterator is at a higher position

			inline stx::storage::u64 count(const self& to) const
			{

				typename btree::surface_node::ptr last_node = to.currnode;
				typename btree::surface_node::ptr first_node = currnode;
				typename stx::storage::u64 total = 0ull;

				while(first_node != NULL_REF && first_node != last_node)
				{

					total += first_node->get_occupants();
					first_node = first_node->next;
				}

				total += to.current_slot;
				total -= current_slot;

				return total;
			}
			/// Postfix++ advance the iterator to the next slot
			inline self operator++(int)
			{
				self tmp = *this;   // copy ourselves

				if (current_slot + 1 < currnode->get_occupants())
				{
					++current_slot;
				}
				else if (currnode->next != NULL_REF)
				{
					currnode = currnode->next;
					current_slot = 0;
				}
				else
				{
					// this is end()
					current_slot = currnode->get_occupants();
				}

				return tmp;
			}

			/// return the proxy context
			const void * context() const {
				return currnode.get_context();
			}
			/// --Prefix backstep the iterator to the last slot
			inline self& operator--()
			{
				if (current_slot > 0)
				{
					--current_slot;
				}
				else if (currnode->preceding != NULL_REF)
				{
					currnode = currnode->preceding;
					current_slot = currnode->get_occupants() - 1;
				}
				else
				{
					// this is begin()
					current_slot = 0;
				}

				return *this;
			}

			/// Postfix-- backstep the iterator to the last slot
			inline self operator--(int)
			{
				self tmp = *this;   // copy ourselves

				if (current_slot > 0)
				{
					--current_slot;
				}
				else if (currnode->preceding != NULL_REF)
				{
					currnode = currnode->preceding;
					current_slot = currnode->get_occupants() - 1;
				}
				else
				{
					// this is begin()
					current_slot = 0;
				}

				return tmp;
			}

			/// Equality of iterators
			inline bool operator==(const self& x) const
			{
				return (x.currnode == currnode) && (x.current_slot == current_slot);
			}

			/// Inequality of iterators
			inline bool operator!=(const self& x) const
			{
				return (x.currnode != currnode) || (x.current_slot != current_slot);
			}
		};

		/// STL-like read-only iterator object for B+ tree items. The iterator
		/// points to a specific slot number in a surface.
		class const_iterator
		{
		public:
			// *** Types

			/// The key type of the btree. Returned by key().
			typedef typename btree::key_type                key_type;

			/// The data type of the btree. Returned by data().
			typedef typename btree::data_type               data_type;

			/// The value type of the btree. Returned by operator*().
			typedef typename btree::value_type              value_type;

			/// The pair type of the btree.
			typedef typename btree::pair_type               pair_type;

			/// Reference to the value_type. STL required.
			typedef const value_type&               reference;

			/// Pointer to the value_type. STL required.
			typedef const value_type*               pointer;

			/// STL-magic iterator category
			typedef std::bidirectional_iterator_tag         iterator_category;

			/// STL-magic
			typedef ptrdiff_t               difference_type;

			/// Our own type
			typedef const_iterator          self;

		private:
			// *** Members

			/// The currently referenced surface node of the tree
			const typename btree::surface_node::ptr        currnode;

			/// Current key/data slot referenced
			unsigned short                  current_slot;

			/// Friendly to the reverse_const_iterator, so it may access the two data items directly
			friend class const_reverse_iterator;

			/// hamsterveel! A temporary value_type to STL-correctly deliver operator* and
			/// operator->
			mutable value_type              temp_value;

		public:
			// *** Methods

			/// Default-Constructor of a const iterator
			inline const_iterator()
				: currnode(NULL_REF), current_slot(0)
			{ }

			/// Initializing-Constructor of a const iterator
			inline const_iterator(const typename btree::surface_node::ptr l, unsigned short s)
				: currnode(l), current_slot(s)
			{ }

			/// Copy-constructor from a mutable iterator
			inline const_iterator(const iterator &it)
				: currnode(it.currnode), current_slot(it.current_slot)
			{ }

			/// Copy-constructor from a mutable reverse iterator
			inline const_iterator(const reverse_iterator &it)
				: currnode(it.currnode), current_slot(it.current_slot)
			{ }

			/// Copy-constructor from a const reverse iterator
			inline const_iterator(const const_reverse_iterator &it)
				: currnode(it.currnode), current_slot(it.current_slot)
			{ }

			/// Dereference the iterator. Do not use this if possible, use key()
			/// and data() instead. The B+ tree does not stored key and data
			/// together.
			inline reference operator*() const
			{
				temp_value = pair_to_value_type()( pair_type(currnode->keys[current_slot],
					currnode->values[current_slot]) );
				return temp_value;
			}

			/// Dereference the iterator. Do not use this if possible, use key()
			/// and data() instead. The B+ tree does not stored key and data
			/// together.
			inline pointer operator->() const
			{
				temp_value = pair_to_value_type()( pair_type(currnode->keys[current_slot],
					currnode->values[current_slot]) );
				return &temp_value;
			}

			/// Key of the current slot
			inline const key_type& key() const
			{
				return currnode->keys[current_slot];
			}

			/// Read-only reference to the current data object
			inline const data_type& data() const
			{
				return currnode->values[current_slot];
			}

			/// Prefix++ advance the iterator to the next slot
			inline self& operator++()
			{
				if (current_slot + 1 < currnode->get_occupants())
				{
					++current_slot;
				}
				else if (currnode->next != NULL_REF)
				{
					currnode = currnode->next;
					current_slot = 0;
				}
				else
				{
					// this is end()
					current_slot = currnode->get_occupants();
				}

				return *this;
			}

			/// Postfix++ advance the iterator to the next slot
			inline self operator++(int)
			{
				self tmp = *this;   // copy ourselves

				if (current_slot + 1 < currnode->get_occupants())
				{
					++current_slot;
				}
				else if (currnode->next != NULL_REF)
				{
					currnode = currnode->next;
					current_slot = 0;
				}
				else
				{
					// this is end()
					current_slot = currnode->get_occupants();
				}

				return tmp;
			}

			/// Prefix-- backstep the iterator to the last slot
			inline self& operator--()
			{
				if (current_slot > 0)
				{
					--current_slot;
				}
				else if (currnode->preceding != NULL_REF)
				{
					currnode = currnode->preceding;
					current_slot = currnode->get_occupants() - 1;
				}
				else
				{
					// this is begin()
					current_slot = 0;
				}

				return *this;
			}

			/// Postfix-- backstep the iterator to the last slot
			inline self operator--(int)
			{
				self tmp = *this;   // copy ourselves

				if (current_slot > 0)
				{
					--current_slot;
				}
				else if (currnode->preceding != NULL_REF)
				{
					currnode = currnode->preceding;
					current_slot = currnode->get_occupants() - 1;
				}
				else
				{
					// this is begin()
					current_slot = 0;
				}

				return tmp;
			}

			/// Equality of iterators
			inline bool operator==(const self& x) const
			{
				return (x.currnode == currnode) && (x.current_slot == current_slot);
			}

			/// Inequality of iterators
			inline bool operator!=(const self& x) const
			{
				return (x.currnode != currnode) || (x.current_slot != current_slot);
			}
		};

		/// STL-like mutable reverse iterator object for B+ tree items. The
		/// iterator points to a specific slot number in a surface.
		class reverse_iterator
		{
		public:
			// *** Types

			/// The key type of the btree. Returned by key().
			typedef typename btree::key_type                key_type;

			/// The data type of the btree. Returned by data().
			typedef typename btree::data_type               data_type;

			/// The value type of the btree. Returned by operator*().
			typedef typename btree::value_type              value_type;

			/// The pair type of the btree.
			typedef typename btree::pair_type               pair_type;

			/// Reference to the value_type. STL required.
			typedef value_type&             reference;

			/// Pointer to the value_type. STL required.
			typedef value_type*             pointer;

			/// STL-magic iterator category
			typedef std::bidirectional_iterator_tag iterator_category;

			/// STL-magic
			typedef ptrdiff_t               difference_type;

			/// Our own type
			typedef reverse_iterator        self;

		private:
			// *** Members

			/// The currently referenced surface node of the tree
			typename btree::surface_node::ptr      currnode;

			/// One slot past the current key/data slot referenced.
			unsigned short          current_slot;

			/// Friendly to the const_iterator, so it may access the two data items directly
			friend class iterator;

			/// Also friendly to the const_iterator, so it may access the two data items directly
			friend class const_iterator;

			/// Also friendly to the const_iterator, so it may access the two data items directly
			friend class const_reverse_iterator;

			/// Evil! A temporary value_type to STL-correctly deliver operator* and
			/// operator->
			mutable value_type              temp_value;

		public:
			// *** Methods

			/// Default-Constructor of a reverse iterator
			inline reverse_iterator()
				: currnode(NULL_REF), current_slot(0)
			{ }

			/// Initializing-Constructor of a mutable reverse iterator
			inline reverse_iterator(typename btree::surface_node::ptr l, unsigned short s)
				: currnode(l), current_slot(s)
			{ }

			/// Copy-constructor from a mutable iterator
			inline reverse_iterator(const iterator &it)
				: currnode(it.currnode), current_slot(it.current_slot)
			{ }

			/// Dereference the iterator, this is not a value_type& because key and
			/// value are not stored together
			inline reference operator*() const
			{
				BTREE_ASSERT(current_slot > 0);
				temp_value = pair_to_value_type()( pair_type(currnode->keys[current_slot - 1],
					currnode->values[current_slot - 1]) );
				return temp_value;
			}

			/// Dereference the iterator. Do not use this if possible, use key()
			/// and data() instead. The B+ tree does not stored key and data
			/// together.
			inline pointer operator->() const
			{
				BTREE_ASSERT(current_slot > 0);
				temp_value = pair_to_value_type()( pair_type(currnode->keys[current_slot - 1],
					currnode->values[current_slot - 1]) );
				return &temp_value;
			}

			/// Key of the current slot
			inline const key_type& key() const
			{
				BTREE_ASSERT(current_slot > 0);
				return currnode->keys[current_slot - 1];
			}

			/// Writable reference to the current data object
			/// TODO: mark node as changed for update correctness
			inline data_type& data() const
			{
				BTREE_ASSERT(current_slot > 0);
				return currnode->values[current_slot - 1];
			}

			/// Prefix++ advance the iterator to the next slot
			inline self& operator++()
			{
				if (current_slot > 1)
				{
					--current_slot;
				}
				else if (currnode->preceding != NULL_REF)
				{
					currnode = currnode->preceding;
					current_slot = currnode->get_occupants();
				}
				else
				{
					// this is begin() == rend()
					current_slot = 0;
				}

				return *this;
			}

			/// Postfix++ advance the iterator to the next slot
			inline self operator++(int)
			{
				self tmp = *this;   // copy ourselves

				if (current_slot > 1)
				{
					--current_slot;
				}
				else if (currnode->preceding != NULL_REF)
				{
					currnode = currnode->preceding;
					current_slot = currnode->get_occupants();
				}
				else
				{
					// this is begin() == rend()
					current_slot = 0;
				}

				return tmp;
			}

			/// Prefix-- backstep the iterator to the last slot
			inline self& operator--()
			{
				if (current_slot < currnode->get_occupants())
				{
					++current_slot;
				}
				else if (currnode->next != NULL_REF)
				{
					currnode = currnode->next;
					current_slot = 1;
				}
				else
				{
					// this is end() == rbegin()
					current_slot = currnode->get_occupants();
				}

				return *this;
			}

			/// Postfix-- backstep the iterator to the last slot
			inline self operator--(int)
			{
				self tmp = *this;   // copy ourselves

				if (current_slot < currnode->get_occupants())
				{
					++current_slot;
				}
				else if (currnode->next != NULL_REF)
				{
					currnode = currnode->next;
					current_slot = 1;
				}
				else
				{
					// this is end() == rbegin()
					current_slot = currnode->get_occupants();
				}

				return tmp;
			}

			/// Equality of iterators
			inline bool operator==(const self& x) const
			{
				return (x.currnode == currnode) && (x.current_slot == current_slot);
			}

			/// Inequality of iterators
			inline bool operator!=(const self& x) const
			{
				return (x.currnode != currnode) || (x.current_slot != current_slot);
			}
		};

		/// STL-like read-only reverse iterator object for B+ tree items. The
		/// iterator points to a specific slot number in a surface.
		class const_reverse_iterator
		{
		public:
			// *** Types

			/// The key type of the btree. Returned by key().
			typedef typename btree::key_type                key_type;

			/// The data type of the btree. Returned by data().
			typedef typename btree::data_type               data_type;

			/// The value type of the btree. Returned by operator*().
			typedef typename btree::value_type              value_type;

			/// The pair type of the btree.
			typedef typename btree::pair_type               pair_type;

			/// Reference to the value_type. STL required.
			typedef const value_type&               reference;

			/// Pointer to the value_type. STL required.
			typedef const value_type*               pointer;

			/// STL-magic iterator category
			typedef std::bidirectional_iterator_tag         iterator_category;

			/// STL-magic
			typedef ptrdiff_t               difference_type;

			/// Our own type
			typedef const_reverse_iterator  self;

		private:
			// *** Members

			/// The currently referenced surface node of the tree
			const typename btree::surface_node::ptr        currnode;

			/// One slot past the current key/data slot referenced.
			unsigned short                          current_slot;

			/// Friendly to the const_iterator, so it may access the two data items directly.
			friend class reverse_iterator;

			/// Evil! A temporary value_type to STL-correctly deliver operator* and
			/// operator->
			mutable value_type              temp_value;


		public:
			// *** Methods

			/// Default-Constructor of a const reverse iterator
			inline const_reverse_iterator()
				: currnode(NULL_REF), current_slot(0)
			{ }

			/// Initializing-Constructor of a const reverse iterator
			inline const_reverse_iterator(const typename btree::surface_node::ptr l, unsigned short s)
				: currnode(l), current_slot(s)
			{ }

			/// Copy-constructor from a mutable iterator
			inline const_reverse_iterator(const iterator &it)
				: currnode(it.currnode), current_slot(it.current_slot)
			{ }

			/// Copy-constructor from a const iterator
			inline const_reverse_iterator(const const_iterator &it)
				: currnode(it.currnode), current_slot(it.current_slot)
			{ }

			/// Copy-constructor from a mutable reverse iterator
			inline const_reverse_iterator(const reverse_iterator &it)
				: currnode(it.currnode), current_slot(it.current_slot)
			{ }

			/// Dereference the iterator. Do not use this if possible, use key()
			/// and data() instead. The B+ tree does not stored key and data
			/// together.
			inline reference operator*() const
			{
				BTREE_ASSERT(current_slot > 0);
				temp_value = pair_to_value_type()( pair_type(currnode->keys[current_slot - 1],
					currnode->values[current_slot - 1]) );
				return temp_value;
			}

			/// Dereference the iterator. Do not use this if possible, use key()
			/// and data() instead. The B+ tree does not stored key and data
			/// together.
			inline pointer operator->() const
			{
				BTREE_ASSERT(current_slot > 0);
				temp_value = pair_to_value_type()( pair_type(currnode->keys[current_slot - 1],
					currnode->values[current_slot - 1]) );
				return &temp_value;
			}

			/// Key of the current slot
			inline const key_type& key() const
			{
				BTREE_ASSERT(current_slot > 0);
				return currnode->keys[current_slot - 1];
			}

			/// Read-only reference to the current data object
			inline const data_type& data() const
			{
				BTREE_ASSERT(current_slot > 0);
				return currnode->values[current_slot - 1];
			}

			/// Prefix++ advance the iterator to the previous slot
			inline self& operator++()
			{
				if (current_slot > 1)
				{
					--current_slot;
				}
				else if (currnode->preceding != NULL_REF)
				{
					currnode = currnode->preceding;
					current_slot = currnode->get_occupants();
				}
				else
				{
					// this is begin() == rend()
					current_slot = 0;
				}

				return *this;
			}

			/// Postfix++ advance the iterator to the previous slot
			inline self operator++(int)
			{
				self tmp = *this;   // copy ourselves

				if (current_slot > 1)
				{
					--current_slot;
				}
				else if (currnode->preceding != NULL_REF)
				{
					currnode = currnode->preceding;
					current_slot = currnode->get_occupants();
				}
				else
				{
					// this is begin() == rend()
					current_slot = 0;
				}

				return tmp;
			}

			/// Prefix-- backstep the iterator to the next slot
			inline self& operator--()
			{
				if (current_slot < currnode->get_occupants())
				{
					++current_slot;
				}
				else if (currnode->next != NULL_REF)
				{
					currnode = currnode->next;
					current_slot = 1;
				}
				else
				{
					// this is end() == rbegin()
					current_slot = currnode->get_occupants();
				}

				return *this;
			}

			/// Postfix-- backstep the iterator to the next slot
			inline self operator--(int)
			{
				self tmp = *this;   // copy ourselves

				if (current_slot < currnode->get_occupants())
				{
					++current_slot;
				}
				else if (currnode->next != NULL_REF)
				{
					currnode = currnode->next;
					current_slot = 1;
				}
				else
				{
					// this is end() == rbegin()
					current_slot = currnode->get_occupants();
				}

				return tmp;
			}

			/// Equality of iterators
			inline bool operator==(const self& x) const
			{
				return (x.currnode == currnode) && (x.current_slot == current_slot);
			}

			/// Inequality of iterators
			inline bool operator!=(const self& x) const
			{
				return (x.currnode != currnode) || (x.current_slot != current_slot);
			}
		};

	public:
		// *** Small Statistics Structure

		/** A small struct containing basic statistics about the B+ tree. It can be
		* fetched using get_stats(). */
		struct tree_stats
		{
			/// Number of items in the B+ tree
			stx::storage::i64   tree_size;

			/// Number of leaves in the B+ tree
			size_type       leaves;

			/// Number of interior nodes in the B+ tree
			size_type       interiornodes;

			/// Bytes used by nodes in tree
			ptrdiff_t       use;

			/// Bytes used by surface nodes in tree
			ptrdiff_t       surface_use;

			/// Bytes used by interior nodes in tree
			ptrdiff_t       interior_use;

            /// print timer
            ptrdiff_t       pto;

            ptrdiff_t       max_use;

			/// number of pages changed since last flush
			size_t changes;
			/// Base B+ tree parameter: The number of key/data slots in each surface
			static const unsigned short     surfaces = btree_self::surfaceslotmax;

			/// Base B+ tree parameter: The number of key slots in each interior node.
			static const unsigned short     interiorslots = btree_self::interiorslotmax;

			/// Zero initialized
			inline tree_stats()
			:	tree_size(0)
			,	leaves(0)
			,	interiornodes(0)
			,	use(0)
			,	surface_use(0)
			,	interior_use(0)
			,	pto(OS_CLOCK()),max_use(1024*1024*8)
			,	changes(0)
			{
			}
			void report_use(){
                /// when use has reached a critical level flushing may start
                if(OS_CLOCK()-pto > 200){
                    BTREE_PRINT("mem use %.4g mb \n",(double)use/(1024.0*1024.0));
                    pto = OS_CLOCK();
                }
			}
			/// Return the total number of nodes
			inline size_type nodes() const
			{
				return interiornodes + leaves;
			}

			/// Return the average fill of leaves
			inline double avgfill_leaves_D_() const
			{
				return static_cast<double>(tree_size) / (leaves * surfaces);
			}
		};

	private:
		// *** Tree Object Data Members

		/// Pointer to the B+ tree's root node, either surface or interior node
		typename node::ptr       root;

		/// Pointer to first surface in the double linked surface chain
		typename surface_node::ptr   headsurface;

		/// Pointer to last surface in the double linked surface chain
		typename surface_node::ptr   last_surface;

		/// Other small statistics about the B+ tree
		tree_stats  stats;

		/// use lz4 in mem compression otherwize use zlib

		static const bool lz4 = true;

		/// Key comparison object. More comparison functions are generated from
		/// this < relation.
		key_compare key_less;

		key_interpolator key_terp;
		/// Memory allocator.
		allocator_type allocator;

		/// storage
		storage_type *storage;

		void initialize_contexts(){
			_nodes_loaded_mem_reported = 0;
			root.set_context(this);
			headsurface.set_context(this);
			last_surface.set_context(this);
			stx::storage::i64 b = 0;
			if(get_storage()->get_boot_value(b)){

				restore((stx::storage::stream_address)b);
				stx::storage::i64 sa = 0;
				get_storage()->get_boot_value(stats.tree_size,2);
				get_storage()->get_boot_value(sa,3);
				headsurface.set_where((stx::storage::stream_address)sa);
				get_storage()->get_boot_value(sa,4);
				last_surface.set_where((stx::storage::stream_address)sa);
			}

		}
	public:
		// *** Constructors and Destructor

		/// Default constructor initializing an empty B+ tree with the standard key
		/// comparison function
		explicit inline btree(storage_type& storage, const allocator_type &alloc = allocator_type())
		:	root(NULL_REF)
		,	headsurface(NULL_REF)
		,	last_surface(NULL_REF)
		,	allocator(alloc)
		,	storage(&storage)
		{
			++btree_totl_instances;
			initialize_contexts();
		}

		/// Constructor initializing an empty B+ tree with a special key
		/// comparison object
		explicit inline btree
		(	storage_type& storage
		,	const key_compare &kcf
		,	const allocator_type &alloc = allocator_type()
		)
		:	root(NULL_REF)
		,	headsurface(NULL_REF)
		,	last_surface(NULL_REF)
		,	key_less(kcf)
		,	allocator(alloc)
		,	storage(&storage)
		{
			++btree_totl_instances;
			initialize_contexts();
		}

		/// Constructor initializing a B+ tree with the range [first,last)
		template <class InputIterator>
		inline btree
		(	storage_type& storage
		,	InputIterator first
		,	InputIterator last
		,	const allocator_type &alloc = allocator_type()
		)
		:	root(NULL_REF)
		,	headsurface(NULL_REF)
		,	last_surface(NULL_REF)
		,	allocator(alloc)
		,	storage(&storage)
		{
			++btree_totl_instances;
			insert(first, last);
		}

		/// Constructor initializing a B+ tree with the range [first,last) and a
		/// special key comparison object
		template <class InputIterator>
		inline btree
		(	storage_type& storage
		,	InputIterator first
		,	InputIterator last
		,	const key_compare &kcf
		,	const allocator_type &alloc = allocator_type()
		)
		:	root(NULL_REF)
		,	headsurface(NULL_REF)
		,	last_surface(NULL_REF)
		,	key_less(kcf)
		,	allocator(alloc)
		,	storage(&storage)
		{
			++btree_totl_instances;
			insert(first, last);
		}

		/// Frees up all used B+ tree memory pages
		inline ~btree()
		{
			clear();
			--btree_totl_instances;
		}

		/// Fast swapping of two identical B+ tree objects.
		void swap(btree_self& from)
		{
			std::swap(root, from.root);
			std::swap(headsurface, from.headsurface);
			std::swap(last_surface, from.last_surface);
			std::swap(stats, from.stats);
			std::swap(key_less, from.key_less);
			std::swap(allocator, from.allocator);
		}

	public:
		// *** Key and Value Comparison Function Objects

		/// Function class to compare value_type objects. Required by the STL
		class value_compare
		{
		protected:
			/// Key comparison function from the template parameter
			key_compare     key_comp;

			/// Constructor called from btree::value_comp()
			inline value_compare(key_compare kc)
				: key_comp(kc)
			{ }

			/// Friendly to the btree class so it may call the constructor
			friend class btree<key_type, data_type, storage_type, value_type, key_compare, key_interpolator, traits, allow_duplicates>;

		public:
			/// Function call "less"-operator resulting in true if x < y.
			inline bool operator()(const value_type& x, const value_type& y) const
			{
				return key_comp(x.first, y.first);
			}
		};

		/// Constant access to the key comparison object sorting the B+ tree
		inline key_compare key_comp() const
		{
			return key_less;
		}

		/// Constant access to a constructed value_type comparison object. Required
		/// by the STL
		inline value_compare value_comp() const
		{
			return value_compare(key_less);
		}

	private:
		// *** Convenient Key Comparison Functions Generated From key_less

		/// True if a <= b ? constructed from key_less()
		inline bool key_lessequal(const key_type &a, const key_type b) const
		{
			return !key_less(b, a);
		}

		/// True if a > b ? constructed from key_less()
		inline bool key_greater(const key_type &a, const key_type &b) const
		{
			return key_less(b, a);
		}

		/// True if a >= b ? constructed from key_less()
		inline bool key_greaterequal(const key_type &a, const key_type b) const
		{
			return !key_less(a, b);
		}

		/// True if a == b ? constructed from key_less(). This requires the <
		/// relation to be a total order, otherwise the B+ tree cannot be sorted.
		inline bool key_equal(const key_type &a, const key_type &b) const
		{
			return !key_less(a, b) && !key_less(b, a);
		}

		/// saves the boot values - connot in read only mode or when nodes are shared
		void write_boot_values()
		{
			if(shared.nodes) return;
			if(get_storage()->is_readonly()) return;
			if(stats.changes)
			{
				/// avoid letting those pigeons out
				get_storage()->set_boot_value(root.get_where());
				get_storage()->set_boot_value(stats.tree_size,2);
				get_storage()->set_boot_value(headsurface.get_where(),3);
				get_storage()->set_boot_value(last_surface.get_where(),4);
				stats.changes = 0;
			}
		}
	public:
		// *** Allocators

		/// Return the base node allocator provided during construction.
		allocator_type get_allocator() const
		{
			return allocator;
		}

		/// Return the storage interface used for storage operations
		storage_type* get_storage() const
        {
			return storage;
        }

		/// writes all modified pages to storage only
		void flush(){
			stx::storage::u64 flushed = 0;
			if(stats.tree_size){

				save_recursive(flushed,root);
				/*for(typename _AddressedNodes::iterator n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
					typename node::ptr np = (*n).second;
					np.set_where((*n).first);
					np.save(*this);

				}*/
				write_boot_values();

				///BTREE_PRINT("flushing %ld\n",flushed);
			}


		}

		/// release shared surfaces - interior nodes are never shared

		void release_surfaces(){
			nst::u64 flushed = 0;
			this->headsurface.unload();
			this->last_surface.unload();
			flush_recursive_surfaces(flushed,root);
			typedef std::vector<std::pair<stream_address, node*> > _ToDelete;
			_ToDelete td;
			for(typename _AddressedNodes::iterator n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
				td.push_back((*n));
			}
			for(typename _ToDelete::iterator t = td.begin(); t != td.end(); ++t){
				typename node::ptr np = (*t).second;
				np.set_where((*t).first);
				np.flush(*this);
			}
			td.clear();
			if(shared.nodes != NULL){
				nst::synchronized synched(shared.get_named_mutex());
				typename _AddressedVersionNodes::iterator h;
				for(typename _AddressedNodes::iterator n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
					_AddressPair ap = make_ap((*n).first);
					h = (*shared.nodes).find(ap);
					if(h!=(*shared.nodes).end()){
						if((*n).second == (*h).second){
							td.push_back((*n));
						}
					}
				}
				for(typename _ToDelete::iterator t = td.begin(); t != td.end(); ++t){
					nodes_loaded.erase((*t).first);
				}
			}
		}
		/// writes all modified pages to storage and frees all surface nodes
		void reduce_use(){
			flush_buffers(true);
		}
		void flush_buffers(bool reduce){

			/// size_t nodes_before = nodes_loaded.size();
			ptrdiff_t save_tot = btree_totl_used;
			flush();
			nst::u64 flushed = 0;

			this->headsurface.unload();
			this->last_surface.unload();

			flush_recursive(flushed,root);
			if(reduce){
				typedef std::vector<std::pair<stream_address, surface_node*> > _ToDeleteSurface;
				typedef std::vector<std::pair<stream_address, node*> > _ToDelete;
				_ToDelete td;

				for(typename _AddressedNodes::iterator n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
					td.push_back((*n));
				}
				for(typename _ToDelete::iterator t = td.begin(); t != td.end(); ++t){
					typename node::ptr np = (*t).second;
					np.set_where((*t).first);
					np.flush(*this);
				}
				if(shared.nodes != NULL){
					nst::synchronized synched(shared.get_named_mutex());
					typename _AddressedVersionNodes::iterator h;
					_ToDeleteSurface td;
					for(typename _AddressedNodes::iterator n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
						stream_address w = (*n).first;

						_AddressPair ap = make_ap(w);

						h = (*shared.nodes).find(ap);

						if(h!=(*shared.nodes).end()){
							if((*n).second == (*h).second){
								if(!(*n).second->issurfacenode()){
									throw malformed_page_exception();
								}
								surface_node * sn = static_cast<surface_node*>((*n).second);
								if(!sn->shared){
									throw malformed_page_exception();
								}
								if(sn->a_refs <= 0){
									throw malformed_page_exception();
								}
								if(sn->unshare()){

									td.push_back(std::make_pair((*n).first, sn));
									(*shared.nodes).erase(ap);
								}
							}
						}
					}
					for(typename _ToDeleteSurface::iterator t = td.begin(); t != td.end(); ++t){

						typename node::ptr np = (*t).second;
						np.set_where((*t).first);
						--((*t).second->refs); /// remove the reference set by shared
						np.flush(*this);
					}
				}
				if(save_tot > btree_totl_used)
					BTREE_PRINT("total tree use %.8g MiB after flush , nodes removed %lld remaining %lld\n",(double)btree_totl_used/(1024.0*1024.0), (long long)nodes_before - (long long)nodes_loaded.size() , (long long)nodes_loaded.size());	
			}
		}
	private:
		// *** Node Object Allocation and Deallocation Functions

		void change_use(ptrdiff_t used, ptrdiff_t inode, ptrdiff_t snode){
            stats.use += used;
			stats.interior_use += inode;
			stats.surface_use += snode;
			add_btree_totl_used (used);
            /// stats.report_use();
        };

		/// Return an allocator for surface_node objects
		typename surface_node::alloc_type surface_node_allocator()
		{
			return typename surface_node::alloc_type(allocator);
		}

		/// Return an allocator for interior_node objects
		typename interior_node::alloc_type interior_node_allocator()
		{
			return typename interior_node::alloc_type(allocator);
		}

		/// Allocate and initialize a surface node
		typename surface_node::ptr allocate_surface(stream_address w = 0)
		{
			if(nodes_loaded.count(w)){
				BTREE_PRINT("btree::allocate surface loading a new version of %ld\n",w);
			}
			change_use(sizeof(surface_node),0,sizeof(surface_node));

			surface_node* pn = new (surface_node_allocator().allocate(1)) surface_node();
			deleted.erase((ptrdiff_t)pn);
			pn->initialize();
			typename surface_node::ptr n = pn;
			n->next.set_context(this);
			n->preceding.set_context(this);
			n.create(this,w);
			stats.leaves++;
			if(n.get_where()){
				nodes_loaded[n.get_where()] = pn;
			}
			report_nodes_loaded_mem();
			return n;
		}

		/// Allocate and initialize an interior node
		typename interior_node::ptr allocate_interior(unsigned short level, stream_address w = 0)
		{
			change_use(sizeof(interior_node),sizeof(interior_node),0);

			interior_node* pn = new (interior_node_allocator().allocate(1)) interior_node();
			deleted.erase((ptrdiff_t)pn);
			pn->initialize(level);
			typename interior_node::ptr n = pn;
			n.create(this,w);
			stats.interiornodes++;
			if(n.get_where()){
				nodes_loaded[n.get_where()] = pn;
			}
			return n;
		}
		typedef std::set<ptrdiff_t> _Deleted;
		_Deleted deleted;

		bool is_deleted(node_ref* n) const {
			return deleted.count((ptrdiff_t)n) > 0;
		}
		/// Correctly free either interior or surface node, destructs all contained key
		/// and value objects

		inline void free_node( surface_node* n, stream_address w ){

			if(!(n->shared) && n->refs==0){ /// the shared nodes will have +1 references
				save(n,w);
				nodes_loaded.erase(w);
				surface_node *ln = n;
				typename surface_node::alloc_type a(surface_node_allocator());
				/// TODO: adding multithreaded freeing of pages can improve transactional
				/// performance where small ammounts of random page access is prevalent 

				//static mt_free liberator(surface_node_allocator());
				surface_node * removed = ln;
				//liberator.liberate(removed);
				a.destroy(removed);
				a.deallocate(removed, 1);
				change_use(-(ptrdiff_t)sizeof(surface_node),0,-(ptrdiff_t)sizeof(surface_node));
				stats.leaves--;
			}

		}
		inline void free_node(interior_node* n, stream_address w){
			if(n->refs == 0){
				save(n,w);
				nodes_loaded.erase(w);
				typename interior_node::alloc_type a(interior_node_allocator());
				interior_node * removed = n;
				a.destroy(removed);
				a.deallocate(removed, 1);
				change_use(-(ptrdiff_t)sizeof(interior_node),-(ptrdiff_t)sizeof(interior_node),0);
				stats.interiornodes--;
			}

        }
		//inline void free_node( typename surface_node::ptr n){
		//	free_node(n.rget(), n.get_where());
		//}
		inline void free_node( typename node::ptr n ){
			free_node(n.rget(), n.get_where());
		}
		inline void free_node(node* n, stream_address w)
		{
			if(n == NULL){
				return;
			}
			if(nodes_loaded.count(w) > 0){
				if (n->issurfacenode())
				{
					free_node(static_cast<surface_node*>(n),w);
				}
				else
				{
					free_node(static_cast<interior_node* >(n),w);
				}
			}else{
				//printf("node %lld already removed\n", (NS_STORAGE::u64)w);
			}
			report_nodes_loaded_mem();
		}

	public:
		///
		/// share pages with other trees in the named allocation space
		///
		void share(std::string name){
			if(NULL == shared.nodes){
				shared.share(name);
				reload();
			}

		}

		/// unshare pages and release surfaces
		void unshare(){
			if(NULL != shared.nodes){

				shared.nodes = NULL;
				reload();
			}
		}
		// *** Fast Destruction of the B+ Tree
		/// reload the tree when a new transaction starts
		void reload()
		{
			clear();

			initialize_contexts();
		}
		/// Frees all key/data pairs and all nodes of the tree
		void clear()
		{
			if (root!=NULL_REF)
			{
				reduce_use(); /// get rid of unshared nodes and unlink shared nodes

				headsurface = last_surface = NULL_REF;

				clear_recursive(root.rget()); /// unlink interior nodes and any remaining unshared leaf nodes

				root = NULL_REF;

				stats = tree_stats();
				typedef std::vector<std::pair<stream_address,node*> > _Cleared;
				_Cleared cleared;
				for(typename _AddressedNodes::iterator n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
					cleared.push_back((*n));
				}
				for(typename _Cleared::iterator n = cleared.begin(); n != cleared.end(); ++n){
					(*this).free_node((*n).second, (*n).first);/// free re
				}

				nodes_loaded.clear();
				report_nodes_loaded_mem();
			}

			BTREE_ASSERT(stats.tree_size == 0);
		}

		/// the root address will be valid (>0) unless this is an empty tree

		stream_address get_root_address() const {
			return root.get_where();
		}

		/// setting the root address clears any current data and sets the address of the root node

		void set_root_address(stream_address w){
			if(w){
				(*this).clear();

				root = (*this).load(w);
			}
		}

		void restore(stream_address r){
			set_root_address(r);
		}

	private:
		/// Recursively free up nodes

		/// currently this function can be replaced by an iteration through the
		/// iteration through the nodes list
		/// i.e.
		///	for(typename _AddressedNodes::iterator n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n)
		///

		void clear_recursive(node* n)
		{
			if (!n->issurfacenode())
			{
				interior_node* interiornode = static_cast<interior_node*>(n);

				for (unsigned short slot = 0; slot < interiornode->get_occupants() + 1; ++slot)
				{
					if(interiornode->childid[slot].is_loaded())
					{
						stream_address w = interiornode->childid[slot].get_where();
						node * n = interiornode->childid[slot].rget();
						if (n->issurfacenode())
						{
							free_node(n,w);

						}else
						{
							clear_recursive(n);
							free_node(n,w);
						}

					}
				}
			}
		}

		/// releases * nodes
		/// TODO: * is everything - needs to be unmodified only

		void flush_recursive(stx::storage::u64 &flushed, typename node::ptr n)
		{

			if(n == NULL_REF){
				return;
			}


			if (!n->issurfacenode())
			{
				typename interior_node::ptr interiornode = n;

				for (unsigned short slot = 0; slot < interiornode->get_occupants() + 1; ++slot)
				{
					if(interiornode->level == 1)
					{
						interiornode->childid[slot].flush(*this);

					}else
					{
						if (interiornode->childid[slot]->issurfacenode())
						{
							interiornode->childid[slot].flush(*this);

						}else
						{
							flush_recursive(flushed, interiornode->childid[slot]);
							interiornode->childid[slot].flush(*this);

						}
					}
				}
			}
		}
		void flush_recursive_surfaces(stx::storage::u64 &flushed, typename node::ptr n)
		{

			if(n == NULL_REF){
				return;
			}


			if (!n->issurfacenode())
			{
				typename interior_node::ptr interiornode = n;

				for (unsigned short slot = 0; slot < interiornode->get_occupants() + 1; ++slot)
				{
					if(interiornode->level == 1)
					{
						interiornode->childid[slot].flush(*this);

					}else
					{
						if (interiornode->childid[slot]->issurfacenode())
						{
							interiornode->childid[slot].flush(*this);

						}else
						{
							flush_recursive_surfaces(flushed, interiornode->childid[slot]);
						}
					}
				}
			}
		}
		/// this function puts all modified nodes in storage

		void save_recursive(stx::storage::u64 &flushed, typename node::ptr n)
		{

			if(n == NULL_REF)
				return;

			if (!n->issurfacenode())
			{
				typename interior_node::ptr interiornode = n;

				for (unsigned short slot = 0; slot < interiornode->get_occupants() + 1; ++slot)
				{
					if(interiornode->level==1)
					{
						if(interiornode->childid[slot].is_loaded())
						{
							save_recursive(flushed, interiornode->childid[slot]);
						}
					}else
						save_recursive(flushed, interiornode->childid[slot]);

				}
				interiornode.save(*this);
			}else{
				//typename surface_node::ptr c = n;
				n.save(*this);


			}
		}
	public:
		// *** STL Iterator Construction Functions

		/// Constructs a read/data-write iterator that points to the first slot in
		/// the first surface of the B+ tree.
		inline iterator begin()
		{
			return iterator(headsurface, 0);
		}

		/// Constructs a read/data-write iterator that points to the first invalid
		/// slot in the last surface of the B+ tree.
		inline iterator end()
		{
			return iterator(last_surface, last_surface != NULL_REF ? last_surface->get_occupants() : 0);
		}

		/// Constructs a read-only constant iterator that points to the first slot
		/// in the first surface of the B+ tree.
		inline const_iterator begin() const
		{
			return const_iterator(headsurface, 0);
		}

		/// Constructs a read-only constant iterator that points to the first
		/// invalid slot in the last surface of the B+ tree.
		inline const_iterator end() const
		{
			return const_iterator(last_surface, last_surface != NULL_REF ? last_surface->get_occupants() : 0);
		}

		/// Constructs a read/data-write reverse iterator that points to the first
		/// invalid slot in the last surface of the B+ tree. Uses STL magic.
		inline reverse_iterator rbegin()
		{
			return reverse_iterator(end());
		}

		/// Constructs a read/data-write reverse iterator that points to the first
		/// slot in the first surface of the B+ tree. Uses STL magic.
		inline reverse_iterator rend()
		{
			return reverse_iterator(begin());
		}

		/// Constructs a read-only reverse iterator that points to the first
		/// invalid slot in the last surface of the B+ tree. Uses STL magic.
		inline const_reverse_iterator rbegin() const
		{
			return const_reverse_iterator(end());
		}

		/// Constructs a read-only reverse iterator that points to the first slot
		/// in the first surface of the B+ tree. Uses STL magic.
		inline const_reverse_iterator rend() const
		{
			return const_reverse_iterator(begin());
		}

	private:
		// *** B+ Tree Node Binary Search Functions

		/// Searches for the first key in the node n less or equal to key. Uses
		/// binary search with an optional linear self-verification. This is a
		/// template function, because the keys array is located at different
		/// places in surface_node and interior_node.

		inline int find_lower(const surface_node* n, const key_type& key) const
		{
			return n->find_lower<key_compare,key_interpolator>(((btree&)(*this)).stats, key_less, key_terp, key);
		}
		template <typename node_ptr_type>
		inline int find_lower(const node_ptr_type n, const key_type& key) const
		{
			return n->find_lower<key_compare,key_interpolator>(((btree&)(*this)).stats, key_less, key_terp, key);

#ifdef _BT_CHECK
			BTREE_PRINT("btree::find_lower: on " << n << " key " << key << " -> (" << lo << ") " << hi << ", ");

			// verify result using simple linear search
			if (selfverify)
			{
				int i = n->get_occupants() - 1;
				while(i >= 0 && key_lessequal(key, n->keys[i]))
					i--;
				i++;

				BTREE_PRINT("testfind: " << i << std::endl);
				BTREE_ASSERT(i == hi);
			}
			else
			{
				BTREE_PRINT(std::endl);
			}
#endif

		}

		/// Searches for the first key in the node n greater than key. Uses binary
		/// search with an optional linear self-verification. This is a template
		/// function, because the keys array is located at different places in
		/// surface_node and interior_node.
		template <typename node_ptr_type>
		inline int find_upper(const node_ptr_type np, const key_type& key) const
		{
			if (np->get_occupants() == 0) return 0;
			return t_find_upper( np.rget(), np->keys, key);

		}
		inline int t_find_upper(const interior_node * n, const key_type* keys,  const key_type& key) const
		{
			return _find_upper( static_cast<const node*>(n), n->keys, key);
		}
		inline int t_find_upper(const surface_node * n, const key_type* keys,  const key_type& key) const
		{

			n->sort(((btree&)(*this)).stats, key_less);
			return _find_upper( static_cast<const node*>(n), n->keys, key);
		}
		inline int _find_upper(const node * n, const key_type* keys,  const key_type& key) const
		{

			int lo = 0,
				hi = n->get_occupants() - 1;

			while(lo < hi)
			{
				int mid = (lo + hi) >> 1;

				if (key_less(key, keys[mid]))
				{
					hi = mid - 1;
				}
				else
				{
					lo = mid + 1;
				}
			}

			if (hi < 0 || key_lessequal(keys[hi], key))
				hi++;

			BTREE_PRINT("btree::find_upper: on " << n << " key " << key << " -> (" << lo << ") " << hi << ", ");

			// verify result using simple linear search
			if (selfverify)
			{
				int i = n->get_occupants() - 1;
				while(i >= 0 && key_less(key, keys[i]))
					i--;
				i++;

				BTREE_PRINT("btree::find_upper testfind: " << i << std::endl);
				BTREE_ASSERT(i == hi);
			}
			else
			{
				BTREE_PRINT(std::endl);
			}

			return hi;
		}

	public:
		// *** Access Functions to the Item Count

		/// Return the number of key/data pairs in the B+ tree
		inline size_type size() const
		{
			return stats.tree_size;
		}

		/// Returns true if there is at least one key/data pair in the B+ tree
		inline bool empty() const
		{
			return (size() == size_type(0));
		}

		/// Returns the largest possible size of the B+ Tree. This is just a
		/// function required by the STL standard, the B+ Tree can hold more items.
		inline size_type max_size() const
		{
			return size_type(-1);
		}

		/// Return a const reference to the current statistics.
		inline const struct tree_stats& get_stats() const
		{
			return stats;
		}

	public:
		// *** Standard Access Functions Querying the Tree by Descending to a surface

		/// Non-STL function checking whether a key is in the B+ tree. The same as
		/// (find(k) != end()) or (count() != 0).
		bool exists(const key_type &key) const
		{
			typename node::ptr n = root;
			if (!n) return false;

			while(!n->issurfacenode())
			{
				typename interior_node::ptr interior = n;
				int slot = find_lower((interior_node*)interior, key);

				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;

			int slot = find_lower((surface_node*)surface, key);
			return (slot < surface->get_occupants() && key_equal(key, surface->keys[slot]));
		}

		/// Tries to locate a key in the B+ tree and returns an iterator to the
		/// key/data slot if found. If unsuccessful it returns end().
		iterator find(const key_type &key)
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return end();

			while(!n->issurfacenode())
			{

				typename interior_node::ptr interior = n;

				int slot = find_lower(interior, key);
				interior->childid[slot].load();
				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;

			int slot = find_lower(surface.rget(), key);
			return (slot < surface->get_occupants() && key_equal(key, surface->keys[slot]))
				? iterator(surface, slot) : end();
		}

		/// Tries to locate a key in the B+ tree and returns an constant iterator
		/// to the key/data slot if found. If unsuccessful it returns end().
		const_iterator find(const key_type &key) const
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return end();

			while(!n->issurfacenode())
			{
				typename interior_node::ptr interior = n;
				int slot = find_lower(interior, key);
				interior->childid[slot].load();
				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;

			int slot = find_lower(surface, key);
			return (slot < surface->get_occupants() && key_equal(key, surface->keys[slot]))
				? const_iterator(surface, slot) : end();
		}

		/// Tries to locate a key in the B+ tree and returns the number of
		/// identical key entries found.
		size_type count(const key_type &key) const
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return 0;

			while(!n->issurfacenode())
			{
				const typename interior_node::ptr interior = n;
				int slot = find_lower(interior, key);

				n = interior->childid[slot];
			}
			typename surface_node::ptr surface = n;

			int slot = find_lower(surface, key);
			size_type num = 0;

			while (surface != NULL_REF && slot < surface->get_occupants() && key_equal(key, surface->keys[slot]))
			{
				++num;
				if (++slot >= surface->get_occupants())
				{
					surface = surface->next;
					slot = 0;
				}
			}

			return num;
		}

		/// Searches the B+ tree and returns an iterator to the first pair
		/// equal to or greater than key, or end() if all keys are smaller.
		iterator lower_bound(const key_type& key)
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return end();

			while(!n->issurfacenode())
			{
				const typename interior_node::ptr interior = n;
				int slot = find_lower(interior, key);
				interior->childid[slot].load();
				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;

			int slot = find_lower(surface, key);
			return iterator(surface, slot);
		}

		/// Searches the B+ tree and returns a constant iterator to the
		/// first pair equal to or greater than key, or end() if all keys
		/// are smaller.
		const_iterator lower_bound(const key_type& key) const
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return end();

			while(!n->issurfacenode())
			{

				typename interior_node::ptr interior = n;
				int slot = find_lower(interior, key);

				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;

			int slot = find_lower(surface, key);
			return const_iterator(surface, slot);
		}

		/// Searches the B+ tree and returns an iterator to the first pair
		/// greater than key, or end() if all keys are smaller or equal.
		iterator upper_bound(const key_type& key)
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return end();

			while(!n->issurfacenode())
			{
				typename interior_node::ptr interior = n;
				int slot = find_upper(interior, key);

				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;

			int slot = find_upper(surface, key);
			return iterator(surface, slot);
		}

		/// Searches the B+ tree and returns a constant iterator to the
		/// first pair greater than key, or end() if all keys are smaller
		/// or equal.
		const_iterator upper_bound(const key_type& key) const
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return end();

			while(!n->issurfacenode())
			{
				typename interior_node::ptr interior = n;
				int slot = find_upper(interior, key);

				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;

			int slot = find_upper(surface, key);
			return const_iterator(surface, slot);
		}

		/// Searches the B+ tree and returns both lower_bound() and upper_bound().
		inline std::pair<iterator, iterator> equal_range(const key_type& key)
		{
			return std::pair<iterator, iterator>(lower_bound(key), upper_bound(key));
		}

		/// Searches the B+ tree and returns both lower_bound() and upper_bound().
		inline std::pair<const_iterator, const_iterator> equal_range(const key_type& key) const
		{
			return std::pair<const_iterator, const_iterator>(lower_bound(key), upper_bound(key));
		}

	public:
		// *** B+ Tree Object Comparison Functions

		/// Equality relation of B+ trees of the same type. B+ trees of the same
		/// size and equal elements (both key and data) are considered
		/// equal. Beware of the random ordering of duplicate keys.
		inline bool operator==(const btree_self &other) const
		{
			return (size() == other.size()) && std::equal(begin(), end(), other.begin());
		}

		/// Inequality relation. Based on operator==.
		inline bool operator!=(const btree_self &other) const
		{
			return !(*this == other);
		}

		/// Total ordering relation of B+ trees of the same type. It uses
		/// std::lexicographical_compare() for the actual comparison of elements.
		inline bool operator<(const btree_self &other) const
		{
			return std::lexicographical_compare(begin(), end(), other.begin(), other.end());
		}

		/// Greater relation. Based on operator<.
		inline bool operator>(const btree_self &other) const
		{
			return other < *this;
		}

		/// Less-equal relation. Based on operator<.
		inline bool operator<=(const btree_self &other) const
		{
			return !(other < *this);
		}

		/// Greater-equal relation. Based on operator<.
		inline bool operator>=(const btree_self &other) const
		{
			return !(*this < other);
		}

	public:
		/// *** Fast Copy: Assign Operator and Copy Constructors

		/// Assignment operator. All the key/data pairs are copied
		inline btree_self& operator= (const btree_self &other)
		{
			if (this != &other)
			{
				clear();

				key_less = other.key_comp();
				allocator = other.get_allocator();

				if (other.size() != 0)
				{
					stats.leaves = stats.interiornodes = 0;
					if (other.root)
					{
						root = copy_recursive(other.root);
					}
					stats = other.stats;
				}

				if (selfverify) verify();
			}
			return *this;
		}

		/// Copy constructor. The newly initialized B+ tree object will contain a
		/// copy of all key/data pairs.
		inline btree(const btree_self &other)
			: root(NULL_REF), headsurface(NULL_REF), last_surface(NULL_REF),
			stats( other.stats ),
			key_less( other.key_comp() ),
			allocator( other.get_allocator() )
		{
			++btree_totl_instances;
			if (size() > 0)
			{
				stats.leaves = stats.interiornodes = 0;
				if (other.root)
				{
					root = copy_recursive(other.root);
				}
				if (selfverify) verify();
			}
		}

	private:
		/// Recursively copy nodes from another B+ tree object
		struct node* copy_recursive(const node *n)
		{
			if (n->issurfacenode())
			{
				const surface_node *surface = static_cast<const surface_node*>(n);
				surface_node *newsurface = allocate_surface();

				newsurface->set_occupants(surface->get_occupants());
				std::copy(surface->keys, surface->keys + surface->get_occupants(), newsurface->keys);
				std::copy(surface->values, surface->values + surface->get_occupants(), newsurface->values);

				if (headsurface == NULL_REF)
				{
					headsurface = last_surface = newsurface;
					newsurface->preceding = newsurface->next = NULL_REF;
				}
				else
				{
					newsurface->preceding = last_surface;
					last_surface->next = newsurface;
					last_surface = newsurface;
				}

				return newsurface;
			}
			else
			{
				const interior_node *interior = static_cast<const interior_node*>(n);
				interior_node *newinterior = allocate_interior(interior->level);

				newinterior->set_occupants(interior->get_occupants());
				std::copy(interior->keys, interior->keys + interior->get_occupants(), newinterior->keys);

				for (unsigned short slot = 0; slot <= interior->get_occupants(); ++slot)
				{
					newinterior->childid[slot] = copy_recursive(interior->childid[slot]);
				}

				return newinterior;
			}
		}

	public:
		// *** Public Insertion Functions

		/// Attempt to insert a key/data pair into the B+ tree. If the tree does not
		/// allow duplicate keys, then the insert may fail if it is already
		/// present.
		inline std::pair<iterator, bool> insert(const pair_type& x)
		{
			return insert_start(x.first, x.second);
		}

		/// Attempt to insert a key/data pair into the B+ tree. Beware that if
		/// key_type == data_type, then the template iterator insert() is called
		/// instead. If the tree does not allow duplicate keys, then the insert may
		/// fail if it is already present.
		inline std::pair<iterator, bool> insert(const key_type& key, const data_type& data)
		{
			return insert_start(key, data);
		}

		/// Attempt to insert a key/data pair into the B+ tree. This function is the
		/// same as the other insert, however if key_type == data_type then the
		/// non-template function cannot be called. If the tree does not allow
		/// duplicate keys, then the insert may fail if it is already present.
		inline std::pair<iterator, bool> insert2(const key_type& key, const data_type& data)
		{
			return insert_start(key, data);
		}

		/// Attempt to insert a key/data pair into the B+ tree. The iterator hint
		/// is currently ignored by the B+ tree insertion routine.
		inline iterator insert(iterator /* hint */, const pair_type &x)
		{
			return insert_start(x.first, x.second).first;
		}

		/// Attempt to insert a key/data pair into the B+ tree. The iterator hint is
		/// currently ignored by the B+ tree insertion routine.
		inline iterator insert2(iterator /* hint */, const key_type& key, const data_type& data)
		{
			return insert_start(key, data).first;
		}

		/// Attempt to insert the range [first,last) of value_type pairs into the B+
		/// tree. Each key/data pair is inserted individually.
		template <typename InputIterator>
		inline void insert(InputIterator first, InputIterator last)
		{
			InputIterator iter = first;
			while(iter != last)
			{
				insert(*iter);
				++iter;
			}
		}

	private:
		// *** Private Insertion Functions

		/// Start the insertion descent at the current root and handle root
		/// splits. Returns true if the item was inserted
		std::pair<iterator, bool> insert_start(const key_type& key, const data_type& value)
		{
			unshare();

			typename node::ptr newchild ;
			key_type newkey = key_type();

			if (root == NULL_REF)
			{
				last_surface = allocate_surface();
				headsurface = last_surface;
				root = last_surface;
			}

			std::pair<iterator, bool> r = insert_descend(root, key, value, &newkey, newchild);


			if (newchild != NULL_REF)
			{
				typename interior_node::ptr newroot = allocate_interior(root->level + 1);
				newroot->keys[0] = newkey;

				newroot->childid[0] = root;
				newroot->childid[1] = newchild;

				newroot->set_occupants(1);
				newroot.change();
				root.change();
				//newroot->set_modified();
				root = newroot;
			}

			// increment itemcount if the item was inserted
			if (r.second) ++stats.tree_size;

#ifdef BTREE_DEBUG
			if (debug) print(std::cout);
#endif

			if (selfverify)
			{
				verify();
				BTREE_ASSERT(exists(key));
			}

			return r;
		}

		/**
		* @brief Insert an item into the B+ tree.
		*
		* Descend down the nodes to a surface, insert the key/data pair in a free
		* slot. If the node overflows, then it must be split and the new split
		* node inserted into the parent. Unroll / this splitting up to the root.
		*/
		std::pair<iterator, bool> insert_descend(typename node::ptr n,
			const key_type& key, const data_type& value,
			key_type* splitkey, typename node::ptr& splitnode)
		{

			if (!n->issurfacenode())
			{
				typename interior_node::ptr interior = n;

				key_type newkey = key_type();
				typename node::ptr newchild ;

				int at = interior->find_lower(((btree&)(*this)).stats, key_less, key_terp, key);

				BTREE_PRINT("btree::insert_descend into " << interior->childid[at] << std::endl);

				std::pair<iterator, bool> r = insert_descend(interior->childid[at],
					key, value, &newkey, newchild);

				if (newchild != NULL_REF)
				{
					BTREE_PRINT("btree::insert_descend newchild with key " << newkey << " node " << newchild << " at at " << at << std::endl);
					newchild.change();
					if (interior->isfull())
					{
						split_interior_node(interior, splitkey, splitnode, at);

						BTREE_PRINT("btree::insert_descend done split_interior: putslot: " << at << " putkey: " << newkey << " upkey: " << *splitkey << std::endl);

#ifdef BTREE_DEBUG
						if (debug)
						{
							print_node(std::cout, interior);
							print_node(std::cout, splitnode);
						}
#endif

						// check if insert at is in the split sibling node
						BTREE_PRINT("btree::insert_descend switch: " << at << " > " << interior->get_occupants()+1 << std::endl);

						if (at == interior->get_occupants()+1 && interior->get_occupants() < splitnode->get_occupants())
						{
							// special case when the insert at matches the split
							// place between the two nodes, then the insert key
							// becomes the split key.

							BTREE_ASSERT(interior->get_occupants() + 1 < interiorslotmax);

							typename interior_node::ptr splitinterior = splitnode;

							// move the split key and it's datum into the left node
							interior->keys[interior->get_occupants()] = *splitkey;
							interior->childid[interior->get_occupants()+1] = splitinterior->childid[0];
							interior->inc_occupants();
							interior.change();
							// set new split key and move corresponding datum into right node
							splitinterior->childid[0] = newchild;
							splitinterior.change();
							*splitkey = newkey;

							return r;
						}
						else if (at >= interior->get_occupants()+1)
						{
							// in case the insert at is in the newly create split
							// node, we reuse the code below.

							at -= interior->get_occupants()+1;
							interior = splitnode;
							BTREE_PRINT("btree::insert_descend switching to splitted node " << interior << " at " << at <<std::endl);
						}
					}

					// put pointer to child node into correct at

					BTREE_ASSERT(at >= 0 && at <= ref->get_occupants());

					int i = interior->get_occupants();
					interior_node * ref = interior.rget();
					while(i > at)
					{
						ref->keys[i] = ref->keys[i - 1];
						ref->childid[i + 1] = ref->childid[i];
						i--;
					}

					interior->keys[at] = newkey;
					interior->childid[at + 1] = newchild;
					interior.change();
					interior->inc_occupants();
				}

				return r;
			}
			else // n->issurfacenode() == true
			{
				typename surface_node::ptr surface = n;

				if
				(	//surface->sorted==0 &&
					!allow_duplicates &&
					!surface->isfull()
				)
				{	/// this code only runs during fill

					int i = surface->get_occupants() - 1;
					BTREE_ASSERT(i + 1 < surfaceslotmax);
					int at = i + 1;
					surface.change();
					surface->insert(at, key, value);
					surface->sorted = 0;

					return std::pair<iterator, bool>(iterator(surface, at), true);


				}else{

					int at = surface->find_lower(stats, key_less, key_terp, key);

					if (!allow_duplicates && at < surface->get_occupants() && key_equal(key, surface->keys[at]))
					{
						surface.change();
						return std::pair<iterator, bool>(iterator(surface, at), false);
					}

					if (surface->isfull())
					{
						split_surface_node(surface, splitkey, splitnode);

						// check if insert at is in the split sibling node
						if (at >= surface->get_occupants())
						{
							at -= surface->get_occupants();
							surface = splitnode;

						}


					}

					// put data item into correct data at

					int i = surface->get_occupants() - 1;
					BTREE_ASSERT(i + 1 < surfaceslotmax);
					surface.change();
					surface_node * surfactant = surface.rget();
					key_type * keys = surfactant->keys;
					data_type * values = surfactant->values;
					int j = i + 1;
					for(; j > at; )	{
						keys[j] = keys[j-1];
						values[j] = values[j-1];
						--j;
					}
					i = at - 1;

					typename surface_node::ptr splitsurface = splitnode;

					surface->insert(at, key, value);
					surface->sorted = surface->get_occupants();
					if (splitsurface != NULL_REF && surface != splitsurface && at == surface->get_occupants()-1)
					{
						// special case: the node was split, and the insert is at the
						// last at of the old node. then the splitkey must be
						// updated.
						*splitkey = key;
					}

					return std::pair<iterator, bool>(iterator(surface, i + 1), true);
				}
			}
		}

		/// Split up a surface node into two equally-filled sibling leaves. Returns
		/// the new nodes and it's insertion key in the two parameters.
		void split_surface_node(typename surface_node::ptr surface, key_type* _newkey, typename node::ptr &_newsurface)
		{
			BTREE_ASSERT(surface->isfull());

			unsigned int mid = (surface->get_occupants() >> 1);

			BTREE_PRINT("btree::split_surface_node on " << surface << std::endl);

			typename surface_node::ptr newsurface = allocate_surface();

			newsurface->set_occupants(surface->get_occupants() - mid);

			newsurface->next = surface->next;
			if (newsurface->next == NULL_REF)
			{
				BTREE_ASSERT(surface == last_surface);
				last_surface = newsurface;
			}
			else
			{
				newsurface->next->preceding = newsurface;
				newsurface->next.change();
			}
			surface->sort(((btree&)(*this)).stats, key_less);
			for(unsigned int slot = mid; slot < surface->get_occupants(); ++slot)
			{
				unsigned int ni = slot - mid;
				newsurface->keys[ni] = surface->keys[slot];
				newsurface->values[ni] = surface->values[slot];
			}
			/// indicates change for copy on write semantics
			surface.change();
			surface->set_occupants(mid);
			surface->sorted = mid;
			surface->next = newsurface;

			newsurface->preceding = surface;
			newsurface->sorted = newsurface->get_occupants();
			newsurface.change();

			*_newkey = surface->keys[surface->get_occupants()-1];
			_newsurface = newsurface;
		}

		/// Split up an interior node into two equally-filled sibling nodes. Returns
		/// the new nodes and it's insertion key in the two parameters. Requires
		/// the slot of the item will be inserted, so the nodes will be the same
		/// size after the insert.
		void split_interior_node(typename interior_node::ptr interior, key_type* _newkey, typename node::ptr &_newinterior, unsigned int addslot)
		{
			BTREE_ASSERT(interior->isfull());

			unsigned int mid = (interior->get_occupants() >> 1);

			BTREE_PRINT("btree::split_interior: mid " << mid << " addslot " << addslot << std::endl);

			// if the split is uneven and the overflowing item will be put into the
			// larger node, then the smaller split node may underflow
			if (addslot <= mid && mid > interior->get_occupants() - (mid + 1))
				mid--;

			BTREE_PRINT("btree::split_interior: mid " << mid << " addslot " << addslot << std::endl);

			BTREE_PRINT("btree::split_interior_node on " << interior << " into two nodes " << mid << " and " << interior->get_occupants() - (mid + 1) << " sized" << std::endl);

			typename interior_node::ptr newinterior = allocate_interior(interior->level);

			newinterior->set_occupants(interior->get_occupants() - (mid + 1));

			for(unsigned int slot = mid + 1; slot < interior->get_occupants(); ++slot)
			{
				unsigned int ni = slot - (mid + 1);
				newinterior->keys[ni] = interior->keys[slot];
				newinterior->childid[ni] = interior->childid[slot];
			}
			newinterior->childid[newinterior->get_occupants()] = interior->childid[interior->get_occupants()];
			newinterior.change();

			interior.change();
			interior->set_occupants(mid);


			*_newkey = interior->keys[mid];
			_newinterior = newinterior;
		}

	private:
		// *** Support Class Encapsulating Deletion Results

		/// Result flags of recursive deletion.
		enum result_flags_t
		{
			/// Deletion successful and no fix-ups necessary.
			btree_ok = 0,

			/// Deletion not successful because key was not found.
			btree_not_found = 1,

			/// Deletion successful, the last key was updated so parent keyss
			/// need updates.
			btree_update_lastkey = 2,

			/// Deletion successful, children nodes were merged and the parent
			/// needs to remove the empty node.
			btree_fixmerge = 4
		};

		/// B+ tree recursive deletion has much information which is needs to be
		/// passed upward.
		struct result_t
		{
			/// Merged result flags
			result_flags_t  flags;

			/// The key to be updated at the parent's slot
			key_type        lastkey;

			/// Constructor of a result with a specific flag, this can also be used
			/// as for implicit conversion.
			inline result_t(result_flags_t f = btree_ok)
				: flags(f), lastkey()
			{ }

			/// Constructor with a lastkey value.
			inline result_t(result_flags_t f, const key_type &k)
				: flags(f), lastkey(k)
			{ }

			/// Test if this result object has a given flag set.
			inline bool has(result_flags_t f) const
			{
				return (flags & f) != 0;
			}

			/// Merge two results OR-ing the result flags and overwriting lastkeys.
			inline result_t& operator|= (const result_t &other)
			{
				flags = result_flags_t(flags | other.flags);

				// we overwrite existing lastkeys on purpose
				if (other.has(btree_update_lastkey))
					lastkey = other.lastkey;

				return *this;
			}
		};

	public:
		// *** Public Erase Functions

		/// Erases one (the first) of the key/data pairs associated with the given
		/// key.
		bool erase_one(const key_type &key)
		{
			BTREE_PRINT("btree::erase_one(" << key << ") on btree size " << size() << std::endl);

			unshare();

			if (selfverify) verify();

			if (root == NULL_REF) return false;

			result_t result = erase_one_descend(key, root, NULL, NULL, NULL, NULL, NULL, 0);

			if (!result.has(btree_not_found))
				--stats.tree_size;

#ifdef BTREE_DEBUG
			if (debug) print(std::cout);
#endif
			if (selfverify) verify();

			return !result.has(btree_not_found);
		}

		/// Erases all the key/data pairs associated with the given key. This is
		/// implemented using erase_one().
		size_type erase(const key_type &key)
		{
			unshare();

			size_type c = 0;

			while( erase_one(key) )
			{
				++c;
				if (!allow_duplicates) break;
			}

			return c;
		}

		/// Erase the key/data pair referenced by the iterator.
		void erase(iterator iter)
		{
			BTREE_PRINT("btree::erase_iter(" << iter.currnode << "," << iter.current_slot << ") on btree size " << size() << std::endl);

			if (selfverify) verify();

			if (!root) return;

			result_t result = erase_iter_descend(iter, root, NULL, NULL, NULL, NULL, NULL, 0);

			if (!result.has(btree_not_found))
				--stats.tree_size;

#ifdef BTREE_DEBUG
			if (debug) print(std::cout);
#endif
			if (selfverify) verify();
		}

#ifdef BTREE_TODO
		/// Erase all key/data pairs in the range [first,last). This function is
		/// currently not implemented by the B+ Tree.
		void erase(iterator /* first */, iterator /* last */)
		{
			abort();
		}
#endif

	private:
		// *** Private Erase Functions

		/** @brief Erase one (the first) key/data pair in the B+ tree matching key.
		*
		* Descends down the tree in search of key. During the descent the parent,
		* left and right siblings and their parents are computed and passed
		* down. Once the key/data pair is found, it is removed from the surface. If
		* the surface underflows 6 different cases are handled. These cases resolve
		* the underflow by shifting key/data pairs from adjacent sibling nodes,
		* merging two sibling nodes or trimming the tree.
		*/
		result_t erase_one_descend
		(	const key_type& key,
			typename node::ptr curr,
			typename node::ptr left, typename node::ptr right,
			typename interior_node::ptr leftparent, typename interior_node::ptr rightparent,
			typename interior_node::ptr parent, unsigned int parentslot
		)
		{
			if (curr->issurfacenode())
			{
				typename surface_node::ptr surface = curr;
				typename surface_node::ptr leftsurface = left;
				typename surface_node::ptr rightsurface = right;

				int slot = find_lower(surface, key);

				if (slot >= surface->get_occupants() || !key_equal(key, surface->keys[slot]))
				{
					BTREE_PRINT("Could not find key " << key << " to erase." << std::endl);

					return btree_not_found;
				}

				BTREE_PRINT("Found key in surface " << curr << " at slot " << slot << std::endl);
				/// indicates change for copy on write semantics
				surface.change();

				for (int i = slot; i < surface->get_occupants() - 1; i++)
				{
					surface->keys[i] = surface->keys[i + 1];
					surface->values[i] = surface->values[i + 1];
				}
				surface->dec_occupants();

				result_t myres = btree_ok;

				// if the last key of the surface was changed, the parent is notified
				// and updates the key of this surface
				if (slot == surface->get_occupants())
				{
					if (parent != NULL_REF && parentslot < parent->get_occupants())
					{
						BTREE_ASSERT(parent->childid[parentslot] == curr);
						parent->keys[parentslot] = surface->keys[surface->get_occupants() - 1];
					}
					else
					{
						if (surface->get_occupants() >= 1)
						{
							BTREE_PRINT("Scheduling lastkeyupdate: key " << surface->keys[surface->get_occupants() - 1] << std::endl);
							myres |= result_t(btree_update_lastkey, surface->keys[surface->get_occupants() - 1]);
						}
						else
						{
							BTREE_ASSERT(surface == root);
						}
					}
				}

				if (surface->isunderflow() && !(surface == root && surface->get_occupants() >= 1))
				{
					// determine what to do about the underflow

					// case : if this empty surface is the root, then delete all nodes
					// and set root to NULL.
					if (leftsurface == NULL_REF && rightsurface == NULL_REF)
					{
						BTREE_ASSERT(surface == root);
						BTREE_ASSERT(surface->get_occupants() == 0);

						free_node(root.rget(),root.get_where());

						root = surface = NULL_REF;
						headsurface = last_surface = NULL_REF;

						// will be decremented soon by insert_start()
						BTREE_ASSERT(stats.tree_size == 1);
						BTREE_ASSERT(stats.leaves == 0);
						BTREE_ASSERT(stats.interiornodes == 0);

						return btree_ok;
					}
					// case : if both left and right leaves would underflow in case of
					// a shift, then merging is necessary. choose the more local merger
					// with our parent
					else if ( (leftsurface == NULL_REF || leftsurface->isfew()) && (rightsurface == NULL_REF || rightsurface->isfew()) )
					{
						if (leftparent == parent)
							myres |= merge_leaves(leftsurface, surface, leftparent);
						else
							myres |= merge_leaves(surface, rightsurface, rightparent);
					}
					// case : the right surface has extra data, so balance right with current
					else if ( (leftsurface != NULL_REF && leftsurface->isfew()) && (rightsurface != NULL_REF && !rightsurface->isfew()) )
					{
						if (rightparent == parent)
							myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
						else
							myres |= merge_leaves(leftsurface, surface, leftparent);
					}
					// case : the left surface has extra data, so balance left with current
					else if ( (leftsurface != NULL_REF && !leftsurface->isfew()) && (rightsurface != NULL_REF && rightsurface->isfew()) )
					{
						if (leftparent == parent)
							shift_right_surface(leftsurface, surface, leftparent, parentslot - 1);
						else
							myres |= merge_leaves(surface, rightsurface, rightparent);
					}
					// case : both the surface and right leaves have extra data and our
					// parent, choose the surface with more data
					else if (leftparent == rightparent)
					{
						if (leftsurface->get_occupants() <= rightsurface->get_occupants())
							myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
						else
							shift_right_surface(leftsurface, surface, leftparent, parentslot - 1);
					}
					else
					{
						if (leftparent == parent)
							shift_right_surface(leftsurface, surface, leftparent, parentslot - 1);
						else
							myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
					}
				}

				return myres;
			}
			else // !curr->issurfacenode()
			{
				typename interior_node::ptr interior = curr;
				typename interior_node::ptr leftinterior = left;
				typename interior_node::ptr rightinterior = right;

				typename node::ptr myleft, myright;
				typename interior_node::ptr myleftparent, myrightparent;

				int slot = find_lower(interior, key);

				if (slot == 0)
				{
					typename interior_node::ptr l = left;
					myleft = (left == NULL_REF) ? NULL : l->childid[left->get_occupants() - 1];
					myleftparent = leftparent;
				}
				else
				{
					myleft = interior->childid[slot - 1];
					myleftparent = interior;
				}

				if (slot == interior->get_occupants())
				{
					typename interior_node::ptr r = right;
					myright = (right == NULL_REF) ? NULL_REF : r->childid[0];
					myrightparent = rightparent;
				}
				else
				{
					myright = interior->childid[slot + 1];
					myrightparent = interior;
				}

				BTREE_PRINT("erase_one_descend into " << interior->childid[slot] << std::endl);

				result_t result = erase_one_descend(key,
					interior->childid[slot],
					myleft, myright,
					myleftparent, myrightparent,
					interior, slot);

				result_t myres = btree_ok;

				if (result.has(btree_not_found))
				{
					return result;
				}

				if (result.has(btree_update_lastkey))
				{
					if (parent != NULL_REF
						&& parentslot < parent->get_occupants())
					{
						BTREE_PRINT("Fixing lastkeyupdate: key " << result.lastkey << " into parent " << parent << " at parentslot " << parentslot << std::endl);

						BTREE_ASSERT(parent->childid[parentslot] == curr);
						parent->keys[parentslot] = result.lastkey;
					}
					else
					{
						BTREE_PRINT("Forwarding lastkeyupdate: key " << result.lastkey << std::endl);
						myres |= result_t(btree_update_lastkey, result.lastkey);
					}
				}

				if (result.has(btree_fixmerge))
				{
					// either the current node or the next is empty and should be removed
					if (interior->childid[slot]->get_occupants() != 0)
						slot++;

					// this is the child slot invalidated by the merge
					BTREE_ASSERT(interior->childid[slot]->get_occupants() == 0);

					free_node(interior->childid[slot]);

					for(int i = slot; i < interior->get_occupants(); i++)
					{
						interior->keys[i - 1] = interior->keys[i];
						interior->childid[i] = interior->childid[i + 1];
					}
					interior->dec_occupants();

					if (interior->level == 1)
					{
						// fix split key for children leaves
						slot--;
						typename surface_node::ptr child = interior->childid[slot];
						interior->keys[slot] = child->keys[ child->get_occupants()-1 ];
					}
				}

				if (interior->isunderflow() && !(interior == root && interior->get_occupants() >= 1))
				{
					// case: the interior node is the root and has just one child. that child becomes the new root
					if (leftinterior == NULL && rightinterior == NULL)
					{
						BTREE_ASSERT(interior == root);
						BTREE_ASSERT(interior->get_occupants() == 0);

						root = interior->childid[0];

						interior->set_occupants(0);
						free_node(interior);

						return btree_ok;
					}
					// case : if both left and right leaves would underflow in case of
					// a shift, then merging is necessary. choose the more local merger
					// with our parent
					else if ( (leftinterior == NULL || leftinterior->isfew()) && (rightinterior == NULL || rightinterior->isfew()) )
					{
						if (leftparent == parent)
							myres |= merge_interior(leftinterior, interior, leftparent, parentslot - 1);
						else
							myres |= merge_interior(interior, rightinterior, rightparent, parentslot);
					}
					// case : the right surface has extra data, so balance right with current
					else if ( (leftinterior != NULL && leftinterior->isfew()) && (rightinterior != NULL && !rightinterior->isfew()) )
					{
						if (rightparent == parent)
							shift_left_interior(interior, rightinterior, rightparent, parentslot);
						else
							myres |= merge_interior(leftinterior, interior, leftparent, parentslot - 1);
					}
					// case : the left surface has extra data, so balance left with current
					else if ( (leftinterior != NULL && !leftinterior->isfew()) && (rightinterior != NULL && rightinterior->isfew()) )
					{
						if (leftparent == parent)
							shift_right_interior(leftinterior, interior, leftparent, parentslot - 1);
						else
							myres |= merge_interior(interior, rightinterior, rightparent, parentslot);
					}
					// case : both the surface and right leaves have extra data and our
					// parent, choose the surface with more data
					else if (leftparent == rightparent)
					{
						if (leftinterior->get_occupants() <= rightinterior->get_occupants())
							shift_left_interior(interior, rightinterior, rightparent, parentslot);
						else
							shift_right_interior(leftinterior, interior, leftparent, parentslot - 1);
					}
					else
					{
						if (leftparent == parent)
							shift_right_interior(leftinterior, interior, leftparent, parentslot - 1);
						else
							shift_left_interior(interior, rightinterior, rightparent, parentslot);
					}
				}

				return myres;
			}
		}

		/** @brief Erase one key/data pair referenced by an iterator in the B+
		* tree.
		*
		* Descends down the tree in search of an iterator. During the descent the
		* parent, left and right siblings and their parents are computed and
		* passed down. The difficulty is that the iterator contains only a pointer
		* to a surface_node, which means that this function must do a recursive depth
		* first search for that surface node in the subtree containing all pairs of
		* the same key. This subtree can be very large, even the whole tree,
		* though in practice it would not make sense to have so many duplicate
		* keys.
		*
		* Once the referenced key/data pair is found, it is removed from the surface
		* and the same underflow cases are handled as in erase_one_descend.
		*/
		result_t erase_iter_descend(const iterator& iter,
			node *curr,
			node *left, node *right,
			interior_node *leftparent, interior_node *rightparent,
			interior_node *parent, unsigned int parentslot)
		{
			if (curr->issurfacenode())
			{
				surface_node *surface = static_cast<surface_node*>(curr);
				surface_node *leftsurface = static_cast<surface_node*>(left);
				surface_node *rightsurface = static_cast<surface_node*>(right);

				// if this is not the correct surface, get next step in recursive
				// search
				if (surface != iter.currnode)
				{
					return btree_not_found;
				}

				if (iter.current_slot >= surface->get_occupants())
				{
					BTREE_PRINT("Could not find iterator (" << iter.currnode << "," << iter.current_slot << ") to erase. Invalid surface node?" << std::endl);

					return btree_not_found;
				}

				int slot = iter.current_slot;

				BTREE_PRINT("Found iterator in surface " << curr << " at slot " << slot << std::endl);

				for (int i = slot; i < surface->get_occupants() - 1; i++)
				{
					surface->keys[i] = surface->keys[i + 1];
					surface->values[i] = surface->values[i + 1];
				}
				surface->dec_occupants();

				result_t myres = btree_ok;

				// if the last key of the surface was changed, the parent is notified
				// and updates the key of this surface
				if (slot == surface->get_occupants())
				{
					if (parent && parentslot < parent->get_occupants())
					{
						BTREE_ASSERT(parent->childid[parentslot] == curr);
						parent->keys[parentslot] = surface->keys[surface->get_occupants() - 1];
					}
					else
					{
						if (surface->get_occupants() >= 1)
						{
							BTREE_PRINT("Scheduling lastkeyupdate: key " << surface->keys[surface->get_occupants() - 1] << std::endl);
							myres |= result_t(btree_update_lastkey, surface->keys[surface->get_occupants() - 1]);
						}
						else
						{
							BTREE_ASSERT(surface == root);
						}
					}
				}

				if (surface->isunderflow() && !(surface == root && surface->get_occupants() >= 1))
				{
					// determine what to do about the underflow

					// case : if this empty surface is the root, then delete all nodes
					// and set root to NULL.
					if (leftsurface == NULL && rightsurface == NULL)
					{
						BTREE_ASSERT(surface == root);
						BTREE_ASSERT(surface->get_occupants() == 0);

						free_node(root);

						root = surface = NULL;
						headsurface = last_surface = NULL;

						// will be decremented soon by insert_start()
						BTREE_ASSERT(stats.tree_size == 1);
						BTREE_ASSERT(stats.leaves == 0);
						BTREE_ASSERT(stats.interiornodes == 0);

						return btree_ok;
					}
					// case : if both left and right leaves would underflow in case of
					// a shift, then merging is necessary. choose the more local merger
					// with our parent
					else if ( (leftsurface == NULL || leftsurface->isfew()) && (rightsurface == NULL || rightsurface->isfew()) )
					{
						if (leftparent == parent)
							myres |= merge_leaves(leftsurface, surface, leftparent);
						else
							myres |= merge_leaves(surface, rightsurface, rightparent);
					}
					// case : the right surface has extra data, so balance right with current
					else if ( (leftsurface != NULL && leftsurface->isfew()) && (rightsurface != NULL && !rightsurface->isfew()) )
					{
						if (rightparent == parent)
							myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
						else
							myres |= merge_leaves(leftsurface, surface, leftparent);
					}
					// case : the left surface has extra data, so balance left with current
					else if ( (leftsurface != NULL && !leftsurface->isfew()) && (rightsurface != NULL && rightsurface->isfew()) )
					{
						if (leftparent == parent)
							shift_right_surface(leftsurface, surface, leftparent, parentslot - 1);
						else
							myres |= merge_leaves(surface, rightsurface, rightparent);
					}
					// case : both the surface and right leaves have extra data and our
					// parent, choose the surface with more data
					else if (leftparent == rightparent)
					{
						if (leftsurface->get_occupants() <= rightsurface->get_occupants())
							myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
						else
							shift_right_surface(leftsurface, surface, leftparent, parentslot - 1);
					}
					else
					{
						if (leftparent == parent)
							shift_right_surface(leftsurface, surface, leftparent, parentslot - 1);
						else
							myres |= shift_left_surface(surface, rightsurface, rightparent, parentslot);
					}
				}

				return myres;
			}
			else // !curr->issurfacenode()
			{
				interior_node *interior = static_cast<interior_node*>(curr);
				interior_node *leftinterior = static_cast<interior_node*>(left);
				interior_node *rightinterior = static_cast<interior_node*>(right);

				// find first slot below which the searched iterator might be
				// located.

				result_t result;
				int slot = find_lower(interior, iter.key());

				while (slot <= interior->get_occupants())
				{
					node *myleft, *myright;
					interior_node *myleftparent, *myrightparent;

					if (slot == 0)
					{
						myleft = (left == NULL) ? NULL : (static_cast<interior_node*>(left))->childid[left->get_occupants() - 1];
						myleftparent = leftparent;
					}
					else
					{
						myleft = interior->childid[slot - 1];
						myleftparent = interior;
					}

					if (slot == interior->get_occupants())
					{
						myright = (right == NULL) ? NULL : (static_cast<interior_node*>(right))->childid[0];
						myrightparent = rightparent;
					}
					else
					{
						myright = interior->childid[slot + 1];
						myrightparent = interior;
					}

					BTREE_PRINT("erase_iter_descend into " << interior->childid[slot] << std::endl);

					result = erase_iter_descend(iter,
						interior->childid[slot],
						myleft, myright,
						myleftparent, myrightparent,
						interior, slot);

					if (!result.has(btree_not_found))
						break;

					// continue recursive search for surface on next slot

					if (slot < interior->get_occupants() && key_less(interior->keys[slot],iter.key()))
						return btree_not_found;

					++slot;
				}

				if (slot > interior->get_occupants())
					return btree_not_found;

				result_t myres = btree_ok;

				if (result.has(btree_update_lastkey))
				{
					if (parent && parentslot < parent->get_occupants())
					{
						BTREE_PRINT("Fixing lastkeyupdate: key " << result.lastkey << " into parent " << parent << " at parentslot " << parentslot << std::endl);

						BTREE_ASSERT(parent->childid[parentslot] == curr);
						parent->keys[parentslot] = result.lastkey;
					}
					else
					{
						BTREE_PRINT("Forwarding lastkeyupdate: key " << result.lastkey << std::endl);
						myres |= result_t(btree_update_lastkey, result.lastkey);
					}
				}

				if (result.has(btree_fixmerge))
				{
					// either the current node or the next is empty and should be removed
					if (interior->childid[slot]->get_occupants() != 0)
						slot++;

					// this is the child slot invalidated by the merge
					BTREE_ASSERT(interior->childid[slot]->get_occupants() == 0);

					free_node(interior->childid[slot]);

					for(int i = slot; i < interior->get_occupants(); i++)
					{
						interior->keys[i - 1] = interior->keys[i];
						interior->childid[i] = interior->childid[i + 1];
					}
					interior->dec_occupants();

					if (interior->level == 1)
					{
						// fix split key for children leaves
						slot--;
						surface_node *child = static_cast<surface_node*>(interior->childid[slot]);
						interior->keys[slot] = child->keys[ child->get_occupants()-1 ];
					}
				}

				if (interior->isunderflow() && !(interior == root && interior->get_occupants() >= 1))
				{
					// case: the interior node is the root and has just one child. that child becomes the new root
					if (leftinterior == NULL && rightinterior == NULL)
					{
						BTREE_ASSERT(interior == root);
						BTREE_ASSERT(interior->get_occupants() == 0);

						root = interior->childid[0];

						interior->set_occupants(0);
						free_node(interior);

						return btree_ok;
					}
					// case : if both left and right leaves would underflow in case of
					// a shift, then merging is necessary. choose the more local merger
					// with our parent
					else if ( (leftinterior == NULL || leftinterior->isfew()) && (rightinterior == NULL || rightinterior->isfew()) )
					{
						if (leftparent == parent)
							myres |= merge_interior(leftinterior, interior, leftparent, parentslot - 1);
						else
							myres |= merge_interior(interior, rightinterior, rightparent, parentslot);
					}
					// case : the right surface has extra data, so balance right with current
					else if ( (leftinterior != NULL && leftinterior->isfew()) && (rightinterior != NULL && !rightinterior->isfew()) )
					{
						if (rightparent == parent)
							shift_left_interior(interior, rightinterior, rightparent, parentslot);
						else
							myres |= merge_interior(leftinterior, interior, leftparent, parentslot - 1);
					}
					// case : the left surface has extra data, so balance left with current
					else if ( (leftinterior != NULL && !leftinterior->isfew()) && (rightinterior != NULL && rightinterior->isfew()) )
					{
						if (leftparent == parent)
							shift_right_interior(leftinterior, interior, leftparent, parentslot - 1);
						else
							myres |= merge_interior(interior, rightinterior, rightparent, parentslot);
					}
					// case : both the surface and right leaves have extra data and our
					// parent, choose the surface with more data
					else if (leftparent == rightparent)
					{
						if (leftinterior->get_occupants() <= rightinterior->get_occupants())
							shift_left_interior(interior, rightinterior, rightparent, parentslot);
						else
							shift_right_interior(leftinterior, interior, leftparent, parentslot - 1);
					}
					else
					{
						if (leftparent == parent)
							shift_right_interior(leftinterior, interior, leftparent, parentslot - 1);
						else
							shift_left_interior(interior, rightinterior, rightparent, parentslot);
					}
				}

				return myres;
			}
		}

		/// Merge two surface nodes. The function moves all key/data pairs from right
		/// to left and sets right's occupants to zero. The right slot is then
		/// removed by the calling parent node.
		result_t merge_leaves
		(	typename surface_node::ptr left
		,	typename surface_node::ptr right
		,	typename interior_node::ptr parent
		)
		{
			BTREE_PRINT("Merge surface nodes " << left << " and " << right << " with common parent " << parent << "." << std::endl);
			(void)parent;

			BTREE_ASSERT(left->issurfacenode() && right->issurfacenode());
			BTREE_ASSERT(parent->level == 1);

			BTREE_ASSERT(left->get_occupants() + right->get_occupants() < surfaceslotmax);

			/// indicates change for copy on write semantics

			left.change();
			right.change();

			for (unsigned int i = 0; i < right->get_occupants(); i++)
			{
				left->keys[left->get_occupants() + i] = right->keys[i];
				left->values[left->get_occupants() + i] = right->values[i];
			}
			left->set_occupants(left->get_occupants() + right->get_occupants());

			left->next = right->next;
			if (left->next != NULL_REF)
				left->next->preceding = left;
			else
				last_surface = left;

			right->set_occupants(0);

			return btree_fixmerge;
		}

		/// Merge two interior nodes. The function moves all key/childid pairs from
		/// right to left and sets right's occupants to zero. The right slot is then
		/// removed by the calling parent node.
		static result_t merge_interior
		(	typename interior_node::ptr left
		,	typename interior_node::ptr right
		,	typename interior_node::ptr parent
		,	unsigned int parentslot
		)
		{
			BTREE_PRINT("Merge interior nodes " << left << " and " << right << " with common parent " << parent << "." << std::endl);

			BTREE_ASSERT(left->level == right->level);
			BTREE_ASSERT(parent->level == left->level + 1);

			BTREE_ASSERT(parent->childid[parentslot] == left);

			BTREE_ASSERT(left->get_occupants() + right->get_occupants() < interiorslotmax);

			if (selfverify)
			{
				// find the left node's slot in the parent's children
				unsigned int leftslot = 0;
				while(leftslot <= parent->get_occupants() && parent->childid[leftslot] != left)
					++leftslot;

				BTREE_ASSERT(leftslot < parent->get_occupants());
				BTREE_ASSERT(parent->childid[leftslot] == left);
				BTREE_ASSERT(parent->childid[leftslot+1] == right);

				BTREE_ASSERT(parentslot == leftslot);
			}

			// retrieve the decision key from parent
			left->keys[left->get_occupants()] = parent->keys[parentslot];
			left->inc_occupants();

			// copy over keys and children from right
			for (unsigned int i = 0; i < right->get_occupants(); i++)
			{
				left->keys[left->get_occupants() + i] = right->keys[i];
				left->childid[left->get_occupants() + i] = right->childid[i];
			}
			left->set_occupants(left->get_occupants() + right->get_occupants());

			left->childid[left->get_occupants()] = right->childid[right->get_occupants()];

			right->set_occupants(0);

			return btree_fixmerge;
		}

		/// Balance two surface nodes. The function moves key/data pairs from right to
		/// left so that both nodes are equally filled. The parent node is updated
		/// if possible.
		static result_t shift_left_surface
		(	typename surface_node::ptr left
		,	typename surface_node::ptr right
		,	typename interior_node::ptr parent
		,	unsigned int parentslot
		)
		{
			BTREE_ASSERT(left->issurfacenode() && right->issurfacenode());
			BTREE_ASSERT(parent->level == 1);

			BTREE_ASSERT(left->next == right);
			BTREE_ASSERT(left == right->preceding);

			BTREE_ASSERT(left->get_occupants() < right->get_occupants());
			BTREE_ASSERT(parent->childid[parentslot] == left);

			unsigned int shiftnum = (right->get_occupants() - left->get_occupants()) >> 1;

			BTREE_PRINT("Shifting (surface) " << shiftnum << " entries to left " << left << " from right " << right << " with common parent " << parent << "." << std::endl);

			BTREE_ASSERT(left->get_occupants() + shiftnum < surfaceslotmax);
			/// indicates change for copy on write semantics
			left.change();
			right.change();
			// copy the first items from the right node to the last slot in the left node.
			for(unsigned int i = 0; i < shiftnum; i++)
			{
				left->keys[left->get_occupants() + i] = right->keys[i];
				left->values[left->get_occupants() + i] = right->values[i];
			}
			left->set_occupants(left->get_occupants() + shiftnum);

			// shift all slots in the right node to the left

			right->set_occupants(right->get_occupants() - shiftnum);
			for(int i = 0; i < right->get_occupants(); i++)
			{
				right->keys[i] = right->keys[i + shiftnum];
				right->values[i] = right->values[i + shiftnum];
			}

			// fixup parent
			if (parentslot < parent->get_occupants())
			{
				parent.change();
				parent->keys[parentslot] = left->keys[left->get_occupants() - 1];
				return btree_ok;
			}
			else   // the update is further up the tree
			{
				return result_t(btree_update_lastkey, left->keys[left->get_occupants() - 1]);
			}
		}

		/// Balance two interior nodes. The function moves key/data pairs from right
		/// to left so that both nodes are equally filled. The parent node is
		/// updated if possible.
		static void shift_left_interior
		(	typename interior_node::ptr left
		,	typename interior_node::ptr right
		,	typename interior_node::ptr parent
		,	unsigned int parentslot
		)
		{
			BTREE_ASSERT(left->level == right->level);
			BTREE_ASSERT(parent->level == left->level + 1);

			BTREE_ASSERT(left->get_occupants() < right->get_occupants());
			BTREE_ASSERT(parent->childid[parentslot] == left);

			unsigned int shiftnum = (right->get_occupants() - left->get_occupants()) >> 1;

			BTREE_PRINT("Shifting (interior) " << shiftnum << " entries to left " << left << " from right " << right << " with common parent " << parent << "." << std::endl);

			BTREE_ASSERT(left->get_occupants() + shiftnum < interiorslotmax);

			if (selfverify)
			{
				// find the left node's slot in the parent's children and compare to parentslot

				unsigned int leftslot = 0;
				while(leftslot <= parent->get_occupants() && parent->childid[leftslot] != left)
					++leftslot;

				BTREE_ASSERT(leftslot < parent->get_occupants());
				BTREE_ASSERT(parent->childid[leftslot] == left);
				BTREE_ASSERT(parent->childid[leftslot+1] == right);

				BTREE_ASSERT(leftslot == parentslot);
			}

			// copy the parent's decision keys and childid to the first new key on the left
			left->keys[left->get_occupants()] = parent->keys[parentslot];
			left->inc_occupants();

			// copy the other items from the right node to the last slots in the left node.
			for(unsigned int i = 0; i < shiftnum - 1; i++)
			{
				left->keys[left->get_occupants() + i] = right->keys[i];
				left->childid[left->get_occupants() + i] = right->childid[i];
			}
			left->set_occupants(left->get_occupants() + shiftnum - 1);

			// fixup parent
			parent->keys[parentslot] = right->keys[shiftnum - 1];
			// last pointer in left
			left->childid[left->get_occupants()] = right->childid[shiftnum - 1];

			// shift all slots in the right node

			right->set_occupants(right->get_occupants() - shiftnum);
			for(int i = 0; i < right->get_occupants(); i++)
			{
				right->keys[i] = right->keys[i + shiftnum];
				right->childid[i] = right->childid[i + shiftnum];
			}
			right->childid[right->get_occupants()] = right->childid[right->get_occupants() + shiftnum];
		}

		/// Balance two surface nodes. The function moves key/data pairs from left to
		/// right so that both nodes are equally filled. The parent node is updated
		/// if possible.
		static void shift_right_surface
		(	typename surface_node::ptr left
		,	typename surface_node::ptr right
		,	typename interior_node::ptr parent
		,	unsigned int parentslot
		)
		{
			BTREE_ASSERT(left->issurfacenode() && right->issurfacenode());
			BTREE_ASSERT(parent->level == 1);

			BTREE_ASSERT(left->next == right);
			BTREE_ASSERT(left == right->preceding);
			BTREE_ASSERT(parent->childid[parentslot] == left);

			BTREE_ASSERT(left->get_occupants() > right->get_occupants());

			unsigned int shiftnum = (left->get_occupants() - right->get_occupants()) >> 1;

			BTREE_PRINT("Shifting (surface) " << shiftnum << " entries to right " << right << " from left " << left << " with common parent " << parent << "." << std::endl);

			if (selfverify)
			{
				// find the left node's slot in the parent's children
				unsigned int leftslot = 0;
				while(leftslot <= parent->get_occupants() && parent->childid[leftslot] != left)
					++leftslot;

				BTREE_ASSERT(leftslot < parent->get_occupants());
				BTREE_ASSERT(parent->childid[leftslot] == left);
				BTREE_ASSERT(parent->childid[leftslot+1] == right);

				BTREE_ASSERT(leftslot == parentslot);
			}
			// indicates pages will be changed for surface only copy on write semantics
			right.change();
			left.change();
			// shift all slots in the right node

			BTREE_ASSERT(right->get_occupants() + shiftnum < surfaceslotmax);

			for(int i = right->get_occupants()-1; i >= 0; i--)
			{
				right->keys[i + shiftnum] = right->keys[i];
				right->values[i + shiftnum] = right->values[i];
			}
			right->set_occupants(right->get_occupants() + shiftnum);

			// copy the last items from the left node to the first slot in the right node.
			for(unsigned int i = 0; i < shiftnum; i++)
			{
				right->keys[i] = left->keys[left->get_occupants() - shiftnum + i];
				right->values[i] = left->values[left->get_occupants() - shiftnum + i];
			}
			left->set_occupants(left->get_occupants() - shiftnum);

			parent->keys[parentslot] = left->keys[left->get_occupants()-1];
		}

		/// Balance two interior nodes. The function moves key/data pairs from left to
		/// right so that both nodes are equally filled. The parent node is updated
		/// if possible.
		static void shift_right_interior
		(	typename interior_node::ptr left
		,	typename interior_node::ptr right
		,	typename interior_node::ptr parent
		,	unsigned int parentslot
		)
		{
			BTREE_ASSERT(left->level == right->level);
			BTREE_ASSERT(parent->level == left->level + 1);

			BTREE_ASSERT(left->get_occupants() > right->get_occupants());
			BTREE_ASSERT(parent->childid[parentslot] == left);

			unsigned int shiftnum = (left->get_occupants() - right->get_occupants()) >> 1;

			BTREE_PRINT("Shifting (surface) " << shiftnum << " entries to right " << right << " from left " << left << " with common parent " << parent << "." << std::endl);

			if (selfverify)
			{
				// find the left node's slot in the parent's children
				unsigned int leftslot = 0;
				while(leftslot <= parent->get_occupants() && parent->childid[leftslot] != left)
					++leftslot;

				BTREE_ASSERT(leftslot < parent->get_occupants());
				BTREE_ASSERT(parent->childid[leftslot] == left);
				BTREE_ASSERT(parent->childid[leftslot+1] == right);

				BTREE_ASSERT(leftslot == parentslot);
			}

			// shift all slots in the right node

			BTREE_ASSERT(right->get_occupants() + shiftnum < interiorslotmax);

			right->childid[right->get_occupants() + shiftnum] = right->childid[right->get_occupants()];
			for(int i = right->get_occupants()-1; i >= 0; i--)
			{
				right->keys[i + shiftnum] = right->keys[i];
				right->childid[i + shiftnum] = right->childid[i];
			}
			right->set_occupants(right->get_occupants() + shiftnum);

			// copy the parent's decision keys and childid to the last new key on the right
			right->keys[shiftnum - 1] = parent->keys[parentslot];
			right->childid[shiftnum - 1] = left->childid[left->get_occupants()];

			// copy the remaining last items from the left node to the first slot in the right node.
			for(unsigned int i = 0; i < shiftnum - 1; i++)
			{
				right->keys[i] = left->keys[left->get_occupants() - shiftnum + i + 1];
				right->childid[i] = left->childid[left->get_occupants() - shiftnum + i + 1];
			}

			// copy the first to-be-removed key from the left node to the parent's decision slot
			parent->keys[parentslot] = left->keys[left->get_occupants() - shiftnum];

			left->set_occupants(left->get_occupants() - shiftnum);
		}

#ifdef BTREE_DEBUG
	public:
		// *** Debug Printing

		/// Print out the B+ tree structure with keys onto the given ostream. This
		/// function requires that the header is compiled with BTREE_DEBUG and that
		/// key_type is printable via std::ostream.
		void print(std::ostream &os) const
		{
			if (root)
			{
				print_node(os, root, 0, true);
			}
		}

		/// Print out only the leaves via the double linked list.
		void print_leaves(std::ostream &os) const
		{
			os << "leaves:" << std::endl;

			const surface_node *n = headsurface;

			while(n)
			{
				os << "  " << n << std::endl;

				n = n->next;
			}
		}

	private:

		/// Recursively descend down the tree and print out nodes.
		static void print_node(std::ostream &os, const node* node, unsigned int depth=0, bool recursive=false)
		{
			for(unsigned int i = 0; i < depth; i++) os << "  ";

			os << "node " << node << " level " << node->level << " occupants " << node->get_occupants() << std::endl;

			if (node->issurfacenode())
			{
				const surface_node *surfacenode = static_cast<const surface_node*>(node);

				for(unsigned int i = 0; i < depth; i++) os << "  ";
				os << "  surface prev " << surfacenode->preceding << " next " << surfacenode->next << std::endl;

				for(unsigned int i = 0; i < depth; i++) os << "  ";

				for (unsigned int slot = 0; slot < surfacenode->get_occupants(); ++slot)
				{
					os << surfacenode->keys[slot] << "  "; // << "(data: " << surfacenode->values[slot] << ") ";
				}
				os << std::endl;
			}
			else
			{
				const interior_node *interiornode = static_cast<const interior_node*>(node);

				for(unsigned int i = 0; i < depth; i++) os << "  ";

				for (unsigned short slot = 0; slot < interiornode->get_occupants(); ++slot)
				{
					os << "(" << interiornode->childid[slot] << ") " << interiornode->keys[slot] << " ";
				}
				os << "(" << interiornode->childid[interiornode->get_occupants()] << ")" << std::endl;

				if (recursive)
				{
					for (unsigned short slot = 0; slot < interiornode->get_occupants() + 1; ++slot)
					{
						print_node(os, interiornode->childid[slot], depth + 1, recursive);
					}
				}
			}
		}
#endif
	public:
		// *** Loading the B+ Tree root from some kind of slow storage
		void __D_load()
		{
			/// get rid of old data
			clear();
		}
	public:
		// *** Verification of B+ Tree Invariants

		/// Run a thorough verification of all B+ tree invariants. The program
		/// aborts via assert() if something is wrong.
		void verify() const
		{
			key_type minkey, maxkey;
			tree_stats vstats;

			if (root!=NULL_REF)
			{
				verify_node(root, &minkey, &maxkey, vstats);

				assert( vstats.tree_size == stats.tree_size );
				assert( vstats.leaves == stats.leaves );
				assert( vstats.interiornodes == stats.interiornodes );

				verify_surfacelinks();
			}
		}
        void set_max_use(ptrdiff_t max_use){
			stats.max_use = std::max<ptrdiff_t>(max_use, 1024*1024*64);
        }
	private:

		/// Recursively descend down the tree and verify each node
		void verify_node(const typename node::ptr n, key_type* minkey, key_type* maxkey, tree_stats &vstats) const
		{
			BTREE_PRINT("verifynode " << n << std::endl);

			if (n->issurfacenode())
			{
				const typename surface_node::ptr surface = n;

				assert( surface == root || !surface->isunderflow() );
				assert( surface->get_occupants() > 0 );
				// stats.tree_size +=
				surface->sort(((btree&)(*this)).stats, key_less);

				for(unsigned short slot = 0; slot < surface->get_occupants() - 1; ++slot)
				{
					assert(key_lessequal(surface->keys[slot], surface->keys[slot + 1]));
				}

				*minkey = surface->keys[0];
				*maxkey = surface->keys[surface->get_occupants() - 1];

				vstats.leaves++;
				vstats.tree_size += surface->get_occupants();
			}
			else // !n->issurfacenode()
			{
				const typename interior_node::ptr interior = n;
				vstats.interiornodes++;

				assert( interior == root || !interior->isunderflow() );
				assert( interior->get_occupants() > 0 );

				for(unsigned short slot = 0; slot < interior->get_occupants() - 1; ++slot)
				{
					assert(key_lessequal(interior->keys[slot], interior->keys[slot + 1]));
				}

				for(unsigned short slot = 0; slot <= interior->get_occupants(); ++slot)
				{
					const typename node::ptr subnode = interior->childid[slot];
					key_type subminkey = key_type();
					key_type submaxkey = key_type();

					assert(subnode->level + 1 == interior->level);
					verify_node(subnode, &subminkey, &submaxkey, vstats);

					BTREE_PRINT("verify subnode " << subnode << ": " << subminkey << " - " << submaxkey << std::endl);

					if (slot == 0)
						*minkey = subminkey;
					else
						assert(key_greaterequal(subminkey, interior->keys[slot-1]));

					if (slot == interior->get_occupants())
						*maxkey = submaxkey;
					else
						assert(key_equal(interior->keys[slot], submaxkey));

					if (interior->level == 1 && slot < interior->get_occupants())
					{
						// children are leaves and must be linked together in the
						// correct order
						const typename surface_node::ptr surfacea = interior->childid[slot];
						const typename surface_node::ptr surfaceb = interior->childid[slot + 1];

						assert(surfacea->next == surfaceb);
						assert(surfacea == surfaceb->preceding);
						(void)surfacea;
						(void)surfaceb;
					}
					if (interior->level == 2 && slot < interior->get_occupants())
					{
						// verify surface links between the adjacent interior nodes
						const typename interior_node::ptr parenta = interior->childid[slot];
						const typename interior_node::ptr parentb = interior->childid[slot+1];

						const typename surface_node::ptr surfacea = parenta->childid[parenta->get_occupants()];
						const typename surface_node::ptr surfaceb = parentb->childid[0];

						assert(surfacea->next == surfaceb);
						assert(surfacea == surfaceb->preceding);
						(void)surfacea;
						(void)surfaceb;
					}
				}
			}
		}

		/// Verify the double linked list of leaves.
		void verify_surfacelinks() const
		{
			typename surface_node::ptr n = headsurface;

			assert(n->level == 0);
			assert(n==NULL_REF || n->preceding == NULL_REF);

			unsigned int testcount = 0;

			while(n!=NULL_REF)
			{
				assert(n->level == 0);
				assert(n->get_occupants() > 0);

				for(unsigned short slot = 0; slot < n->get_occupants() - 1; ++slot)
				{
					assert(key_lessequal(n->keys[slot], n->keys[slot + 1]));
				}

				testcount += n->get_occupants();

				if (n->next!=NULL_REF)
				{
					assert(key_lessequal(n->keys[n->get_occupants()-1], n->next->keys[0]));

					assert(n == n->next->preceding);
				}
				else
				{
					assert(last_surface == n);
				}

				n = n->next;
			}

			assert(testcount == size());
		}

	private:




	};
} // namespace stx

#endif // _STX_BTREE_H_
