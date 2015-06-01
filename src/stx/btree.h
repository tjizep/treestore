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
#ifdef _MSC_VER
#include <Windows.h>
#include <sparsehash/internal/sparseconfig.h>
#endif

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
#include <stx/storage/pool.h>

#include <Poco/AtomicCounter.h>
#include <Poco/Mutex.h>
#include <Poco/Thread.h>
#include <Poco/ThreadPool.h>
#include <Poco/TaskManager.h>
#include <Poco/Task.h>
#include <Poco/Timestamp.h>
#include <stx/storage/pool.h>
#include <rabbit/unordered_map>

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
#define BTREE_ASSERT(x)         do {  } while(0)

#endif

/// std::max function does not work for initializing static const limiters
#ifndef max_const
#define max_const(a,b)            (((a) > (b)) ? (a) : (b))
#endif

// * define an os related realtime clock

#define OS_CLOCK Poco::Timestamp().epochMicroseconds

/// define the memory allocation strategy
#define AGGRESSIVE_MEM false

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
extern stx::storage::allocation::pool allocation_pool;
extern ptrdiff_t btree_totl_used ;
extern ptrdiff_t btree_totl_instances ;
extern void add_btree_totl_used(ptrdiff_t added);
extern void remove_btree_totl_used(ptrdiff_t added);
class malformed_page_exception : public std::exception{
public:
    malformed_page_exception() throw(){};
};

struct _D_has_attached_page_data {
    template <typename T, void (T::*)() = &T::_D_attached_page_data>
    struct get {};
};

template <typename T>
struct attached_flag{

	bool attached_page_data(){
		return false;
	};
};
template <typename T>
struct has_typedef_attached_values {
    // Types "yes" and "no" are guaranteed to have different sizes,
    // specifically sizeof(yes) == 1 and sizeof(no) == 2.
    typedef char yes[1];
    typedef char no[2];

    template <typename C>
    static yes& test(typename C::attached_values*);

    template <typename>
    static no& test(...);

    // If the "sizeof" of the result of calling test<T>(0) would be equal to sizeof(yes),
    // the first overload worked and T has a nested type named foobar.
    static const bool value = sizeof(test<T>(nullptr)) == sizeof(yes);
};
namespace nst = stx::storage;
namespace stx
{
	class idle_processor{
	public:
		virtual void idle_time() = 0;
	};
	typedef std::vector<idle_processor*, sta::tracker<idle_processor*> > _idle_processors;
	extern _idle_processors idle_processors;
	extern bool memory_low_state;
	static void process_idle_times(){
		for(_idle_processors::iterator i = idle_processors.begin(); i != idle_processors.end(); ++i){
			(*i)->idle_time();
		}
	}
	/// mini pointer for portable iterator references
	struct mini_pointer{

		storage::stream_address w;
	};

	/// an iterator intializer pair
	typedef std::pair<mini_pointer, unsigned short> initializer_pair;

	template<typename _KeyType>
	struct interpolator{

		inline bool encoded( bool ) const {
			return false;
		}
		inline bool encoded_values( bool ) const {
			return false;
		}
		inline bool attached_values( ) const {
			return false;
		}
		inline void encode(nst::buffer_type::iterator&, const _KeyType*, nst::u16) const {

		}

		inline void decode(const nst::buffer_type&, nst::buffer_type::const_iterator&, _KeyType*, nst::u16) const {
		}
		template<typename _AnyValue>
		inline void decode_values(const nst::buffer_type&, nst::buffer_type::const_iterator&, _AnyValue*, nst::u16) const {
		}
		template<typename _AnyValue>
		inline void encode_values(nst::buffer_type&, nst::buffer_type::iterator&, const _AnyValue*, nst::u16) const {
		}
		template<typename _AnyValue>
		inline size_t encoded_values_size(const _AnyValue*, nst::u16) const {
			return 0;
		}
		int encoded_size(const _KeyType*, nst::u16){
			return 0;
		}
		/// interpolation functions can be an something like linear interpolation
		inline bool can(const _KeyType &, const _KeyType &, int ) const {
			return false;
		}

		unsigned int interpolate(const _KeyType &k, const _KeyType &first , const _KeyType &last, int size) const{
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
			max_scan = 0,
			interior_mul = 1,
			keys_per_page = 128,
			caches_per_page = 16,
			max_release = 8,
			version_reload
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

		typedef _Key		key_type;

		/// Number of slots in each surface of the tree. Estimated so that each node
		/// has a size of about btree_traits::bytes_per_page bytes.
		static const int    surfaces = btree_traits::keys_per_page; //max_const( 8l, btree_traits::bytes_per_page / (sizeof(key_proxy)) );

		/// Number of cached keys
		static const int	caches = btree_traits::caches_per_page;

		/// Number of slots in each interior node of the tree. Estimated so that each node
		/// has a size of about btree_traits::bytes_per_page bytes.
		static const int    interiorslots = btree_traits::keys_per_page; //max_const( 8l, btree_traits::bytes_per_page / (sizeof(key_proxy) + sizeof(void*)) );

		///
		/// the max scan value for the hybrid lower bound that allows bigger pages
		static const int    max_scan = btree_traits::max_scan;

		/// Number of cached keys
		static const int	max_release = btree_traits::max_release;

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

		typedef _Key key_type;

		typedef _Data data_type;

		typedef _Data mapped_type;

		/// Base B+ tree parameter: The number of cached key/data slots in each surface

		static const int	caches = btree_traits::caches_per_page ;

		/// Number of slots in each surface of the tree. A page has a size of about btree_traits::bytes_per_page bytes.

		static const int    surfaces = btree_traits::keys_per_page;//max_const( 8l, (btree_traits::bytes_per_page) / (sizeof(key_proxy) + sizeof(data_proxy)) ); //

		/// Number of slots in each interior node of the tree. a Page has a size of about btree_traits::bytes_per_page bytes.

		static const int    interiorslots = btree_traits::keys_per_page;//max_const( 8l,  (btree_traits::bytes_per_page*btree_traits::interior_mul) / (sizeof(key_proxy) + sizeof(void*)) );//
		///
		/// the max scan value for the hybrid lower bound that allows bigger pages
		static const int    max_scan = btree_traits::max_scan;

		///
		/// the max page releases during aggressive cleanup
		static const int    max_release = btree_traits::max_release;

	};

	enum states_
	{
		initial = 1,
		created,
		unloaded,
		loaded,
		changed
	};
	typedef short states;
	struct node_ref{
		/// references for eviction
		short					refs;
		short					s;

		bool					orphaned;
		/// the storage address for reference
		nst::stream_address		address;
		void set_address_(nst::stream_address address){
			(*this).address = address;
		}
		nst::stream_address get_address() const {
			return (*this).address ;
		}
		node_ref() : refs(0), s(initial), orphaned(false), address(0) /// , a_refs(0)
		{
		}

		/// set the orphaned flag to true if the shared flag is false

		void orphan(){
			(*this).orphaned = true;
		}

		/// return true if this pointer has been orphaned from the container
		/// later a node will be freed by any container thats in the context
		/// container cannot be freed itself

		bool is_orphaned(){
			return (*this).orphaned ;
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
		typedef typename traits::key_type  key_type;

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

		/// Base B+ tree parameter: The number of cached key slots in each surface
		static const unsigned short         cacheslotmax =  traits::caches;

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

		/// parameter to control aggressive page release for the improvement of memory use
		static const int					max_release = traits::max_release;
		/// forward decl.

		class tree_stats;
	private:
		// *** Node Classes for In-Memory and Stored Nodes

		/// proxy and interceptor class for reference counted pointers
		/// and automatic loading



		class base_proxy
		{
		protected:

			btree * context;
		public:
			node_ref * ptr;

			stream_address w;

			void make_mini(mini_pointer& m) const {

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

			inline void unref(typename stx::node_ref* ptr){
				if(ptr != NULL_REF)
				{
					ptr->refs--;

				}
			}
			inline void ref(typename stx::node_ref* ptr){
				if(ptr != NULL_REF)
				{

					ptr->refs++;

				}
			}
		public:
			inline void realize(const mini_pointer& m,btree * context){
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

				ref(super::ptr);
			}

			/// removes a reference for page management
			inline void unref(){

				unref(super::ptr);
				if(super::ptr!=NULL_REF){
					if(super::ptr->refs == 0 && super::ptr->is_orphaned()){
						if(has_context()){
							this->get_context()->free_node(rget(),this->get_where());
						}else{
							printf("context missing\n");
						}
					}
				}
			}

			/// called to set the state and members correcly when member ptr is marked as created
			/// requires non zero w (wHere it gets stored) parameter

			void create(btree * context, stream_address w){

				super::w = w;
				set_context( context );

				if(!w)
				{
					if(context->get_storage()->is_readonly()){

						printf("ERROR: allocating new page in readonly mode\n");
						throw std::exception();
					}
					context->get_storage()->allocate(super::w, stx::storage::create);
					context->get_storage()->complete();
					get_context()->change(rget(),this->get_where());
				}
				(*this).set_state( created );

			}


			/// load the persist proxy and set its state to changed
			void change_before(){
				load();
				switch((*this).get_state()){
				case created:
					break;
				case changed:
					break;
				default:
					(*this).set_state(changed);
					get_context()->change(rget(),this->get_where());

				}

				rget()->reset_cache_occupants();
				update_interpolator(rget());


			}
			void change(){
				if(version_reload && (*this).ptr != NULL_REF && (*this).is_invalid()){
					printf("WARNING: Change should be called without an invalid member as this could mean that the operation preceding it is undone\n");
				}
				change_before();
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
				BTREE_ASSERT(l != NULL_REF);
				if(l->issurfacenode()){
					update_links(static_cast<surface_node*>(l));
				}else{
					update_links(static_cast<interior_node*>(l));
				}

			}


			void update_interpolator(typename  btree::surface_node* n){
				n->initialize_interpolator(typename btree::key_interpolator(),n->keys);
			}

			void update_interpolator(typename  btree::interior_node* n){
				n->initialize_interpolator(typename btree::key_interpolator(),n->keys);
			}

			void update_interpolator(typename btree::node* l){
				BTREE_ASSERT(l != NULL_REF);
				if(l->issurfacenode()){
					update_interpolator(static_cast<surface_node*>(l));
				}else{
					update_interpolator(static_cast<interior_node*>(l));
				}

			}
			void update_links(typename btree::interior_node* n){
				BTREE_ASSERT(n != NULL_REF);
				btree* context = (*this).get_context();
				nst::u32 o = n->get_occupants() + 1;
				for(nst::u32 p = 0; p < o;++p){
					n->childid[p].discard(*context);
				}

			}
			void unlink(){
				typename btree::node* n = (*this).rget();
				if(n != NULL_REF)
					update_links(n);
			}

			/// if the state is set to loaded and a valid wHere is set the proxy will change state to unloaded
			void unload_only(){

				if((*this).get_state()==loaded && super::w){
					BTREE_ASSERT((*this).ptr != NULL_REF);
					unref();
					(*this).ptr = NULL_REF;

				}
			}

			void unload(bool release = true, bool write_data = true ){
				if(write_data)
					save(*get_context());
				if(release){
					unload_only();
				}
			}
			/// sort a node
			void sort(){
				if((*this).ptr != NULL && (*this).rget()->issurfacenode()){

					(*this).get_context()->sort(static_cast<surface_node*>((*this).rget()));
				}
			}

			/// removes held references without saving dirty data and resets state
			void clear(){
				if(super::w && (*this).ptr != NULL_REF){
					unref();
					(*this).ptr = NULL_REF;

				}
				this->set_state(initial);
				this->set_where(0);
			}
			private:
				void update_links(typename btree::surface_node* l){
					if(rget()->issurfacenode()){
						if(l->preceding.is_loaded()){
							l->preceding.rget()->unload_next();
							l->preceding.unload(true,false);
						}
						if(l->get_next().is_loaded()){
							surface_node * n = const_cast<surface_node*>(l->get_next().rget());
							n->preceding.unload(true,false);
							n->unload_next();
						}
					}else{
						BTREE_PRINT("surface node reports its not a surface node\n");
					}

				}

				bool next_check(const typename btree::surface_node* s) const {
					if(selfverify){
						if
						(	s!=nullptr
						&&	s->get_address()  > 0
						&&	(*this).get_where() > 0
						){
							if
							(	s->get_address() == s->get_next().get_where()
							){
								printf("next check failed\n");
								return false;
							}
							if
							(	s==(*this).ptr && s->get_address() != (*this).get_where()
							){
								printf("self address check failed\n");
								return false;
							}
						}
					}
					return true;
			}
			bool next_check(const typename btree::interior_node*) const {
				return false;
			}

			bool next_check(const typename btree::node* n) const {

				if(n!=nullptr){
					if(n->level == 0){
						return next_check(static_cast<const surface_node*>(n));
					}
				}

				return false;
			}

			public:
			const typename btree::surface_node* rget_surface() const {
				return reinterpret_cast<const surface_node*>(rget());
			}
			typename btree::surface_node* rget_surface() {
				return reinterpret_cast<surface_node*>(rget());
			}


			void refresh()
			{
				reload_this(this);
			}

			void discard(btree & b,bool release = true)
			{
				(*this).context = &b;
				if((*this).ptr != NULL_REF && (*this).get_state() == loaded) {

					update_links(static_cast<_Loaded*>(rget()));

					unload_only();

				}

			}
			void flush(btree & b,bool release = true)
			{
				(*this).context = &b;
				(*this).save(b);

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
				null_check();
				/// this is hidden from the MSVC inline optimizer which seems to be overactive
				if(has_context()){
					(*p) = (*p).get_context()->load(((super*)p)->w);
				}else{
					printf("WARNING: no context supplied\n");
				}

			}
			/// used to hide things from compiler optimizers which may inline to aggresively
			NO_INLINE void refresh_this(pointer_proxy* p) {//
				if(!version_reload) return;
				null_check();
				/// this is hidden from the MSVC inline optimizer which seems to be overactive
				if(has_context()){
					(*p).get_context()->refresh(((super*)p)->w,rget());
				}else{
					printf("WARNING: no context supplied\n");
				}

			}
			NO_INLINE void reload_this(pointer_proxy* p) {//
				if(!version_reload) return;
				null_check();
				/// this is hidden from the MSVC inline optimizer which seems to be overactive
				if(has_context()){

					(*p).get_context()->load(((super*)p)->w,rget());

				}else{
					printf("WARNING: no context supplied\n");
				}

			}
			/// used to hide things from compiler optimizers which may inline to aggresively
			NO_INLINE void load_this(pointer_proxy* p,stream_address loader, nst::u16 slot) {//
				null_check();
				/// this is hidden from the MSVC inline optimizer which seems to be overactive
				(*p) = (*p).get_context()->load(((super*)p)->w,loader,slot);

			}


			bool next_check() const {

				return next_check(rget());
			}


			void validate_surface_links(){
				null_check();
				if(selfverify){
					if((*this).ptr != NULL && (*this).rget()->issurfacenode()){
						surface_node * c = rget_surface();
						if(c->get_next().ptr != NULL_REF){

							const surface_node * n = c->get_next().rget_surface();
							if(n->preceding.get_where() != (*this).get_where()){
								printf("The node has invalid preceding pointer\n");
							}
						}
						if(c->preceding.ptr != NULL_REF){
							surface_node * p = c->preceding.rget_surface();
							if(p->get_next().get_where() != (*this).get_where()){
								printf("The node has invalid next pointer\n");
							}
						}
					}
				}
			}
			void null_check() const {
				if(selfverify){
					if(this->get_where()==0 && (*this).ptr != NULL_REF){
						printf("invalid where\n");
					}
				}

				///if((size_t)(this) < 60000ll){
					//// ::MessageBox(NULL,"Debug","Null error",MB_OK);
				///}
			}
			/// determines if the page should be loaded loads it and change state to loaded.
			/// The initial state can be any state
			void load() {
				null_check();
				if((*this).ptr == NULL_REF){
					if(super::w){
						/// (*this) = (*this).get_context()->load(super::w);
						/// replaced by
						load_this(this);

					}
				}
				if((*this).is_invalid()){
					refresh_this(this);
				}


			}
			/// The initial state can be any state
			void load(stream_address loader, nst::u16 slot) {
				null_check();
				if((*this).ptr == NULL_REF){
					if(super::w){

						load_this(this,loader,slot);

					}
				}
			}

			/// const version of above function - un-consting it for eveel hackery
			FORCE_INLINE void load() const
			{
				null_check();
				pointer_proxy* p = const_cast<pointer_proxy*>(this);
				p->load();
			}

			/// lets the pointer go relinquishing any states or members held
			/// and returning the relinquished resource
			_Loaded* release()
			{
				null_check();
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

				if(this != &left){

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


				}
				return *this;
			}

			pointer_proxy& operator=(const pointer_proxy &left)
			{
				if(this != &left)
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
			/// return true if the member is not set
			bool empty() const {

				return (*this).ptr == NULL_REF;
			}
			/// return the raw member ptr without loading
			/// just the non const version

			inline _Loaded * rget()
			{

				_Loaded * r = static_cast<_Loaded*>(super::ptr);

				return r;
			}

			/// return the raw member ptr without loading

			inline const _Loaded * rget() const
			{
				const _Loaded * r = static_cast<const _Loaded*>(super::ptr);

				return r;
			}

			bool is_valid() const {
				if(has_context())
					return get_context()->is_valid(rget(),super::w);
				return true;
			}

			/// is this node transactionally invalid

			bool is_invalid() const {
				if(has_context())
					return get_context()->is_invalid(rget(),super::w);
				return false;
			}
			/// 'natural' ref operator returns the pointer with loading

			inline _Loaded * operator->()
			{
				if((*this).ptr != NULL_REF && (*this).is_invalid()){
					refresh_this(this);
				}
				if((*this).ptr != NULL_REF){
					validate_surface_links();
					next_check();
					return (_Loaded*)(super::ptr);
				}
				load();
				next_check();
				return (_Loaded*)(super::ptr);
			}

			/// 'natural' deref operator returns the pointer with loading

			_Loaded& operator*()
			{
				load();
				next_check();
				return *rget();
			}

			/// 'natural' ref operator returns the pointer with loading - through const violation

			inline const _Loaded * operator->() const
			{
				load();
				next_check();
				return rget();
			}

			/// 'natural' deref operator returns the pointer with loading - through const violation

			const _Loaded& operator*() const
			{
				next_check();
				load();
				return *rget();
			}

			/// the 'safe' pointer getter
			inline const _Loaded * get() const
			{
				load();
				next_check();
				return rget();
			}
			/// the 'safe' pointer getter
			inline _Loaded * get()
			{
				load();
				next_check();
				return rget();
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
		public:
			int is_deleted;
		private:

			/// Number of key occupants, so number of valid nodes or data
			/// pointers

			storage::u16  cache_occupants;

			/// occupants: since all pages are the same size - its like a block of flats
			/// where the occupant count varies over time the size stays the same

			storage::u16  occupants;

			/// cpu cache keys
			key_type        cached[cacheslotmax+1];

			/// the persistence context
			btree * context;
		
			
		protected:
			/// the pages last transaction that it was checked on
			storage::u64	transaction ;

			/// iterpolator cached keys
			key_type        int_lower;
			key_type        int_upper;
			bool can_interp;
			bool force_refresh;
			/// the transaction version of this node
			nst::version_type version;
			/// populate cache

			template<typename key_type>
			int populate_cache( key_type* cache, int cache_size, const key_type* keys, int keys_size) const {
				/// distribute cache nodes into cache
				int step = keys_size / cache_size;
				int c = 0;
				if(step > 2){
					for(int k = 0; c < cache_size && k < keys_size; k += step){
						cache[c++]  = keys[k];
					}
					cache[c] = keys[keys_size-1];
				}
				return c;
			}
			template<typename key_type>
			void check_cache(const key_type* keys){
				if(!can_interp){
					if(get_cache_occupants() == 0){
						///set_cache_occupants(populate_cache(&cached[0], cacheslotmax, keys, get_occupants()));
					}
				}
			}

			template<typename key_type>
			void check_cache(const key_type* keys) const {
				((node*)this)->check_cache(keys);
			}
			void check_node() const {
				if(occupants > interiorslotmax+1){
					printf("ERROR: page is probably corrupt\n");
				}
			}
		public:
			node(){
				this->context = NULL_REF;
				is_deleted = 0;
			}

			~node(){
				is_deleted = 1;
			}
			
			void set_context(btree * context){
				this->context = context;
			}
		
			btree* get_context() {
				return this->context ;
			}

			void check_deleted()const{
				if(is_deleted!=0){
					printf("ERROR: node is deleted\n");
				}
			}

			void reset_cache_occupants(){
				set_cache_occupants(0);
			}


			void inc_occupants(){
				++occupants;
				set_cache_occupants(0);
				check_node();
			}
			/// set transaction checked

			nst::u64 get_transaction() const {
				check_node();
				return (*this).transaction;
			}

			void set_transaction(nst::u64 transaction) {
				check_node();
				(*this).transaction = transaction;
			}

			/// force refresh

			void set_force_refresh (bool force_refresh ){
				(*this).force_refresh = force_refresh ;
			}
			bool is_force_refresh () const{
				return (*this).force_refresh ;
			}
			/// get version

			nst::version_type get_version() const {
				check_node();
				return (*this).version;
			}

			/// set version

			void set_version(nst::version_type version){
				check_node();
				(*this).version = version;
			}
			void dec_occupants(){
				check_node();
				if(occupants > 0)
					--occupants;
				set_cache_occupants(0);
			}
			/// return the cache key count

			storage::u16 get_cache_occupants() const {
				check_node();
				return cache_occupants;
			}

			/// return the key value pair count

			storage::u16 get_occupants() const {
				check_node();
				return occupants;
			}

			/// set the key value pair count

			void set_occupants(storage::u16 o) {
				check_node();
				occupants = o;
				set_cache_occupants(0);
			}

			/// set cache size
			void set_cache_occupants(storage::u16 o){
				check_node();
				cache_occupants = o;
			}
			/// Level in the b-tree, if level == 0 -> surface node

			storage::u16  level;

			/// clock counter for lru a.k.a le roux

			//storage::u32 cc;

			/// Delayed initialisation of constructed node

			inline void initialize(btree* context, const unsigned short l)
			{
				level = l;
				occupants = 0;

				can_interp = false;
				force_refresh = false;
				version = 0;
				transaction = 0;
				address = 0;
				cache_occupants = 0;
				set_context(context);

			}
			bool is_modified() const {
				check_node();
				return s != loaded;
			}

			/// True if this is a surface node

			inline bool issurfacenode() const
			{
				check_node();
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
			template<typename key_interpolator>
			void initialize_interpolator(key_interpolator interp, const key_type* keys){
				check_node();
				can_interp = false;
				int o = get_occupants() ;
				if(o > 0){
					int_lower = keys[0];
					int_upper = keys[o-1];
					can_interp = interp.can(keys[0], keys[o-1], o);
				}
			}

			/// multiple search type lower bound template function
			/// performs a lower bound mapping using a couple of techniques simultaneously

			template<typename key_compare, typename key_interpolator >
			inline int find_lower(key_compare key_less,key_interpolator interp, const key_type* keys, const key_type& key) const {
				check_node();
				int o = get_occupants() ;
				if (o  == 0) return 0;
				check_cache(keys);

				unsigned int l = 0, h = o;
				if(can_interp){
					l = interp.interpolate(key, int_lower, int_upper, o);
					if(l >0) --l;
					if(l < h && key_less(keys[l],key)){
						l++;
						if(l < h && key_less(keys[l],key)){
						}else return l;
						++l;
						if(l < h && key_less(keys[l],key)){
						}else return l;
						++l;
						if(l < h && key_less(keys[l],key)){
						}else return l;
					}

				}
				l = 0;
				/// multiple search type lower bound function
				unsigned int ml  = 0,mb = 0;
				unsigned int step = o / cacheslotmax;
				if(get_cache_occupants()){
					ml = min_find_lower(key_less,&cached[0],get_cache_occupants(),key);
					if(ml > 0) mb = ml-1;
					l = mb * step ;
					h =(ml==cacheslotmax) ? o : ml * step;
					/// residual linear search
					///
				}else{
					int m;
					while(h-l > traits::max_scan) { //		(l < h) {  //(h-l > traits::max_scan) { //
						m = (l + h) >> 1;
						if (key_lessequal(key_less, key, keys[m])) {
							h = m;
						}else {
							l = m + 1;
						}
					}

				}
				while (l < h && key_less(keys[l],key)) ++l;

				return l;
			}

			/// simple search type lower bound template function
			template<typename key_compare>
			inline unsigned int min_find_lower(key_compare key_less,const key_type* keys, int o, const key_type& key) const {
				check_node();
				if (o  == 0) return 0;
				register unsigned int l = 0, h = o;
				/// truncated binary search
				while(h-l > (unsigned int)traits::max_scan) { //		(l < h) {  //(h-l > traits::max_scan) { //
					int m = (l + h) >> 1;
					if (key_lessequal(key_less, key, keys[m])) {
						h = m;
					}else {
						l = m + 1;
					}
				}
				/// residual linear search
				while (l < h && key_less(keys[l],key)) ++l;

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
			inline void initialize(btree* context, const unsigned short l)
			{
				node::initialize(context, l);
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
			inline int find_lower(key_compare key_less, key_interpolator interp, const key_type& key) const
			{
				return node::find_lower(key_less, interp, keys, key);

			}

			/// decodes a page from given storage and buffer and puts it in slots
			/// the buffer type is expected to be some sort of vector although no strict
			/// checking is performed
			template<typename key_interpolator >
			void load(btree * context, stream_address address, nst::version_type version, storage_type & storage, const buffer_type& buffer, key_interpolator interp)
			{
			    using namespace stx::storage;
				buffer_type::const_iterator reader = buffer.begin();
				(*this).address = address;
				(*this).set_cache_occupants (0);
				(*this).set_occupants (leb128::read_signed(reader));
				(*this).level = leb128::read_signed(reader);
				(*this).set_version(version);
				for(u16 k = 0; k <= interiorslotmax;++k){
					childid[k] = NULL_REF;
				}
				check_deleted();
				for(u16 k = 0; k < (*this).get_occupants();++k){
					storage.retrieve(buffer, reader, keys[k]);
				}
				for(u16 k = 0; k <= (*this).get_occupants();++k){
					stream_address sa = (stream_address)leb128::read_signed64(reader,buffer.end());
					childid[k].set_context(context);
					childid[k].set_where(sa);
				}
				check_deleted();
				(*this).check_cache(keys);
				this->check_node();
				node::initialize_interpolator(interp, keys);
			}

			/// encodes a node into storage page and resizes the output buffer accordingly
			/// TODO: check for any failure conditions particularly out of memory
			/// and throw an exception

			void save(storage_type &storage, buffer_type& buffer) const{
				this->check_node();
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
			void clear_references(){
				this->check_node();
				using namespace stx::storage;
				/// removes any remaining references
				for(u16 c = this->get_occupants()+1; c <= interiorslotmax;++c){
					childid[c].clear();
				}
			}
			void set_address(stream_address address){
				node_ref::set_address_(address);
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


		protected:
			/// Double linked list pointers to traverse the leaves
			typename surface_node::ptr		next;
			buffer_type						attached;

		public:
			void check_next() const {
				return;
				if(this->get_address() > 0 && next.get_where() == this->get_address()){
					printf("set bad next\n");
				}
			}

			buffer_type	&get_attached(){
				return (*this).attached;
			}
			const buffer_type &get_attached() const {
				return (*this).attached;
			}
			void transfer_attached(buffer_type& attached){
				(*this).attached.swap(attached);
			}
			void set_next(const ptr& next){

				(*this).next = next;
				check_next();
			}
			void set_next_preceding(const ptr& np){
				(*this).next->preceding = np;
				check_next();
			}
			void set_next_context(btree* context){
				this->next.set_context(context);
				check_next();
			}
			void unload_next(){

				this->next.unload(true,false);
				check_next();
			}
			void set_address(stream_address address){

				node_ref::set_address_(address);
				check_next();
			}
			
			const ptr& get_next() const {
				check_next();
				return (*this).next ;
			}

			void change_next(){
				(*this).next.change_before();
				check_next();
			}

			/// Keys of children or data pointers
			key_type        keys[surfaceslotmax];

			/// Array of data
			data_type       values[surfaceslotmax];

			/// Is the node sorted or not
			short sorted;

			/// the slot loader logical back pointers for quick release alg

			nst::u16 loaded_slot;

			stream_address loader;

			void set_slot_loader(stream_address loader, nst::u16 loaded_slot){
				(*this).loaded_slot = loaded_slot;
				(*this).loader = loader;
			}

			/// return the slot in which this page belongs
			nst::u16 get_loaded_slot() const {
				return (*this).loaded_slot;
			}

			/// return the loader of this page (interior node)
			stream_address get_loader() const {
				return (*this).loader;
			}
			/// default constructor anyone
			surface_node(){

			}
			/// Set variables to initial values
			inline void initialize(btree* context)
			{
				node::initialize(context,0);
				(*this).sorted = 0;
				(*this).loader = 0;
				(*this).loaded_slot = 0;
				(*this).transaction = 0;
				(*this).preceding = next = NULL_REF;

			}
			bool unshare(){

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

			///interpolation update

			template<typename key_interpolator>
			void initialize_interpolator(key_interpolator interp, const key_type* keys){
				(*this).can_interp = false;
				int o = this->get_occupants() ;
				if(sorted == o && o > 0){
					node::initialize_interpolator(interp, keys);
				}
			}

			template< typename key_compare, typename key_interpolator >
			inline int find_lower(key_compare key_less, key_interpolator interp, const key_type& key) const
			{
				//this->sort(get_context()->stats, key_less);

				return node::find_lower(key_less, interp, keys, key);//a_refs < 4

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

				this->check_cache(keys);
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



						if(btree::allow_duplicates) //true||
						{
							std::sort(unsorted, unsorted + node::get_occupants() );

							for(int i = 0; i < node::get_occupants(); ++i)
							{

								keys[i] = unsorted[i].key ;
								values[i] = unsorted[i].value ;

							}
						}else
						{

							std::stable_sort(unsorted, unsorted + node::get_occupants() );

							int i = 0, s = 0, p = 0;
							keys[i] = unsorted[i].key;
							values[i] = unsorted[i].value ;
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

			void load(btree* context,
				stream_address address,
				nst::version_type version,
				storage_type & storage,
				buffer_type& buffer,
				size_t bsize,
				key_interpolator interp){

			    using namespace stx::storage;
				(*this).address = address;
				/// size_t bs = buffer.size();
				buffer_type::const_iterator reader = buffer.begin();
				(*this).set_cache_occupants (0);
				(*this).set_occupants(leb128::read_signed(reader));
				(*this).level = leb128::read_signed(reader);
				(*this).set_version(version);
				(*this).next = NULL_REF;
				(*this).preceding = NULL_REF;
				stream_address sa = leb128::read_signed(reader);
				(*this).preceding.set_context(context);
				(*this).preceding.set_where(sa);
				sa = leb128::read_signed(reader);
				if(sa == address){
					printf("ERROR: loading node with invalid next address\n");
				}
				(*this).next.set_context(context);
				(*this).next.set_where(sa);

				if(interp.encoded(btree::allow_duplicates)){
					interp.decode(buffer, reader, keys, (*this).get_occupants());
				}else{
					for(u16 k = 0; k < (*this).get_occupants();++k){
						storage.retrieve(buffer, reader, keys[k]);
					}
				}

				if(interp.encoded_values(btree::allow_duplicates)){
					interp.decode_values(buffer, reader, values, (*this).get_occupants());
				}else{

					for(u16 k = 0; k < (*this).get_occupants();++k){

						storage.retrieve(buffer, reader, values[k]);

					}
				}

				size_t d = reader - buffer.begin();

				if(d != bsize){
					BTREE_ASSERT(d == buffer.size());
				}
				(*this).sorted = (*this).get_occupants();
				(*this).check_cache(keys);

				node::initialize_interpolator(interp, keys);
				if
				(
					has_typedef_attached_values<key_type>::value
				||	has_typedef_attached_values<data_type>::value
				){
					transfer_attached(buffer);
				}
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
				}else{
					for(u16 k = 0; k < (*this).get_occupants();++k){
						storage_use += storage.store_size(keys[k]);
					}

				}
				if(interp.encoded_values(btree::allow_duplicates)){
					storage_use += interp.encoded_values_size(values, (*this).get_occupants());
				}else{
					for(u16 k = 0; k < (*this).get_occupants();++k){
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
				if(interp.encoded_values(btree::allow_duplicates)){
					interp.encode_values(buffer, writer, values, (*this).get_occupants());
				}else{
					for(u16 k = 0; k < (*this).get_occupants();++k){
						storage.store(writer, values[k]);
					}
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

		/// typedef std::unordered_map<stream_address, node*> _AddressedNodes;
		/// , sta::tracker<stream_address,sta::bt_counter>


		typedef rabbit::unordered_map<stream_address, node*> _AddressedNodes;
		///typedef std::unordered_map<stream_address, node*> _AddressedNodes;



		typedef std::pair<stream_address, ::stx::storage::version_type> _AddressPair;
		typedef std::map<_AddressPair, node*, ::std::less<_AddressPair>> _AddressedVersionNodes; /// , ::sta::tracker<_AddressPair, ::sta::bt_counter>
		typedef std::pair<stream_address, node*> _AllocatedNode;
		typedef std::pair<stream_address, surface_node*> _AllocatedSurfaceNode;
		typedef std::vector< _AllocatedSurfaceNode, ::sta::tracker<_AllocatedSurfaceNode,::sta::bt_counter> > _AllocatedSurfaceNodes;
		typedef std::vector< _AllocatedNode, ::sta::tracker<_AllocatedSurfaceNode,::sta::bt_counter> > _AllocatedNodes;

		
		/// provides register for currently loaded/decoded nodes
		/// used to prevent reinstantiation of existing nodes
		/// therefore providing consistency to updates to any
		/// node

		_AddressedNodes		nodes_loaded;
		_AddressedNodes		interiors_loaded;
		_AddressedNodes		surfaces_loaded;
		_AllocatedNodes		modified;

		///	returns NULL if a node with given storage address is not currently
		/// loaded. otherwise returns the currently loaded node

		const node* get_loaded(stream_address w) const {
			auto i = nodes_loaded.find(w);
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

		/// called when a page is changed

		void change(interior_node* n, stream_address w){
			n->set_address(w);
			modified.push_back(std::make_pair(w, n));
		}

		/// same for surface nodes

		void change(surface_node* n, stream_address w){
			if(w == last_surface.get_where()){
				stats.last_surface_size = last_surface->get_occupants();
			}
			n->set_address(w);
			modified.push_back(std::make_pair(w, n));
		}

		/// change an abstract node

		void change(node* n, stream_address w){
			if(n->issurfacenode()){

				change(static_cast<surface_node*>(n), w);

			}else{

				change(static_cast<interior_node*>(n), w);

			}

		}

		/// called to direct the saving of a inferior node to storage through instance
		/// management routines in the b-tree

		void save(interior_node* n, stream_address& w){

			if(get_storage()->is_readonly()){
				return;
			}
			if(n->is_modified()){
				//printf("[B-TREE SAVE] i-node  %lld  ->  %s ver. %lld\n", (long long)w, get_storage()->get_name().c_str(), (long long)get_storage()->get_version());
				using namespace stx::storage;
				buffer_type &buffer = get_storage()->allocate(w, stx::storage::create);

				n->save(*get_storage(), buffer);

				if(lz4){
					inplace_compress_lz4(buffer,temp_compress);
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
		buffer_type temp_compress;
		void save(surface_node* n, stream_address& w){

			if(get_storage()->is_readonly()){

				return;
			}
			if(n->is_modified()){
				//printf("[B-TREE SAVE] s-node %lld  ->  %s ver. %lld\n", (long long)w, get_storage()->get_name().c_str(), (long long)get_storage()->get_version());
				using namespace stx::storage;
				n->sort(stats, key_less);
				buffer_type &buffer = get_storage()->allocate(w,stx::storage::create);
				if(n->get_address() > 0 && n->get_address() == n->get_next().get_where()){
					printf("saving node with recursive address\n");
				}
				n->save(key_interpolator(), *get_storage(), buffer);
				if(lz4){
					inplace_compress_lz4(buffer,temp_compress);
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
		void save(typename btree::node* n,stream_address w){
			if(n == NULL_REF){
				printf("WARNING: attempting to save NULL\n");
				return ;
			}
			if(get_storage()->is_readonly()){
				return;
			}
			if(n->issurfacenode()){
				typename surface_node::ptr s= n;
				s.set_context(this);
				s.set_where(w);
				save(s);

			}else{
				typename interior_node::ptr s= n;
				s.set_context(this);
				s.set_where(w);
				save(s);
			}

		}

		/// called to route the loading of a interior or exterior node to storage through instance
		/// management routines in the b-tree
		buffer_type load_buffer ;
		buffer_type temp_buffer;
		void check_special_node(){
			if(false){
				auto n = nodes_loaded.find(574217);
				if(n != nodes_loaded.end()){
					if((*n).second->issurfacenode()){
						static_cast<surface_node*>((*n).second)->check_next();
					}
				}
			}
		}
		typename node::ptr load(stream_address w,stream_address loader=0, nst::u16 slot=0) {
			check_special_node();
			typename btree::node * nt = nodes_loaded[w];
			if(nt != NULL){
				if(is_invalid(nt, w)){
					refresh(w, nt);
				}
				typename node::ptr ns = nt;
				ns.set_where(w);
				ns.set_context(this);
				return ns;
			}
			return load(w, nullptr, loader, slot) ;
		}
		bool is_valid(const node* page, stream_address w) const {
			if (selfverify){
				if(get_storage()->current_transaction_order() < page->get_transaction()){
					printf("ERROR: page is probably corrupt\n");
				}
			}
			if(!version_reload) return true;

			if(page != NULL_REF){
				if(page->get_version() == 0) return true;
				return get_storage()->current_transaction_order() == page->get_transaction();
			}
			return false;
		}
		bool is_invalid(const node* page, stream_address w) const {

			return !is_valid(page, w);
		}

		static void set_child(typename interior_node::ptr parent,typename node::ptr child, int slot){
			if(parent->level==1){				
				if(parent->childid[slot].is_loaded()){
					typename surface_node::ptr surface = parent->childid[slot];
					if(surface->level != 0){
						printf("WARNING: invalid level on surface\n");
					}else{
						surface->set_slot_loader(parent.get_where(), slot);
					}
				}
			}
			parent->childid[slot] = child;
		}
		typename node::ptr refresh(stream_address w, surface_node* preallocated) {
			if(preallocated != nullptr){
				return load(w, preallocated, preallocated->get_loader(),preallocated->get_loaded_slot());
			}else{
				return load(w, preallocated);
			}
		}

		typename node::ptr refresh(stream_address w, node* preallocated) {

			return load(w, preallocated);
		}
		void erase_address(stream_address w){
				nodes_loaded.erase(w);
				surfaces_loaded.erase(w);
				interiors_loaded.erase(w);
		}
		void surface_check(const surface_node* surface,stream_address w) const {
			if(surface && surface->get_address() && w != surface->get_address()){
				
				const surface_node* surface_test1 = nullptr;
				const surface_node* surface_test2 = nullptr;
				
				_AddressedNodes::const_iterator s = nodes_loaded.find(surface->get_address());
				if(s!=nodes_loaded.end()){
					surface_test1 = static_cast<const surface_node*>((*s).second);
				}
				s = nodes_loaded.find(w);
				if(s != nodes_loaded.end()){
					surface_test2 =  static_cast<const surface_node*>((*s).second);
					if(surface_test2 == surface_test1){
						printf("multiple addresses pointing to same node");
					}
				}
				
				s = surfaces_loaded.find(surface->get_address());
				if(s!=surfaces_loaded.end()){
					surface_test1 = static_cast<const surface_node*>((*s).second);
				}
				s = surfaces_loaded.find(w);
				if(s != surfaces_loaded.end()){
					surface_test2 =  static_cast<const surface_node*>((*s).second);
					if(surface_test2 == surface_test1){
						printf("multiple addresses pointing to same node (2)");
				
					}
				}
								

			}
		}
		typename node::ptr load(stream_address w, node* preallocated,stream_address loader=0, nst::u16 slot=0) {
			using namespace stx::storage;
			size_t refs = 0;

			if(preallocated!=nullptr){
				if(w!=preallocated->get_address()){
					printf("WARNING: input address does not match preallocated node address\n");
					///erase_address(preallocated->get_address());
					///erase_address(w);
				}
				/// printf("restoring preallocated version on %lld\n",(long long)w);

				refs = preallocated->refs;
				if(preallocated->is_force_refresh()){
					/// JUST FOR DEBUGGING: nst::u64 to = this->get_storage()->current_transaction_order();
					preallocated->set_force_refresh(false);
					request.clear();
					request.push_back(std::make_pair(w,preallocated->get_version()));
					nst::u64 response = get_storage()->get_greater_version_diff(request);
					if(!response){
						printf("there was no new data\n");
					}else{
						printf("there was data\n");
					}

				}else if(w==preallocated->get_address()){
					
					request.clear();

					request.push_back(std::make_pair(w,preallocated->get_version()));
					nst::u64 response = get_storage()->get_greater_version_diff(request);
					if(!response){
						preallocated->set_transaction ( get_storage()->current_transaction_order() );
						typename node::ptr n = preallocated ;
						return n;
					}
				}
			}
			i32 level = 0;			
			/// storage operation started
			buffer_type& dangling_buffer = get_storage()->allocate(w, stx::storage::read);
			if(get_storage()->is_end(dangling_buffer) || dangling_buffer.size() == 0){
				printf("bad allocation at %li in %s\n",(long int)w, get_storage()->get_name().c_str());
				BTREE_ASSERT(get_storage()->is_end(dangling_buffer) && dangling_buffer.size() > 0);
				throw std::exception();
			}
			bool is_preallocated = false;
			nst::version_type version = get_storage()->get_allocated_version();
			_AddressPair ap = std::make_pair(w, version);

			size_t load_size = r_decompress_lz4(load_buffer, dangling_buffer );
			get_storage()->complete(); /// storage operation complete
			
			buffer_type::iterator reader = load_buffer.begin();
			leb128::read_signed(reader);
			level = leb128::read_signed(reader);

			if(level==0){ // its a surface
				typename surface_node::ptr s ;
				is_preallocated = (preallocated != nullptr && preallocated->level == level);
				if(is_preallocated){
					static_cast<surface_node*>(preallocated)->initialize(this);
					preallocated->refs = refs;
					s = static_cast<surface_node*>(preallocated) ;
					static_cast<surface_node*>(preallocated)->set_slot_loader(loader, slot);
				}else{
					if(preallocated != nullptr){
						printf("ERROR: page should be reassigned\n");
					}
					s = allocate_surface(w, loader, slot);
				}

				s.set_where(w);
				s->load(this, w, version, *(get_storage()), load_buffer, load_size, key_interpolator());
				s.set_state(loaded);
				s.set_where(w);
				if(!is_preallocated){
					nodes_loaded[w] = s.rget();
					surfaces_loaded[w] = s.rget();
				}

				s.rget()->set_transaction (get_storage()->current_transaction_order() );
				return s;
			}else{
				typename interior_node::ptr s;
				is_preallocated = (preallocated != nullptr && preallocated->level != 0);

				if(is_preallocated){
					static_cast<interior_node*>(preallocated)->initialize(this,level);
					preallocated->refs = refs;
					s = static_cast<interior_node*>(preallocated) ;
				}else{
					s = allocate_interior(level, w);
				}
				s.set_where(w);
				s->load(this, w, version, *(get_storage()), load_buffer, key_interpolator());

				s.set_state(loaded);
				s.set_where(w);
				if(!is_preallocated){
					nodes_loaded[w] = s.rget();
					interiors_loaded[w]= s.rget();
				}
				s.rget()->set_transaction (get_storage()->current_transaction_order() );
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
			typedef stx::initializer_pair initializer_pair;

		private:
			// *** Members

			/// The currently referenced surface node of the tree
			typename btree::surface_node::ptr      currnode;

			/// Current key/data slot referenced
			unsigned short          current_slot;
			//key_type * _keys;
			//data_type * _data;
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
		private:
			/// assign pointer caches
			void assign_pointers(){
				//_data = &currnode->values[0];
				//_keys = &currnode->keys[0];
			}
			void initialize_pointers(){
				//_data = NULL_REF;
				//_keys = NULL_REF;
			}
		public:
			// *** Methods

			/// Default-Constructor of a mutable iterator
			inline iterator()
				: currnode(NULL_REF), current_slot(0) //, _data(NULL_REF), _keys(NULL_REF)
			{ }

			/// Initializing-Constructor of a mutable iterator
			inline iterator(typename btree::surface_node::ptr l, unsigned short s)
				: currnode(l), current_slot(s)
			{
				assign_pointers();
			}

			/// Initializing-Constructor-pair of a mutable iterator
			inline iterator(const initializer_pair& initializer)
				: currnode(initializer.first), current_slot(initializer.second) //, _data(NULL_REF), _keys(NULL_REF)
			{
				assign_pointers();
			}

			/// Copy-constructor from a reverse iterator
			inline iterator(const reverse_iterator &it)
				: currnode(it.currnode), current_slot(it.current_slot) //, _data(NULL_REF), _keys(NULL_REF)
			{
				assign_pointers();
			}

			/// The next to operators have been comented out since they will cause
			/// problems in shared mode because they cannot 'render' logical
			/// keys, (keys pointed to but  not  loaded) may be fixed with
			/// hackery but not likely

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
				//return _keys[current_slot];
				return currnode->keys[current_slot];
			}
			inline key_type& key()
			{
				//return _keys[current_slot];
				return currnode->keys[current_slot];
			}

			/// Writable reference to the current data object
			inline data_type& data()
			{
				//return _data[current_slot];
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

			/// refresh finction to load the latest version of the current node
			/// usually only used for debugging
			void refresh(){
				if(currnode != NULL_REF){
					currnode.refresh();
				}
			}

			/// iterator constructing pair
			template<typename _MapType>
			inline void from_initializer(_MapType& context, const initializer_pair& init)  {
				currnode.realize(init.first,&context);
				current_slot = init.second;
				assign_pointers();
			}

			inline void from_initializer(const initializer_pair& init)  {
				currnode.realize(init.first,currnode.get_context());
				current_slot = init.second;
				assign_pointers();
			}
			inline iterator& operator= (const initializer_pair& init)  {
				currnode.realize(init.first,currnode.get_context());
				current_slot = init.second;
				assign_pointers();
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
				initialize_pointers();
			}

			/// returns true if the iterator is valid
			inline bool valid() const {
				return (currnode.has_context() && currnode.get_where()!=0);
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
				
					first_node.sort();
					total += first_node->get_occupants();
					
					first_node = first_node->get_next();
					
				
					
				}

				total += to.current_slot;
				total -= current_slot;

				return total;
			}

			/// ++Prefix advance the iterator to the next slot
			inline self& operator++()
			{

				if (current_slot + 1 < currnode->get_occupants())
				{
					++current_slot;
				}
				else if (currnode->get_next() != NULL_REF)
				{
					/// TODO:
					/// add a iterator only function here to immediately let go of iterated pages
					/// when memory is at a premium (which it usually is)
					///

					
					auto context = currnode.get_context();
					
					if(context){						
						context->check_low_memory_state();												
					}
					currnode = currnode->get_next();
					currnode.sort();					
					current_slot = 0;
					assign_pointers();
				}
				else
				{
					// this is end()

					current_slot = currnode->get_occupants();
				}

				return *this;
			}


			/// Postfix++ advance the iterator to the get_next() slot
			inline self operator++(int)
			{
				self tmp = *this;   // copy ourselves


				if (current_slot + 1 < currnode->get_occupants())
				{
					++current_slot;
				}
				else if (currnode->get_next() != NULL_REF)
				{
					if(currnode.has_context())
						currnode.get_context()->check_low_memory_state();

					currnode = currnode->get_next();
					currnode.sort();
					current_slot = 0;
					assign_pointers();
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
					if(currnode.has_context())
						currnode.get_context()->check_low_memory_state();

					currnode = currnode->preceding;
					currnode.sort();
					current_slot = currnode->get_occupants() - 1;
					assign_pointers();
				}
				else
				{
					// this is begin()
					assign_pointers();
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
					if(currnode.has_context())
						currnode.get_context()->check_low_memory_state();

					currnode = currnode->preceding;
					currnode.sort();
					current_slot = currnode->get_occupants() - 1;
					assign_pointers();
				}
				else
				{
					// this is begin()
					current_slot = 0;
					assign_pointers();
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
				else if (currnode->get_next() != NULL_REF)
				{
					if(currnode.has_context())
						currnode.get_context()->check_low_memory_state();

					currnode = currnode->get_next();
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
				else if (currnode->get_next() != NULL_REF)
				{
					if(currnode.has_context())
						currnode.get_context()->check_low_memory_state();

					currnode = currnode->get_next();
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
					if(currnode.has_context())
						currnode.get_context()->check_low_memory_state();

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

					if(currnode.has_context())
						currnode.get_context()->check_low_memory_state();

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
			/// refresh finction to load the latest version of the current node
			/// usually only used for debugging
			void refesh(){
				if((*this).currenode!=NULL_REF){
					currnode.refresh();
				}
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
				else if (currnode->get_next() != NULL_REF)
				{
					currnode = currnode->get_next();
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
				else if (currnode->get_next() != NULL_REF)
				{
					currnode = currnode->get_next();
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
				else if (currnode->get_next() != NULL_REF)
				{
					currnode = currnode->get_next();
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
				else if (currnode->get_next() != NULL_REF)
				{
					currnode = currnode->get_next();
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

			/// cached size of the last surface
			nst::i64 last_surface_size;

			/// Base B+ tree parameter: The number of cached key/data slots in each surface
			static const unsigned short         caches =  btree_self::cacheslotmax;

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
			,	last_surface_size(0)
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

		/// do a delete check

		static const bool delete_check = true;

		/// reload version mode. if true then pages as reloaded on the fly improving small transaction perfromance

		static const bool version_reload = false;

		/// Key comparison object. More comparison functions are generated from
		/// this < relation.
		key_compare key_less;

		key_interpolator key_terp;
		/// Memory allocator.
		allocator_type allocator;

		/// storage
		storage_type *storage;

		void initialize_contexts(){

			
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
				get_storage()->get_boot_value(stats.last_surface_size,5);
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

			if(get_storage()->is_readonly()) return;
			if(stats.changes)
			{
				/// avoid letting those pigeons out
				get_storage()->set_boot_value(root.get_where());
				get_storage()->set_boot_value(stats.tree_size,2);
				get_storage()->set_boot_value(headsurface.get_where(),3);
				get_storage()->set_boot_value(last_surface.get_where(),4);
				get_storage()->set_boot_value(stats.last_surface_size,5);
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

			if(stats.tree_size){
				if(!get_storage()->is_readonly()){

					for(typename _AllocatedNodes::iterator n = modified.begin(); n != modified.end(); ++n){
						this->save((*n).second,(*n).first);
					}
					modified.clear();
					if(false){
						for(auto n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
							if((*n).second != NULL_REF){
								this->save((*n).second,(*n).first);
							}/// can happen when reduce/allocate called recursively
						}
					}
					write_boot_values();
				}
				///BTREE_PRINT("flushing %ld\n",flushed);
			}


		}
		private:
		typedef std::pair<stream_address, node*> _NodePair;
		typedef std::pair<stream_address, surface_node*> _SurfaceNodePair;
		typedef std::vector<_SurfaceNodePair, ::sta::tracker<_SurfaceNodePair,::sta::bt_counter> > _ToDeleteSurface;
		typedef std::vector<_SurfaceNodePair, ::sta::tracker<_SurfaceNodePair,::sta::bt_counter> > _ToDelete;
		/// remove inter node dependencies
		typedef std::vector<_SurfaceNodePair, ::sta::tracker<_SurfaceNodePair,::sta::bt_counter> > _UnlinkNodes;
		_UnlinkNodes unlink_nodes;
		void unlink_surface_2(surface_node * surface){
			typename node::ptr t;
			auto i = interiors_loaded.begin();
			if(surface->get_loader() != 0){
				/// find the current loaded instance of the interior node
				i = interiors_loaded.find( surface->get_loader() );
				if(i!=interiors_loaded.end()){
					typename btree::interior_node* interior = (btree::interior_node*)(*i).second;
					nst::u16 p = surface->get_loaded_slot();
					interior->childid[p].discard(*this);
				}/// else interior does not exist anymore = mission accomplished
			}/// this case may result in all the links not being removed
			t = surface;
			t.set_context(this);
			t.unlink();	/// remove its sibling links
		}
		void raw_unlink_nodes_2(){
			this->headsurface.unlink();
			this->last_surface.unlink();
			this->headsurface.set_context(this);
			this->last_surface.set_context(this);
			this->root.set_context(this);
			///typedef std::vector<node::ptr,::sta::tracker<node::ptr,::sta::bt_counter>> _LinkedList;

			size_t c = 0;
			for(auto n = surfaces_loaded.begin(); n != surfaces_loaded.end(); ++n){

				surface_node* surface = (surface_node*)(*n).second;

				unlink_surface_2(surface);
				++c;
			}

		}
		void free_single_surface(stream_address a){

			auto n = this->surfaces_loaded.find(a);
			if(n!=surfaces_loaded.end()){
				surface_node* surface = (surface_node*)(*n).second;
				this->unlink_surface_2(surface);
				this->free_node((*n).second,(*n).first);
			}

		}
		void raw_unlink_nodes(){

			this->headsurface.discard(*this);
			this->last_surface.discard(*this);
			this->root.discard(*this);

			this->headsurface.set_context(this);
			this->last_surface.set_context(this);
			this->root.set_context(this);

			/// typedef std::vector<node::ptr, ::sta::tracker<node::ptr,::sta::bt_counter>> _LinkedList;

			typename node::ptr t;
			for(auto n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
				if(nodes_loaded.count((*n).first) > 0){
					if((*n).second == NULL_REF){
						/// printf("WARNING: cannot unlink null node\n");
					}else {
						t = (*n).second;
						t.set_context(this);
						t.unlink();
					}
				}
			}

		}

		void unlink_local_nodes(){

			raw_unlink_nodes();

		}
		void unlink_local_nodes_2(){

			raw_unlink_nodes_2();

		}

		void release_shared(){

		}
		public:

		/// checks if theres a low memory state and release written pages
		void check_low_memory_state(){
			/// ||allocation_pool.is_near_depleted()
			if(::stx::memory_low_state){
				reduce_use();
			}
			///last_surface.validate_surface_links();
		}

		/// writes all modified pages to storage and frees all surface nodes
		void reduce_use(){
			flush_buffers(true);
		}

		void flush_buffers(bool reduce){

			/// size_t nodes_before = nodes_loaded.size();
			ptrdiff_t save_tot = btree_totl_used;
			flush();
 			if(reduce){
				size_t nodes_before = nodes_loaded.size();
				if(nodes_before > 32){
					unlink_local_nodes_2();
					local_reduce_free();
					if(nodes_loaded.size() > nodes_before/2){
						unlink_local_nodes();
						///local_reduce_free();					
						local_free();
					}
				}

				if(save_tot > btree_totl_used)
					BTREE_PRINT("total tree use %.8g MiB after flush , nodes removed %lld remaining %lld\n"
					,	(double)btree_totl_used/(1024.0*1024.0)
					,	(long long)nodes_before - (long long)nodes_loaded.size()
					,	(long long)nodes_loaded.size());
				
			}
		}
	private:
		// *** Node Object Allocation and Deallocation Functions

		void change_use(ptrdiff_t used, ptrdiff_t inode, ptrdiff_t snode){
            stats.use += used;
			stats.interior_use += inode;
			stats.surface_use += snode;
			/// add_btree_totl_used (used);
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
		typename surface_node::ptr allocate_surface(stream_address w = 0, stream_address loader=0,nst::u16 slot =0)
		{
			if(nodes_loaded.count(w)){
				BTREE_PRINT("btree::allocate surface loading a new version of %lld\n",(nst::lld)w);
			}
			surface_node* pn = NULL_REF;

			if(pn == NULL_REF){

				if(pn == NULL_REF){
					pn = allocation_pool.allocate<surface_node,btree>(this);
				}
			}
			change_use(sizeof(surface_node),0,sizeof(surface_node));
			stats.leaves = surfaces_loaded.size();
			pn->initialize(this);
			pn->set_slot_loader(loader, slot);
			typename surface_node::ptr n = pn;
			n.create(this,w);
			n->set_next_context(this);
			n->preceding.set_context(this);

			if(n.get_where()){
				n->set_address(n.get_where());
				nodes_loaded[n.get_where()] = pn;
				surfaces_loaded[n.get_where()] = pn;
			}else{
				printf("no address supplied\n");
			}
			
			return n;
		}
		/// Allocate and initialize an interior node
		typename interior_node::ptr allocate_interior(unsigned short level, stream_address w = 0)
		{
			change_use(sizeof(interior_node),sizeof(interior_node),0);
			/// new (interior_node_allocator().allocate(1)) interior_node();
			interior_node* pn = allocation_pool.allocate<interior_node,btree>(this);

			pn->initialize(this,level);
			typename interior_node::ptr n = pn;
			n.create(this,w);
			
			if(n.get_where()){
				n->set_address(n.get_where());
				nodes_loaded[n.get_where()] = pn;
				interiors_loaded[n.get_where()] = pn;
			}
			stats.interiornodes = interiors_loaded.size();
			return n;
		}
		inline void free_node( surface_node* n, stream_address w ){
			if((n->refs==0 ) || n->is_orphaned()){
				save(n,w);
				if(!n->is_orphaned()){
					nodes_loaded.erase(w);
					surfaces_loaded.erase(w);
				}

				if(delete_check && n->is_deleted){
					printf("ERROR: node has been deleted\n");
				}

				if(n->is_orphaned()){
					add_btree_totl_used (-(ptrdiff_t)sizeof(surface_node));
				}else{
					change_use(-(ptrdiff_t)sizeof(surface_node),0,-(ptrdiff_t)sizeof(surface_node));
				}
				allocation_pool.free<surface_node>(n);

				stats.leaves = surfaces_loaded.size();

			}
		}

		void orphan_node(typename btree::node* n){
			if(n == NULL_REF) {
				printf("WARNING: received a NULL reference\n");
				return;
			}
			if (n->issurfacenode())
			{
				orphan_node(static_cast<surface_node*>(n));
			}
			else
			{
				orphan_node(static_cast<interior_node* >(n));
			}
		}

		void orphan_unuse(typename btree::interior_node* n){
			change_use(-(ptrdiff_t)sizeof(interior_node),-(ptrdiff_t)sizeof(interior_node),0);
		}

		void orphan_unuse(typename btree::surface_node* n){
			change_use(-(ptrdiff_t)sizeof(surface_node),0,-(ptrdiff_t)sizeof(surface_node));
		}

		void orphan_node(typename btree::interior_node* n){
			n->orphan();
		}

		void orphan_node(typename btree::surface_node* n){

			n->orphan();

		}

		inline void free_node(interior_node* n, stream_address w){
			if(n->refs == 0 || n->is_orphaned()){
				save(n,w);
				if(!n->is_orphaned()){
					nodes_loaded.erase(w);
					if(surfaces_loaded.count(w)){
						printf("ERROR: node cannot be both surface and interior unless its the root node\n");
					}
					if(interiors_loaded.count(w)==0){
						printf("ERROR: node was not registered in the interior list\n");
					}
					interiors_loaded.erase(w);
				}
				typename interior_node::alloc_type a(interior_node_allocator());
				interior_node * removed = n;
				if(removed->is_orphaned()){
					add_btree_totl_used (-(ptrdiff_t)sizeof(interior_node));
				}else{
					change_use(-(ptrdiff_t)sizeof(interior_node),-(ptrdiff_t)sizeof(interior_node),0);
				}
				/// a.destroy(removed);
				/// a.deallocate(removed, 1);
				allocation_pool.free<interior_node>(removed);
				stats.interiornodes--;
			}

        }
		//inline void free_node( typename surface_node::ptr n){
		//	free_node(n.rget(), n.get_where());
		//}
		inline void free_node( typename node::ptr n ){
			free_node(n.rget(), n.get_where());
		}
		inline bool free_surface_node(node* n, stream_address w){
			if(n == NULL_REF){
				return false;
			}
			//if(n->is_orphaned() || nodes_loaded.count(w) > 0){
				if (n->issurfacenode())
				{
					free_node(static_cast<surface_node*>(n),w);
					return true;
				}
			//}
			return false;
		}
		inline void free_node(node* n, stream_address w)
		{
			if(n == NULL_REF){
				return;
			}
			//if( n->is_orphaned() || nodes_loaded.count(w) > 0){
				if (n->issurfacenode())
				{
					free_node(static_cast<surface_node*>(n),w);
				}
				else
				{
					free_node(static_cast<interior_node* >(n),w);
				}
			//}else{
				//printf("node %lld already removed\n", (NS_STORAGE::u64)w);
			//}
			
		}

	public:
		///
		/// share pages with other trees in the named allocation space
		///
		void share(std::string name){
			unshare();

		}

		/// unshare pages and release surfaces
		void unshare(){

		}
		// *** Fast Destruction of the B+ Tree
		/// reload the tree when a new transaction starts
		nst::_VersionRequests request;
		nst::_VersionRequests response;

		void reload()
		{
				bool do_reload = version_reload;
				stx::storage::i64 sa = 0;
				stx::storage::i64 b = 0;
				if(do_reload){
					if(get_storage()->get_boot_value(b) && surfaces_loaded.size() < 300){ ///
						if(b == root.get_where()){
							get_storage()->get_boot_value(stats.tree_size,2);
							get_storage()->get_boot_value(sa,3);
							if(headsurface.get_where() != sa){
								this->headsurface.unlink();
								this->headsurface.unload();
								this->headsurface.set_context(this);
								this->headsurface.set_where((nst::stream_address)sa);
							}
							get_storage()->get_boot_value(sa,4);
							if(last_surface.get_where() != sa){
								this->last_surface.unlink();
								this->last_surface.unload();
								this->last_surface.set_context(this);
								this->last_surface.set_where((nst::stream_address)sa);
							}
							get_storage()->get_boot_value(stats.last_surface_size,5);


							return;
						}
					}
				}else{
					do_reload = true;
				}
				if(do_reload){
					clear();
					initialize_contexts();

				}

		}

		///

		void local_surface_free(){

			_NodePair node_pairs[max_release];
			size_t nodes = std::min<size_t>(max_release,stats.leaves);
			size_t p = 0;
			for(auto n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
				if((*n).second->issurfacenode() && (*n).second->refs==0){
					node_pairs[p++] = (*n);
					if(p == nodes) break;
				}
			}
			for(size_t r = 0; r < p; ++r){
				this->free_surface_node(node_pairs[r].second,node_pairs[r].first);
			}

		}
		void local_free(){

			typedef std::pair<stream_address, node*> _NodePair;
			if(nodes_loaded.size() < 256){
				_NodePair node_pairs[256];
				size_t nodes = nodes_loaded.size();
				size_t p = 0;
				for(auto n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
					node_pairs[p++] = (*n);

				}
				for(size_t r = 0; r < nodes; ++r){
					this->free_node(node_pairs[r].second,node_pairs[r].first);
				}
			}else{
				_AddressedNodes todo = nodes_loaded;
				for(auto n = todo.begin(); n != todo.end(); ++n){
					this->free_node((*n).second,(*n).first);

				}
			}


		}

		void local_reduce_free(){

			auto todo = surfaces_loaded;

			for(auto n = todo.begin(); n != todo.end(); ++n){
				this->free_node((*n).second,(*n).first);

			}

		}
		/// orphan the nodes which are still referenced

		void orphan_remaining(){

			local_free();
			/// printf("There are %ld orphans in an instance of %s\n",(long int)nodes_loaded.size(),get_storage()->get_name().c_str());
			for(auto n = nodes_loaded.begin(); n != nodes_loaded.end(); ++n){
				BTREE_ASSERT((*n).second==NULL_REF);
				BTREE_ASSERT((*n).second->refs==0);

				orphan_node((*n).second);
			}

		}
		void local_clear_and_orphan_remaining(){

			orphan_remaining();
			nodes_loaded.clear();
			interiors_loaded.clear();
			surfaces_loaded.clear();
			modified.clear();
			

		}
		/// Frees all key/data pairs and all nodes of the tree
		void clear()
		{
			if (root!=NULL_REF)
			{

				unlink_local_nodes();
				orphan_remaining();
				nodes_loaded.clear();
				interiors_loaded.clear();
				surfaces_loaded.clear();

				modified.clear();

				
				(*this).headsurface = NULL_REF;
				(*this).root = NULL_REF;
				(*this).last_surface = NULL_REF;
				stats = tree_stats();


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

		// sort a possibly unsorted page
		void sort(typename btree::surface_node* surface){
			surface->sort(((btree&)(*this)).stats, key_less);
		}

	public:
		// *** STL Iterator Construction Functions

		/// Constructs a read/data-write iterator that points to the first slot in
		/// the first surface of the B+ tree.
		inline iterator begin()
		{
			check_low_memory_state();

			return iterator(headsurface, 0);
		}

		/// Constructs a read/data-write iterator that points to the first invalid
		/// slot in the last surface of the B+ tree.
		inline iterator end()
		{
			check_low_memory_state();
			typename surface_node::ptr last = last_surface;
			last.set_context(this);
			stats.last_surface_size = last != NULL_REF ? last->get_occupants() : 0;

			return iterator(last, (short)stats.last_surface_size);/// avoids loading the whole page

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

			typename node::ptr last = last_surface;
			last.set_context((btree*)this);
			last = last_surface;

			return const_iterator(last, last.get_where() != 0 ? last->get_occupants() : 0);
		}

		/// Constructs a read/data-write reverse iterator that points to the first
		/// invalid slot in the last surface of the B+ tree. Uses STL magic.
		inline reverse_iterator rbegin()
		{
			check_low_memory_state();

			return reverse_iterator(end());
		}

		/// Constructs a read/data-write reverse iterator that points to the first
		/// slot in the first surface of the B+ tree. Uses STL magic.
		inline reverse_iterator rend()
		{
			check_low_memory_state();

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
		template <typename node_ptr_type>
		inline int find_lower(const node_ptr_type n, const key_type* keys, const key_type& key) const
		{
			return n->find_lower<key_compare,key_interpolator>(key_less, key_terp, keys, key);
		}

		template <typename node_ptr_type>
		inline int find_lower(const node_ptr_type n, const key_type& key) const
		{
			return n->find_lower<key_compare,key_interpolator>(key_less, key_terp, key);

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
			int slot = 0;
			stream_address loader = 0;
			while(!n->issurfacenode())
			{
				typename interior_node::ptr interior = n;
				slot = find_lower((interior_node*)interior, key);
				loader = interior.get_where();
				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;
			surface->set_slot_loader(loader, loader);
			int slot = find_lower((surface_node*)surface, key);
			return (slot < surface->get_occupants() && key_equal(key, surface->keys[slot]));
		}

		/// Tries to locate a key in the B+ tree and returns an iterator to the
		/// key/data slot if found. If unsuccessful it returns end().
		iterator find(const key_type &key)
		{
			check_low_memory_state();

			typename node::ptr n = root;
			if (n==NULL_REF) return end();
			int slot = 0;
			stream_address loader = 0;
			while(!n->issurfacenode())
			{

				typename interior_node::ptr interior = n;

				slot = find_lower(interior, key);
				loader = interior.get_where();
				interior->childid[slot].load(interior.get_where(),slot);
				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;
			surface->set_slot_loader(loader, slot);
			slot = find_lower(surface.rget(), key);
			return (slot < surface->get_occupants() && key_equal(key, surface->keys[slot]))
				? iterator(surface, slot) : end();
		}

		/// Tries to locate a key in the B+ tree and returns an constant iterator
		/// to the key/data slot if found. If unsuccessful it returns end().
		const_iterator find(const key_type &key) const
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return end();
			int slot = 0;

			while(!n->issurfacenode())
			{
				typename interior_node::ptr interior = n;
				slot = find_lower(interior, key);			
				interior->childid[slot].load(interior.get_where(),slot);
				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;

			slot = find_lower(surface, key);
			return (slot < surface->get_occupants() && key_equal(key, surface->keys[slot]))
				? const_iterator(surface, slot) : end();
		}

		/// Tries to locate a key in the B+ tree and returns the number of
		/// identical key entries found.
		size_type count(const key_type &key) const
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return 0;
			int slot = 0;
			
			while(!n->issurfacenode())
			{
				const typename interior_node::ptr interior = n;
				slot = find_lower(interior, key);				
				n = interior->childid[slot];
			}
			typename surface_node::ptr surface = n;
			slot = find_lower(surface, key);
			size_type num = 0;

			while (surface != NULL_REF && slot < surface->get_occupants() && key_equal(key, surface->keys[slot]))
			{
				++num;
				if (++slot >= surface->get_occupants())
				{
					surface = surface->get_next();
					slot = 0;
				}
			}

			return num;
		}

		/// Searches the B+ tree and returns an iterator to the first pair
		/// equal to or greater than key, or end() if all keys are smaller.
		iterator lower_bound(const key_type& key)
		{

			if (root==NULL_REF) return end();
			
		
			typename node::ptr n = root;
		
			
			int slot = 0;
			
			while(!n->issurfacenode())
			{
				slot = find_lower(static_cast<interior_node*>(n. operator->()), key);
				n = static_cast<interior_node*>(n. operator->())->childid[slot];
				//n = n->childid[slot];
			
			}
			typename surface_node::ptr surface = n;
			
			slot = find_lower(surface, key);
			return iterator(surface, slot);
		}

		/// Searches the B+ tree and returns a constant iterator to the
		/// first pair equal to or greater than key, or end() if all keys
		/// are smaller.
		const_iterator lower_bound(const key_type& key) const
		{
			if (root==NULL_REF) return end();
			typename interior_node::ptr n = root;
			int slot = 0;
			
			while(!n->issurfacenode())
			{

				slot = find_lower(static_cast<interior_node*>(n. operator->()), key);
				n = static_cast<interior_node*>(n. operator->())->childid[slot];
			}

			typename surface_node::ptr surface = n;
			
			slot = find_lower(surface, key);
			return const_iterator(surface, slot);
		}

		/// Searches the B+ tree and returns an iterator to the first pair
		/// greater than key, or end() if all keys are smaller or equal.
		iterator upper_bound(const key_type& key)
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return end();
			int slot =0;
			
			while(!n->issurfacenode())
			{
				typename interior_node::ptr interior = n;
				slot = find_upper(interior, key);
			
				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;
			
			slot = find_upper(surface, key);
			return iterator(surface, slot);
		}

		/// Searches the B+ tree and returns a constant iterator to the
		/// first pair greater than key, or end() if all keys are smaller
		/// or equal.
		const_iterator upper_bound(const key_type& key) const
		{
			typename node::ptr n = root;
			if (n==NULL_REF) return end();
			int slot = 0;
			
			while(!n->issurfacenode())
			{
				typename interior_node::ptr interior = n;
				slot = find_upper(interior, key);
			
				
				n = interior->childid[slot];
			}

			typename surface_node::ptr surface = n;
			
			slot = find_upper(surface, key);
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
					//if (other.root)
					//{
					//	root = copy_recursive(other.root);
					//}
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
					newsurface->set_next(NULL_REF);
					newsurface->preceding = newsurface->get_next();
				}
				else
				{
					newsurface->preceding = last_surface;
					last_surface.change_before();
					last_surface->set_next (newsurface);
					last_surface = newsurface;
					last_surface.next_check();
				}
				last_surface.next_check();
				newsurface.next_check();
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

			check_low_memory_state();

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

				int at = interior->find_lower(key_less, key_terp, key);

				BTREE_PRINT("btree::insert_descend into " << interior->childid[at] << std::endl);
				if(interior->childid[at]->issurfacenode()){
					typename surface_node::ptr s = interior->childid[at];
					s->set_slot_loader(interior.get_where(), at);
				}
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
							/// mark change before any modifications

							interior.change_before();

							typename interior_node::ptr splitinterior = splitnode;
							splitinterior.change_before();
							// move the split key and it's datum into the left node
							interior->keys[interior->get_occupants()] = *splitkey;
							interior->childid[interior->get_occupants()+1] = splitinterior->childid[0];
							interior->inc_occupants();

							// set new split key and move corresponding datum into right node
							splitinterior->childid[0] = newchild;

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
					interior.change_before();

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

					interior->inc_occupants();
				}

				return r;
			}
			else // n->issurfacenode() == true
			{
				typename surface_node::ptr surface = n;

				if
				(	//surface->sorted==0 &&
					false &&
					!allow_duplicates &&
					!surface->isfull()
				)
				{	/// this code only runs during fill
					surface.change_before();

					int i = surface->get_occupants() - 1;
					BTREE_ASSERT(i + 1 < surfaceslotmax);
					int at = i + 1;

					surface->insert(at, key, value);
					surface->sorted = 0;
					surface.next_check();

					return std::pair<iterator, bool>(iterator(surface, at), true);


				}else{

					int at = surface->find_lower(key_less, key_terp, key);

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
					// mark node as going to change

					surface.change_before();

					// put data item into correct data at

					int i = surface->get_occupants() - 1;
					BTREE_ASSERT(i + 1 < surfaceslotmax);

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
					surface.next_check();

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
			// mark node as going to change
			surface.change_before();

			unsigned int mid = (surface->get_occupants() >> 1);

			BTREE_PRINT("btree::split_surface_node on " << surface << std::endl);

			typename surface_node::ptr newsurface = allocate_surface();

			newsurface.change_before(); // its new

			newsurface->set_occupants(surface->get_occupants() - mid);

			newsurface->set_next (surface->get_next());
			if (newsurface->get_next() == NULL_REF)
			{
				BTREE_ASSERT(surface == last_surface);
				last_surface = newsurface;
			}
			else
			{
				newsurface->change_next();
				newsurface->set_next_preceding(newsurface);

			}
			surface->sort(((btree&)(*this)).stats, key_less);
			for(unsigned int slot = mid; slot < surface->get_occupants(); ++slot)
			{
				unsigned int ni = slot - mid;
				newsurface->keys[ni] = surface->keys[slot];
				newsurface->values[ni] = surface->values[slot];
			}
			/// indicates change for copy on write semantics

			surface->set_occupants(mid);
			surface->sorted = mid;
			surface->set_next (newsurface);

			newsurface->preceding = surface;
			newsurface->sorted = newsurface->get_occupants();
			surface.next_check();
			newsurface.next_check();
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
			// make node as going to change
			interior.change_before();

			unsigned int mid = (interior->get_occupants() >> 1);

			BTREE_PRINT("btree::split_interior: mid " << mid << " addslot " << addslot << std::endl);

			// if the split is uneven and the overflowing item will be put into the
			// larger node, then the smaller split node may underflow
			if (addslot <= mid && mid > interior->get_occupants() - (mid + 1))
				mid--;

			BTREE_PRINT("btree::split_interior: mid " << mid << " addslot " << addslot << std::endl);

			BTREE_PRINT("btree::split_interior_node on " << interior << " into two nodes " << mid << " and " << interior->get_occupants() - (mid + 1) << " sized" << std::endl);

			typename interior_node::ptr newinterior = allocate_interior(interior->level);

			newinterior.change_before();

			newinterior->set_occupants(interior->get_occupants() - (mid + 1));

			for(unsigned int slot = mid + 1; slot < interior->get_occupants(); ++slot)
			{
				unsigned int ni = slot - (mid + 1);
				newinterior->keys[ni] = interior->keys[slot];
				newinterior->childid[ni] = interior->childid[slot];
				interior->childid[slot].discard(*this);
			}
			newinterior->childid[newinterior->get_occupants()] = interior->childid[interior->get_occupants()];
			/// TODO: BUG: this discard causes an invalid page save
			interior->childid[interior->get_occupants()].discard(*this);


			interior->set_occupants(mid);
			interior->clear_references();

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

				/// indicates change for copy on write semantics
				surface.change_before();

				int slot = find_lower(surface, key);

				if (slot >= surface->get_occupants() || !key_equal(key, surface->keys[slot]))
				{
					BTREE_PRINT("Could not find key " << key << " to erase." << std::endl);

					return btree_not_found;
				}

				BTREE_PRINT("Found key in surface " << curr << " at slot " << slot << std::endl);


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
						// indicates modification

						parent.change_before();

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
				surface.next_check();
				
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
						parent.change_before();
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
					const interior_node * cinterior = interior.rget();
					if (interior->childid[slot]->get_occupants() != 0)
						slot++;

					interior.change_before();
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
						BTREE_ASSERT(slot > 0);
						// fix split key for leaves
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
						interior.change_before();
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

					interior.change_before();

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

			left.change_before();
			right.change_before();

			for (unsigned int i = 0; i < right->get_occupants(); i++)
			{
				left->keys[left->get_occupants() + i] = right->keys[i];
				left->values[left->get_occupants() + i] = right->values[i];
			}
			left->set_occupants(left->get_occupants() + right->get_occupants());

			left->set_next(right->get_next());
			if (left->get_next() != NULL_REF){
				left->change_next();
				left->set_next_preceding(left);
			}else{
				last_surface = left;
			}
			right->set_occupants(0);
			right.next_check();
			left.next_check();
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
			left.change_before();
			right.change_before();
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
			left.next_check();
			right.next_check();
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

			BTREE_ASSERT(left->get_next() == right);
			BTREE_ASSERT(left == right->preceding);

			BTREE_ASSERT(left->get_occupants() < right->get_occupants());
			BTREE_ASSERT(parent->childid[parentslot] == left);

			/// indicates nodes are going to change and loades latest version

			right.change_before();
			left.change_before();
			parent.change_before();

			unsigned int shiftnum = (right->get_occupants() - left->get_occupants()) >> 1;

			BTREE_PRINT("Shifting (surface) " << shiftnum << " entries to left " << left << " from right " << right << " with common parent " << parent << "." << std::endl);

			BTREE_ASSERT(left->get_occupants() + shiftnum < surfaceslotmax);

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
			left.next_check();
			right.next_check();
			// fixup parent
			if (parentslot < parent->get_occupants())
			{
				parent.change_before();
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

			right.change_before();
			left.change_before();
			parent.change_before();

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
			right.next_check();
			left.next_check();
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

			BTREE_ASSERT(left->get_next() == right);
			BTREE_ASSERT(left == right->preceding);
			BTREE_ASSERT(parent->childid[parentslot] == left);

			BTREE_ASSERT(left->get_occupants() > right->get_occupants());

			unsigned int shiftnum = (left->get_occupants() - right->get_occupants()) >> 1;

			BTREE_PRINT("Shifting (surface) " << shiftnum << " entries to right " << right << " from left " << left << " with common parent " << parent << "." << std::endl);
			// indicates pages will be changed for surface only copy on write semantics
			right.change_before();
			left.change_before();
			parent.change_before();

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
			left.next_check();
			right.next_check();
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

			right.change_before();
			left.change_before();
			parent.change_before();

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

				n = n->get_next();
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
				os << "  surface prev " << surfacenode->preceding << " next " << surfacenode->get_next() << std::endl;

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

						assert(surfacea->get_next() == surfaceb);
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

						assert(surfacea->get_next() == surfaceb);
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

				if (n->get_next()!=NULL_REF)
				{
					assert(key_lessequal(n->keys[n->get_occupants()-1], n->get_next()->keys[0]));

					assert(n == n->get_next()->preceding);
				}
				else
				{
					assert(last_surface == n);
				}

				n = n->get_next();
			}

			assert(testcount == size());
		}

	private:




	};
} // namespace stx

#endif // _STX_BTREE_H_
