#ifndef _POOL_TS_H_20141129_

#define _POOL_TS_H_20141129_
#include <limits>
#include <vector>
#include <atomic>
#include <Poco/Mutex.h>
#include <Poco/Thread.h>
#include <Poco/ThreadPool.h>
#include <Poco/ThreadLocal.h>
#include <Poco/TaskManager.h>
#include <Poco/Task.h>
#include <Poco/Timestamp.h>
#include "NotificationQueueWorker.h"
#include <stx/storage/types.h>
#include <vector>
#include <rabbit/unordered_map>
#ifdef _MSC_VER
#include <sparsehash/type_traits.h>
#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>
#endif
#include <typeinfo>
#include <unordered_map>
extern void add_btree_totl_used(ptrdiff_t added);
extern void remove_btree_totl_used(ptrdiff_t added);
#ifdef _MSC_VER
#define thread_local __declspec(thread)
#endif
namespace stx{
	namespace storage{

		typedef Poco::ScopedLockWithUnlock<Poco::Mutex> synchronized;
		typedef Poco::ScopedLockWithUnlock<Poco::FastMutex> f_synchronized;
		typedef Poco::ScopedTryLock<Poco::Mutex> synchronizing;
		extern void add_buffer_use(long long added);
		extern void remove_buffer_use(long long removed);

		extern void add_total_use(long long added);
		extern void remove_total_use(long long removed);

		extern void add_col_use(long long added);
		extern void remove_col_use(long long removed);

		extern void add_stl_use(long long added);
		extern void remove_stl_use(long long added);
		extern long long total_use;
		extern long long buffer_use;
		extern long long col_use;
		extern long long stl_use;

		namespace allocation{
			struct bt_counter{
				void add(size_t bytes){
					add_btree_totl_used(bytes);
				};
				void remove(size_t bytes){
					remove_btree_totl_used(bytes);
				};
			};

			struct stl_counter{

				void add(size_t bytes){
					add_stl_use(bytes);
				};
				void remove(size_t bytes){
					remove_stl_use(bytes);
				};
			};

			struct col_counter{
				void add(size_t bytes){
					add_col_use(bytes);
				};
				void remove(size_t bytes){
					remove_col_use(bytes);
				};
			};

			struct buffer_counter{
				void add(size_t bytes){
					add_buffer_use(bytes);
				};
				void remove(size_t bytes){
					remove_buffer_use(bytes);
				};
			};
			static const bool heap_for_small_data = false;
			/// derived from The C++ Standard Library - A Tutorial and Reference - Nicolai M. Josuttis
			/// the bean counting issue
			template <class T>
			class base_tracker{
			public:

			public:
       // type definitions

				typedef T        value_type;
				typedef T*       pointer;
				typedef const T* const_pointer;
				typedef T&       reference;
				typedef const T& const_reference;
				typedef std::size_t    size_type;
				typedef std::ptrdiff_t difference_type;

				// rebind allocator to type U
				template <class U>
				struct rebind {
					typedef base_tracker<U> other;
				};

				// return address of values
				pointer address (reference value) const {
					return &value;
				}
				const_pointer address (const_reference value) const {
					return &value;
				}

				/* constructors and destructor
				* - nothing to do because the allocator has no state
				*/
				base_tracker() throw() {
				}
				base_tracker(const base_tracker&) throw() {
				}
				template <class U>
				base_tracker (const base_tracker<U>&) throw() {
				}
				~base_tracker() throw() {
				}
				// a guess of the overhead that malloc may incur per allocation
				size_type overhead() const throw() {
					return sizeof(void*);
				}
				// return maximum number of elements that can be allocated
				size_type max_size () const throw() {
					return ((std::numeric_limits<size_t>::max)()) / sizeof(T);
				}

				// allocate but don't initialize num elements of type T
				pointer allocate (size_type num, const void* = 0) {
					// print message and allocate memory with global new

					pointer ret = (pointer)(malloc(num*sizeof(T)));

					return ret;
				}
				/// for 'dense' hash map
				pointer reallocate(pointer p, size_type n) {

					return static_cast<pointer>(realloc(p, n * sizeof(value_type)));
				}
				// initialize elements of allocated storage p with value value
				void construct (T* p, const T& value) {
					// initialize memory with placement new
					new((void*)p)T(value);
				}

				// destroy elements of initialized storage p
				void destroy (pointer p) {
					// destroy objects by calling their destructor
					p->~T();
				}

				// deallocate storage p of deleted elements
				void deallocate (pointer p, size_type num) {
					// print message and deallocate memory with global delete
					free((void*)p);

				}

			};// tracker tracking allocator

			template <class T, class _Counter = stl_counter>
			class tracker{
			public:

			public:
       // type definitions

				typedef T        value_type;
				typedef T*       pointer;
				typedef const T* const_pointer;
				typedef T&       reference;
				typedef const T& const_reference;
				typedef std::size_t    size_type;
				typedef std::ptrdiff_t difference_type;
				typedef _Counter counter_type;
				// rebind allocator to type U
				template <class U>
				struct rebind {
					typedef tracker<U> other;
				};

				// return address of values
				pointer address (reference value) const {
					return &value;
				}
				const_pointer address (const_reference value) const {
					return &value;
				}

				/* constructors and destructor
				* - nothing to do because the allocator has no state
				*/
				tracker() throw() {
				}
				tracker(const tracker&) throw() {
				}
				template <class U>
				tracker (const tracker<U>&) throw() {
				}
				~tracker() throw() {
				}

				size_type overhead() const throw() {
					return sizeof(void*);
				}
				// return maximum number of elements that can be allocated
				size_type max_size () const throw() {
					return ((std::numeric_limits<size_t>::max)()) / sizeof(T);
				}

				// allocate but don't initialize num elements of type T
				pointer allocate (size_type num, const void* = 0) {
					pointer ret = (pointer)(malloc(num*sizeof(T)));
					counter_type c;
					c.add(num*sizeof(T));
					return ret;
				}
				// deallocate storage p of deleted elements
				void deallocate (pointer p, size_type num) {
					free((void*)p);
					counter_type c;
					c.remove(num*sizeof(T));
				}
				/// for 'dense' hash map
				pointer reallocate(pointer p, size_type n) {

					return static_cast<pointer>(realloc(p, n * sizeof(value_type)));
				}
				// initialize elements of allocated storage p with value value
				void construct (pointer p, const T& value) {
					// initialize memory with placement new
					new((void*)p)T(value);
				}

				// destroy elements of initialized storage p
				void destroy (pointer p) {
					// destroy objects by calling their destructor
					p->~T();
				}



			};// tracker tracking allocator
			 // return that all specializations of this allocator are interchangeable
			template <class T1, class T2>
			bool operator== (const tracker<T1>&,const tracker<T2>&) throw() {
				return true;
			}
			template <class T1, class T2>
			bool operator!= (const tracker<T1>&, const tracker<T2>&) throw() {
				return false;
			}

			template <class T>
			class pool_tracker : public base_tracker<T>{
			public:
				/// this is realy *realy* silly, isnt the types inherited as well ?? someone deserves a rasberry
				// type definitions
                typedef base_tracker<T> base_class;
				typedef T        value_type;
				typedef T*       pointer;
				typedef const T* const_pointer;
				typedef T&       reference;
				typedef const T& const_reference;
				typedef std::size_t    size_type;
				typedef std::ptrdiff_t difference_type;

				// rebind allocator to type U

				template <class U>
				struct rebind {
					typedef pool_tracker<U> other;
				};
				pool_tracker() throw() {
				}
				pool_tracker(const pool_tracker&) throw() {
				}
				template <class U>
				pool_tracker (const pool_tracker<U>&) throw() {				}

				// allocate but don't initialize num elements of type T
				pointer allocate (size_type num, const void* = 0) {
					stl_counter c;
					c.add(num*sizeof(T)+this->overhead());
					return base_class::allocate(num);
				}
				// deallocate storage p of deleted elements
				void deallocate (pointer p, size_type num) {
					base_class::deallocate(p, num);
					stl_counter c;
					c.remove(num*sizeof(T)+this->overhead());
				}
			};
			class pool_shared{
			public:
				pool_shared(){
					allocated = 0;
					used = 0;
					max_pool_size = 0;
					instances = 0;
					current = 0;
				}
				u64 max_pool_size;

				std::atomic<u64> allocated;
				std::atomic<u64> used;				
				std::atomic<u64> instances;
				std::atomic<u64> current;
			};

			/// memory allocator overlay to improve allocation speed
			class unlocked_pool{
			public:
				/// types
				struct _Allocated{
					_Allocated(u8 *data,const std::type_info& tp):data(data),ti(&tp),is_new(false){
					}
					_Allocated(){
						ti = &typeid(void);
						data = nullptr;
						is_new = true;
					}
					const std::type_info* ti;
					u8* data;
					bool is_new;
				};

				typedef std::vector<_Allocated> _Bucket; ///, pool_tracker<_Allocated>
				struct _Clocked{
					_Clocked() : size(0),clock(0),min_size(0){
					}
					size_t size;
					size_t min_size;
					u64 clock;

					_Bucket bucket;
				};
				
				typedef rabbit::unordered_map<size_t, _Clocked> _Buckets;
				
			
			protected:
				/// data/fields/state

				static const u64 MAX_ALLOCATION_SIZE = 25600000000000ll;
				static const u64 USED_PERIOD = 1000000000ll;
				static const u64 MIN_ALLOCATION_SIZE = 64ll;
				static const u64 MAX_SMALL_ALLOCATION_SIZE = MIN_ALLOCATION_SIZE*4ll;
				pool_shared * shared;

				u64 clock;
				u64 allocated;
				u64 used_period;
				u64 id;
				_Buckets buckets;


			protected:
				/// methods
				void update_min_used(_Clocked &clocked){
					if(clocked.bucket.size() < clocked.min_size){
						clocked.min_size = clocked.bucket.size();
					}
				}

				_Bucket &get_bucket(size_t size, bool nc = true){
					_Clocked &clocked =buckets[size];
					clocked.size = size;
					_Bucket &current = clocked.bucket;
					if(nc){
						clocked.clock = ++clock;
						update_min_used(clocked);
					}
					return current;
				}



				void setup_dh(){
#ifdef _MSC_VER
					///buckets.set_empty_key(MAX_ALLOCATION_SIZE+1);
					///buckets.set_deleted_key(MAX_ALLOCATION_SIZE+2);
#endif
				}

				typedef std::vector<size_t> _Empties;
				_Empties empties;

				size_t find_lru_size(){
					u64 mclock = (*this).clock;
					size_t msize = MAX_ALLOCATION_SIZE;
					size_t row = 0;
					empties.clear();
					for(_Buckets::iterator b = (*this).buckets.begin(); b != (*this).buckets.end(); ++b){
						if(b.get_value().bucket.empty()){/// skip empty buckets
							empties.push_back(b.get_key());
						}else{

							if( b.get_value().clock < mclock){
								row = 0;
								mclock = b.get_value().clock;
								msize = b.get_key();	
								//break;
							}else{
								++row;
							}
							if(row > 16 && msize < MAX_ALLOCATION_SIZE){ // if theres more than 16 in a row just stop its ok
								break;
							}
						}

					}
					for(_Empties::iterator e = empties.begin(); e != empties.end();++e){
						(*this).buckets.erase((*e));
					}
					
					return msize;
				}
				void erase_lru(){
					if(shared->used > 0 && (shared->allocated + shared->used) < shared->max_pool_size){
						return;
					}
					//if (this->allocated < ( shared->max_pool_size/shared->instances)) return;

					///if(((++shared->current) % shared->instances) != this->id) return;
					
					u64 todo = shared->allocated + shared->used - shared->max_pool_size;
					while(shared->used > 0 && (shared->allocated + shared->used) > shared->max_pool_size){
						size_t lru_size = find_lru_size();
						if(lru_size < MAX_ALLOCATION_SIZE){
							_Clocked &clocked = (*this).buckets[lru_size];						
							if(clocked.size == lru_size){
								while((shared->allocated + shared->used) > shared->max_pool_size){
									_Allocated allocated = clocked.bucket.back();
									///torelease.push_back(allocated);
									delete allocated.data;
									this->allocated -= lru_size + overhead();
									shared->used -= lru_size + overhead();
									clocked.bucket.pop_back();
									if(clocked.bucket.empty())
										break;
								}
								/// printf("removed lru pool buckets %lld from size %lld\n",(long long)required,(long long)lru_size);
							}else{
								printf("Error in lru pool buckets %lld\n",(long long)lru_size);
								
								return;
							}
							
						}else{
							
							return;
						}
					}					
				}
				

			public:

				unlocked_pool(pool_shared* shared)
				:	shared(shared)
				,	clock(0)				
				,	allocated(0)
				,	used_period(0)
				{
					//unlocked_pool::max_pool_size = max_pool_size;
					this->id = ++(shared->instances);
					printf("[INFO] [TS] creating unlocked pool\n");
					setup_dh();


				}

				~unlocked_pool(){
				}

				void check_lru(){
					erase_lru();
				}
				
				size_t get_allocated() const {
					return (*this).shared->allocated;
				}
				void set_max_pool_size(u64 max_pool_size){

					shared->max_pool_size = max_pool_size;

				}
				u64 get_used() {

					return (*this).shared->used;
				}
				u64 get_total_allocated(){

					return (*this).shared->allocated + (*this).shared->used;
				}
				void * allocate(size_t requested){

					_Allocated result = allocate_type(requested, typeid(u8));
					return result.data;
				}
				size_t overhead() const throw(){
					return sizeof(void*);
				}
				
				_Allocated allocate_type(size_t requested, const std::type_info& ti) {

					if(heap_for_small_data) { // && requested < MAX_SMALL_ALLOCATION_SIZE){
						this->allocated += requested + overhead() ;
						shared->allocated += requested + overhead() ;
						_Allocated result(new u8[requested],ti);
						result.is_new = true;

						return result;
					}
					size_t size = requested + (MIN_ALLOCATION_SIZE-(requested % MIN_ALLOCATION_SIZE));
					_Allocated result ;

					_Bucket &current =  get_bucket(size);

					if(!current.empty()){

						result = current.back();
						current.pop_back();
						shared->used -= (size + overhead());
						
					}else{						
						result.ti = &ti;
						result.is_new = true;
						result.data = new u8[size];
						
					}

					this->allocated += size + overhead();
					shared->allocated += size + overhead();					
					release_overflow();
					return result;
				}
				void free(void * data, size_t requested){
					free_type(typeid(u8),data,requested);
				}
				void free_type(const std::type_info& ti, void * data, size_t requested){
					
					if(heap_for_small_data){// && requested < MAX_SMALL_ALLOCATION_SIZE){
						shared->allocated -= requested + overhead();
						this->allocated -= requested + overhead();
						delete data;
						return;
					}
					{
						size_t size = requested + (MIN_ALLOCATION_SIZE-(requested % MIN_ALLOCATION_SIZE));
						_Bucket &current =  get_bucket(size,false);
						_Allocated allocated((u8*)data, ti);
						allocated.is_new = false;
						current.push_back(allocated);
						shared->used += (size + overhead());
						(*this).shared->allocated -= (size + overhead());
						(*this).allocated -= (size + overhead());
					}
					release_overflow();
					
				}

				/// returns true if the pool is depleted
				bool is_depleted() const {

					return ( shared->allocated >= shared->max_pool_size );
				}
				bool is_full() const {

					return ( ( shared->used + shared->allocated )  >= shared->max_pool_size );
				}
				bool is_near_full() const {

					return ( ( shared->used + shared->allocated )  >= shared->max_pool_size ) && ( shared->allocated > (shared->max_pool_size*0.1) );
				}
				void release_overflow(){					
					check_lru();					
				}
				/// returns true if the pool is nearing depletion
				bool is_near_depleted() const {
					u64 total = shared->used + shared->allocated;
					return ( total > 0.99*shared->max_pool_size ) && (shared->allocated > 0.9*shared->max_pool_size);
				}

				/// simple template allocations
				template<typename T>
				T* allocate(){
					return reinterpret_cast<T*>((*this).allocate(sizeof(T)));
				}
				template<typename T,typename _Context>
				T* allocate(_Context * context){
					
					_Allocated allocated = (*this).allocate_type(sizeof(T),typeid(T));
					void * potential = allocated.data;
					new (potential) T();

					return reinterpret_cast<T*>(potential);
				}
				template<typename T>
				void free(T* v){
					
					free_type(typeid(T), v, sizeof(T));
				}

			};
			class pool{
			protected:				
				struct thread_instance{
					pool* instance;
					unlocked_pool* per_thread_pool;
				};
				typedef std::vector<thread_instance> thread_instances;								
				pool_shared shared;
				Poco::FastMutex lock;
				unlocked_pool* unlocked;
			public:
				pool(u64 max_pool_size) : unlocked (nullptr){					
					set_max_pool_size(max_pool_size);
					unlocked = new unlocked_pool(&shared);
				}

				~pool(){

				}
				const unlocked_pool& get_pool() const {
					return const_cast<pool*>(this)->get_pool() ;
				}
				unlocked_pool& get_pool(){
					//if(unlocked)
					return *unlocked;
					if(false){
						static thread_local thread_instances* instances= nullptr;
						if(instances==nullptr){
							instances = new thread_instances;
						}
						for(thread_instances::iterator i = instances->begin(); i != instances->end(); ++i){
							if((*i).instance==this){
								return *((*i).per_thread_pool);
							}
						}
						thread_instance instance;
						instance.instance = this;
						instance.per_thread_pool = new unlocked_pool(&shared);
						instances->push_back(instance);					
						return *(instance.per_thread_pool);
					}
					
				}
				size_t get_allocated() const {

					return get_pool().get_allocated();
				}

				void set_max_pool_size(u64 max_pool_size){					
					this->shared.max_pool_size = max_pool_size;					
				}

				u64 get_used() {
					return this->shared.used;
				}
				u64 get_total_allocated(){

					return get_pool().get_total_allocated();
				}
				void * allocate(size_t requested){					
					if(unlocked){
						f_synchronized l(lock);
						return get_pool().allocate(requested);
					}else 
						return get_pool().allocate(requested);
					
				}


				void free(void * data, size_t requested){
					if(unlocked){
						f_synchronized l(lock);
						get_pool().free(data, requested);
					}else 
						get_pool().free(data, requested);
				}

				/// return true if the give size bytes can be allocated
				bool can_allocate(u64 size) const throw() {
					if(size <  this->shared.used) return true;
					u64 extra = size -  this->shared.used;
					return  this->shared.allocated + extra <  this->shared.max_pool_size;					
				}

				/// returns true if the pool is depleted
				bool is_depleted() const {
					return get_pool().is_depleted();

				}
				bool is_full() const {
					return get_pool().is_full();
				}
				
				bool is_near_full() const {
					return get_pool().is_near_full();
				}

				/// returns true if the pool is nearing depletion
				bool is_near_depleted() const {
					return get_pool().is_near_depleted();
				}
				
				/// simple template allocations
				template<typename T>
				T* allocate(){
					T* r;					
										
					if(unlocked){
						f_synchronized l(lock);
						release_overflow();
						r = get_pool().allocate<T>();					
					}else{
						release_overflow();
						r = get_pool().allocate<T>();					
					}
					
				
					return r;
				}
				template<typename T,typename _Context>
				T* allocate(_Context * context){
					T* r;					
						
					if(unlocked){
						f_synchronized l(lock);
						r = get_pool().allocate<T,_Context>(context);
					}else{
						r = get_pool().allocate<T,_Context>(context);								
					}
					
					
					return r;
				}
				template<typename T>
				inline void free(T* v){
					if(!v) return;
					v->~T();					
					if(unlocked){
						f_synchronized l(lock);
						get_pool().free<T>(v);
					}else{
						get_pool().free<T>(v);
					}
				}
				template<typename T>
				inline void free_ns(T* v){
					f_synchronized l(lock);
					get_pool().free<T>(v);				
				}
			};
		};
	};
};
/// short hand for the namespace
namespace sta = ::stx::storage::allocation;

#endif /// _POOL_TS_H_20141129_
