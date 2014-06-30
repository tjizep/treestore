#ifndef EXAMPLE_STORAGE_CEP2013
#define EXAMPLE_STORAGE_CEP2013
#include <stx/storage/basic_storage.h>
#include <stx/btree.h>
#include <stx/btree_map>

#include <stx/btree_map.h>

#include <stx/btree_multimap>
#include <stx/btree_multimap.h>

#include <stx/btree_set>
#include "transactional_storage.h"
#include <map>
#include <vector>
#ifdef _MSC_VER
#include <conio.h>
#endif
///
/// TODO: the example storage cannot be exchanged between threads
namespace NS_STORAGE = stx::storage;
template<typename _KeyType,typename _ValueType>
class example_storage : public NS_STORAGE::basic_storage{
public:

private:

	typedef NS_STORAGE::sqlite_allocator<NS_STORAGE::stream_address, NS_STORAGE::buffer_type> _Allocations;

	_Allocations allocations;
	NS_STORAGE::stream_address boot;
	std::string name;
public:

	example_storage(std::string name) : name(name),allocations( NS_STORAGE::default_name_factory(name)), boot(1){
		allocations.set_limit(1024*1024*32);
		allocations.begin();

		/// create a block at the boot address if its not there

		allocations.initialize(boot);
		allocations.set_transient();
    }


	~example_storage() {
		try{
			/// TODO: for test move it to a specific 'close' ? function or remove completely
			allocations.rollback();
		}catch(const std::exception& ){
			/// nothing todo in destructor
		}//stx::storage::i64
    }
	bool is_end(NS_STORAGE::buffer_type &b) const {
		return allocations.is_end(b);
	}
	NS_STORAGE::version_type get_allocated_version(){
		return allocations.get_allocated_version();
	}
	void complete(){
		allocations.complete();
	}
	std::string get_name() const {
		return name;
	}
	bool is_readonly() const{
		return false;
	}
	bool get_boot_value(NS_STORAGE::i64 &r, NS_STORAGE::stream_address boot){
		return get_boot_value(r);
	}

	bool get_boot_value(NS_STORAGE::i64 &r){
		r = 0;
		NS_STORAGE::buffer_type &ba = allocations.allocate(boot, NS_STORAGE::read); /// read it
		if(!ba.empty()){
			/// the b+tree/x map needs loading
			NS_STORAGE::buffer_type::const_iterator reader = ba.begin();
			r = NS_STORAGE::leb128::read_signed(reader);
		}
		return !ba.empty();
	}
	void set_boot_value(NS_STORAGE::i64 r,NS_STORAGE::i64 boot){
		set_boot_value(r);
	}
	void set_boot_value(NS_STORAGE::i64 r){

		NS_STORAGE::buffer_type &buffer = allocations.allocate(boot, NS_STORAGE::write); /// read it
		buffer.resize(NS_STORAGE::leb128::signed_size(r));
		NS_STORAGE::buffer_type::iterator writer = buffer.begin();

		NS_STORAGE::leb128::write_signed(writer, r);

	}

	/// a kind of auto commit - by starting the transaction immediately after commit
	void commit(){
		allocations.commit();
		allocations.begin();
	}


	/// Storage functions called by b-tree
	/// allocate a new or existing buffer, new denoted by what == 0 else an existing
	/// buffer with the specfied stream address is returned - if the non nil address
	/// does not exist an exception is thrown'

	NS_STORAGE::buffer_type& allocate(NS_STORAGE::stream_address &what,NS_STORAGE::storage_action how){
		return allocations.allocate(what,how);
	}


	/// function returning the stored size in bytes of a value
	NS_STORAGE::u32 store_size(const _KeyType& k) const {
		return NS_STORAGE::leb128::signed_size(k);
	}


	/// writes a key to a vector::iterator like writer
	template<typename _Iterator>
	void store(_Iterator& writer, _KeyType value) const {

		writer = NS_STORAGE::leb128::write_signed(writer, value);
	}


	/// reads a key from a vector::iterator like reader
	template<typename _Iterator>
	void retrieve(_Iterator& reader, _KeyType &value) const {

		value = NS_STORAGE::leb128::read_signed(reader);
	}




};
template<int _Size = 16>
struct buf_type{
	static const int size =_Size;
	char buf[_Size];
	buf_type(){
		memset(buf,0,sizeof(buf));
	}
	buf_type(const buf_type& in){
		memcpy(buf, in.buf,_Size);
	}
	buf_type(const char* in){
		strncpy(buf, in, _Size);

	}
	buf_type& operator=(const buf_type& in){
		memcpy(buf, in.buf,_Size);
		return (*this);
	}
	buf_type& operator=(const char* in){
		strncpy(buf, in, _Size);
		return (*this);
	}
	bool operator!=(const buf_type& right) const {
		return (memcmp(buf, right.buf,_Size) !=0);
	};
	bool operator<(const buf_type& right) const {
		return (memcmp(buf, right.buf,_Size) < 0);
	};
};
template<typename _BuffType>
class buf_type_storage : public NS_STORAGE::basic_storage{
public:

private:

	typedef NS_STORAGE::sqlite_allocator<NS_STORAGE::stream_address, NS_STORAGE::buffer_type> _Allocations;

	_Allocations allocations;
	NS_STORAGE::stream_address boot;
public:

	buf_type_storage() : allocations( NS_STORAGE::default_name_factory("hellov_data")), boot(1){
		allocations.set_limit(1024*1024*32);
		allocations.begin();

		/// create a block at the boot address if its not there

		allocations.initialize(boot);

    }


	~buf_type_storage() {
		try{
			/// TODO: for test move it to a specific 'close' ? function or remove completely
			allocations.rollback();
		}catch(const std::exception& ){
			/// nothing todo in destructor
		}
    }

	bool get_boot_value(NS_STORAGE::stream_address &r){
		r = 0;
		NS_STORAGE::buffer_type &ba = allocations.allocate(boot, NS_STORAGE::read); /// read it
		if(!ba.empty()){
			/// the b+tree/x map needs loading
			NS_STORAGE::buffer_type::const_iterator reader = ba.begin();
			r = NS_STORAGE::leb128::read_signed(reader);
		}
		return !ba.empty();
	}

	void set_boot_value(NS_STORAGE::stream_address r){

		NS_STORAGE::buffer_type &buffer = allocations.allocate(boot, NS_STORAGE::write); /// read it
		buffer.resize(NS_STORAGE::leb128::signed_size(r));
		NS_STORAGE::buffer_type::iterator writer = buffer.begin();

		NS_STORAGE::leb128::write_signed(writer, r);

	}

	/// a kind of auto commit - by starting the transaction immediately after commit
	void commit(){
		allocations.commit();
		allocations.begin();
	}


	/// Storage functions called by b-tree
	/// allocate a new or existing buffer, new denoted by what == 0 else an existing
	/// buffer with the specfied stream address is returned - if the non nil address
	/// does not exist an exception is thrown'

	NS_STORAGE::buffer_type& allocate(NS_STORAGE::stream_address &what,NS_STORAGE::storage_action how){
		return allocations.allocate(what,how);
	}


	/// function returning the stored size in bytes of a value
	NS_STORAGE::u32 store_size(const _BuffType& k) const {

		return _BuffType::size;
	}


	/// writes a key to a vector::iterator like writer
	template<typename _Iterator>
	void store(_Iterator& writer, const _BuffType &value) const {
		size_t l = _BuffType::size;
		//writer = NS_STORAGE::leb128::write_signed(writer, l);
		for(size_t k =0; k < l; ++k){
			*writer = (basic_storage::value_type)value.buf[k];
			++writer;
		}
	}


	/// reads a key from a vector::iterator like reader
	template<typename _Iterator>
	void retrieve(_Iterator& reader, _BuffType &value) const {

		size_t l = _BuffType::size; //NS_STORAGE::leb128::read_signed(reader);
		//value.resize(l);
		for(size_t k =0; k < l; ++k){
			value.buf[k] = *reader;
			++reader;
		}

	}

	/// function returning the stored size in bytes of a value
	NS_STORAGE::u32 store_size(const unsigned int& k) const {
		return NS_STORAGE::leb128::signed_size(k);
	}


	/// writes a key to a vector::iterator like writer
	template<typename _Iterator>
	void store(_Iterator& writer, unsigned int value) const {

		writer = NS_STORAGE::leb128::write_signed(writer, value);
	}


	/// reads a key from a vector::iterator like reader
	template<typename _Iterator>
	void retrieve(_Iterator& reader, unsigned int &value) const {

		value = NS_STORAGE::leb128::read_signed(reader);
	}

};
template<typename _VectorType>
class vector_storage : public NS_STORAGE::basic_storage{
public:

private:

	typedef NS_STORAGE::sqlite_allocator<NS_STORAGE::stream_address, NS_STORAGE::buffer_type> _Allocations;

	_Allocations allocations;
	NS_STORAGE::stream_address boot;
public:

	vector_storage() : allocations( NS_STORAGE::default_name_factory("hellov_data")), boot(1){
		allocations.set_limit(1024*1024*32);
		allocations.begin();

		/// create a block at the boot address if its not there

		allocations.initialize(boot);

    }


	~vector_storage() {
		try{
			/// TODO: for test move it to a specific 'close' ? function or remove completely
			allocations.rollback();
		}catch(const std::exception& ){
			/// nothing todo in destructor
		}
    }

	bool get_boot_value(NS_STORAGE::stream_address &r){
		r = 0;
		NS_STORAGE::buffer_type &ba = allocations.allocate(boot, NS_STORAGE::read); /// read it
		if(!ba.empty()){
			/// the b+tree/x map needs loading
			NS_STORAGE::buffer_type::const_iterator reader = ba.begin();
			r = NS_STORAGE::leb128::read_signed(reader);
		}
		return !ba.empty();
	}

	void set_boot_value(NS_STORAGE::stream_address r){

		NS_STORAGE::buffer_type &buffer = allocations.allocate(boot, NS_STORAGE::write); /// read it
		buffer.resize(NS_STORAGE::leb128::signed_size(r));
		NS_STORAGE::buffer_type::iterator writer = buffer.begin();

		NS_STORAGE::leb128::write_signed(writer, r);

	}

	/// a kind of auto commit - by starting the transaction immediately after commit
	void commit(){
		allocations.commit();
		allocations.begin();
	}


	/// Storage functions called by b-tree
	/// allocate a new or existing buffer, new denoted by what == 0 else an existing
	/// buffer with the specfied stream address is returned - if the non nil address
	/// does not exist an exception is thrown'

	NS_STORAGE::buffer_type& allocate(NS_STORAGE::stream_address &what,NS_STORAGE::storage_action how){
		return allocations.allocate(what,how);
	}


	/// function returning the stored size in bytes of a value
	NS_STORAGE::u32 store_size(const _VectorType& k) const {

		return NS_STORAGE::leb128::signed_size(k.size())+k.size()*sizeof(_VectorType::value_type);
	}


	/// writes a key to a vector::iterator like writer
	template<typename _Iterator>
	void store(_Iterator& writer, const _VectorType &value) const {
		size_t l = value.size();
		writer = NS_STORAGE::leb128::write_signed(writer, l);
		for(size_t k =0; k < l; ++k){
			*writer = (basic_storage::value_type)value[k];
			++writer;
		}
	}


	/// reads a key from a vector::iterator like reader
	template<typename _Iterator>
	void retrieve(_Iterator& reader, _VectorType &value) const {

		size_t l = NS_STORAGE::leb128::read_signed(reader);
		value.resize(l);
		for(size_t k =0; k < l; ++k){
			value[k] = (_VectorType::value_type)*reader;
			++reader;
		}

	}



};
#endif
