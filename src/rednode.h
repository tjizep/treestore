#ifndef _RED_NODE_H_CEP_
#define _RED_NODE_H_CEP_

#include <stx/storage/types.h>
#include <stx/storage/basic_storage.h>

namespace nst = stx::storage;
namespace NS_STORAGE = stx::storage;
namespace red{
	typedef nst::buffer_type block_type;
	typedef nst::u64 address_type;
	typedef nst::version_type version_type;
	/// Exceptions that may be thrown in various circumstances
	class InvalidAddressException : public std::exception{
	public: /// The address required cannot exist
		InvalidAddressException() throw() {
		}
	};
	
	/// red distributed client allocator
	/// socket for the allocator
	class client_allocator{
	private:
		
	public:
		virtual bool open(const std::string &name) = 0;		
		virtual bool close() = 0;
		virtual bool store(address_type address, const block_type &data)= 0;
		virtual bool contains(address_type address) = 0;
		virtual bool get_version(address_type address, version_type& version) = 0;
		virtual bool get(address_type address, block_type& data) = 0;		
		virtual bool get(address_type address, version_type& version, block_type& data) = 0;		
		virtual bool begin(bool writer)= 0;
		virtual bool commit()= 0;
		virtual bool rollback()= 0;		
		virtual bool is_open() const = 0;

		virtual address_type max_block_address() = 0;
		
	};
	extern client_allocator* create_allocator();
	extern void destroy_allocator(client_allocator*);
	class client_allocator_proxy{
	private:
		client_allocator* thing;
	public:
		client_allocator_proxy() : thing(create_allocator()){
		}
		~client_allocator_proxy(){
			destroy_allocator(thing);
		}
		bool open(const std::string &name) {
			return thing->open(name);
		}

		bool contains(address_type address){
			return thing->contains(address);
		}
		
		bool get_version(address_type address, version_type& version) {
			return thing->get_version(address, version);
		}

		bool store(address_type address, const block_type &data){
			return thing->store(address, data);
		}
		bool get(address_type address, block_type& data) {
			return thing->get(address, data);
		}
		bool get(address_type address, version_type& version, block_type& data) {
			return thing->get(address, version, data);
		}
		bool begin(bool writer){
			return thing->begin(writer);
		}
		bool commit(){
			return thing->commit();
		}
		bool rollback(){
			return thing->rollback();
		}
		bool is_open() const {
			return thing->is_open();
		}
		bool close(){
			return thing->close();
		}

		virtual address_type max_block_address() {
			return thing->max_block_address();
		}
		
	};
	/// store data on red
	void store_data(address_type address, address_type version, const block_type &data);

	
};
#endif