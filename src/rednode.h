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
		client_allocator_proxy() : thing(nullptr){
		}
		~client_allocator_proxy(){
			if(thing == nullptr) return;
			destroy_allocator(thing);
		}
		bool open(bool temp, const std::string &name) {
			close();
			if(!temp){
				thing = create_allocator();
				return thing->open(name);
			}
			return false;
		}

		bool contains(address_type address){
			if(thing == nullptr) return false;
			return thing->contains(address);
		}
		
		bool get_version(address_type address, version_type& version) {
			if(thing == nullptr) return false;
			return thing->get_version(address, version);
		}

		bool store(address_type address, const block_type &data){
			if(thing == nullptr) return false;
			return thing->store(address, data);
		}
		bool get(address_type address, block_type& data) {
			if(thing == nullptr) return false;
			return thing->get(address, data);
		}
		bool get(address_type address, version_type& version, block_type& data) {
			if(thing == nullptr) return false;
			return thing->get(address, version, data);
		}
		bool begin(bool writer){
			if(thing == nullptr) return false;
			return thing->begin(writer);
		}
		bool commit(){
			if(thing == nullptr) return false;
			return thing->commit();
		}
		bool rollback(){
			if(thing == nullptr) return false;
			return thing->rollback();
		}
		bool is_open() const {
			if(thing == nullptr) return false;
			return thing->is_open();
		}
		bool close(){
			if(thing == nullptr) return false;
			bool result = thing->close();
			destroy_allocator(thing);
			thing = nullptr;
			return result;
		}

		virtual address_type max_block_address() {
			if(thing == nullptr) return false;
			return thing->max_block_address();
		}
		
	};
	/// store data on red
	void store_data(address_type address, address_type version, const block_type &data);

	
};
#endif