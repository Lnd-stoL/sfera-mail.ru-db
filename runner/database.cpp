#include <dlfcn.h>

#include <string>
#include <cstring>
#include <iostream>

#include "database.h"

Database::Database(const char *so_path, const char *db_path) {
	this->so_path = so_path;
	this->db_path = db_path;
	this->db_path[min(strlen(db_path), 127)] = '\0';
	this->so_handle = dlopen(so_path, RTLD_LAZY);
	if (!this->so_handle) {
		std::cout << "Error while loading library: " << dlerror() << '\n';
		return;
	}
	this->create_function = (db_create_t )dlsym(this->so_handle, "dbcreate");
	if (!this->create_function) {
		std::cout << "Error while loading dbcreate: " << dlerror() << '\n';
		return;
	}
	this->put_function = (db_put_t )dlsym(this->so_handle, "db_insert");
	if (!this->put_function) {
		std::cout << "Error while loading db_put: " << dlerror() << '\n';
		return;
	}
	this->get_function = (db_get_t )dlsym(this->so_handle, "db_select");
	if (!this->get_function) {
		std::cout << "Error while loading db_get: " << dlerror() << '\n';
		return;
	}
	this->del_function = (db_del_t )dlsym(this->so_handle, "db_delete");
	if (!this->del_function) {
		std::cout << "Error while loading db_del: " << dlerror() << '\n';
		return;
	}
	this->close_function = (db_adm_t )dlsym(this->so_handle, "db_close");
	if (!this->close_function) {
		std::cout << "Error while loading db_close: " << dlerror() << '\n';
		return;
	}
	this->flush_function = (db_adm_t )dlsym(this->so_handle, "db_flush");
	if (!this->flush_function) {
		std::cout << "Error while loading db_flush: " << dlerror() << '\n';
		std::cout << "Skipping\n";
	}
	this->open_function = (db_open_t )dlsym(this->so_handle, "dbopen");
	if (!this->open_function) {
		std::cout << "Error while loading dbopen: " << dlerror() << '\n';
		std::cout << "Skipping\n";
	}
        struct DBC config = {256*1024*1024, 1024, 256*1024};
	this->db_object = this->create_function((char *)this->db_path.c_str(), &config);
	if (!this->db_object) {
		std::cout << "Error while creating DB\n";
		return;
	}
}

Database::~Database() {
	if (this->db_object)
		this->close();
	dlclose(this->so_handle);
}

int Database::put(const std::string& key, const std::string& val) {
	int retval = this->put_function(this->db_object, key.c_str(), key.size(), val.c_str(), val.size());
	if (retval != 0)
		std::cerr << "Error, while inserting \"" << key << "\" with value \"" << val << "\"\n";
	return retval;
}

int Database::get(const std::string& key, char **val, size_t *val_size) {
	int retval = this->get_function(this->db_object, key.c_str(), key.size(), val, val_size);
	//if (retval != 0) std::cerr << "Error, while getting \"" << key <<  "\"\n";
	return retval;
}

int Database::del(const std::string& key) {
	int retval = this->del_function(this->db_object, key.c_str(), key.size());
        //if (retval != 0)
        //	std::cerr << "Error, while deleting \"" << key << "\"\n";
	return retval;
}

int Database::close() {
	int retval = this->close_function(this->db_object);
	this->db_object = NULL;
	return retval;
}
