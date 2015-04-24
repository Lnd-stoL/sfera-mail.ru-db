
#include "libmydb.h"
#include "mydb.hpp"

#include <iostream>

//----------------------------------------------------------------------------------------------------------------------

#define catch_exceptions(fnn,rt)  catch (const errors::syscall_result_failed &err)  { std::cerr << fnn << ": syscall_result_failed: " << err.what() << std::endl; \
 							      return rt; } \
								  catch (const std::runtime_error &err)  { std::cerr << fnn << ": runtime_error: " << err.what() << std::endl; \
 							      return rt; } \
							      catch (const std::exception &err)  { std::cerr << fnn << ": " << err.what() << std::endl; \
 							      return rt; }

//----------------------------------------------------------------------------------------------------------------------

extern "C"
mydb_database * dbopen(const char *file)
{
	try {
		mydb_database *db = new mydb_database(file);
		return db;
	}
	catch_exceptions("dbopen", nullptr);
}


extern "C"
mydb_database * dbcreate(const char *file, DBC *conf)
{
	try {
		mydb_database *db = new mydb_database(file, conf->db_size, mydb_internal_config(conf->page_size));
		return db;
	}
	catch_exceptions("dbcreate", nullptr);
}


extern "C"
int db_close(mydb_database *db)
{
	try {
		//std::cout << db->dump() << std::endl;
		delete db;
		return 0;
	}
	catch_exceptions("db_close", -1);
}


extern "C"
int db_delete(mydb_database *db, void *key, size_t length)
{
	if (db == nullptr)  return -1;

	try {
		db->remove(binary_data(key, length));
		return 0;
	}
	catch_exceptions("db_del", -1);
}


extern "C"
int db_select(mydb_database *db, void *key, size_t keyLength, void **pVal, size_t *pValLength)
{
	if (db == nullptr)  return -1;

	try {
		binary_data result = db->get(binary_data(key, keyLength));
		if (!result.valid()) {
			*pVal = nullptr;
			*pValLength = 0;
			return 1;
		}

		*pVal = result.dataPtr();
		*pValLength = result.length();
		return 0;
	}
	catch_exceptions("db_get", -1);
}


extern "C"
int db_insert(mydb_database *db, void *key, size_t keyLength, void *value, size_t valueLength)
{
	if (db == nullptr)  return -1;

	try {
		db->insert(binary_data(key, keyLength), binary_data(value, valueLength));
		return 0;
	}
	catch_exceptions("db_put", -1);
}


extern "C"
int db_flush(mydb_database *db)
{
	if (db == nullptr)  return -1;

	try {
		return 0;
	}
	catch_exceptions("db_flush", -1);
}
