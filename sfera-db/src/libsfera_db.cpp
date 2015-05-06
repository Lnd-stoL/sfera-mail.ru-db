
#include "libsfera_db.h"

#include "database.hpp"
#include "syscall_checker.hpp"

#include <iostream>

//----------------------------------------------------------------------------------------------------------------------

#define catch_exceptions(fnn,rt)  catch (const errors::syscall_result_failed &err)  { std::cerr << fnn << ": syscall_result_failed: " << err.what() << std::endl; \
 							      return rt; } \
								  catch (const std::runtime_error &err)  { std::cerr << fnn << ": runtime_error: " << err.what() << std::endl; \
 							      return rt; } \
							      catch (const std::exception &err)  { std::cerr << fnn << ": " << err.what() << std::endl; \
 							      return rt; }

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

//----------------------------------------------------------------------------------------------------------------------

extern "C"
database* dbopen(const char *file)
{
	try {
		database *db = database::openExisting(file);
		return db;
	}
	catch_exceptions("dbopen", nullptr);
}


extern "C"
database* dbcreate(const char *file, DBC *conf)
{
	try {
		database_config dbConfig;
		dbConfig.pageSizeBytes = conf->page_size;
		dbConfig.maxDBSize = conf->db_size;
		dbConfig.cacheSizePages = conf->cache_size / conf->page_size;

		database *db = database::createEmpty(file, dbConfig);
		return db;
	}
	catch_exceptions("dbcreate", nullptr);
}


extern "C"
int db_close(database *db)
{
	try {
		//std::cout << db->dump() << std::endl;
		delete db;
		return 0;
	}
	catch_exceptions("db_close", -1);
}


extern "C"
int db_delete(database *db, void *key, size_t length)
{
	if (db == nullptr)  return -1;

	try {
		db->remove(data_blob((uint8_t *)key, length));
		return 0;
	}
	catch_exceptions("db_del", -1);
}


extern "C"
int db_select(database *db, void *key, size_t keyLength, void **pVal, size_t *pValLength)
{
	if (db == nullptr || key == nullptr || keyLength == 0 || pVal == nullptr || pValLength == nullptr)
		return -1;

	try {
		data_blob result = db->get(data_blob((uint8_t *)key, keyLength));
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
int db_insert(database *db, void *key, size_t keyLength, void *value, size_t valueLength)
{
	if (db == nullptr || key == nullptr || keyLength == 0 || value == nullptr || valueLength == 0)
		return -1;

	try {
		db->insert(data_blob((uint8_t *)key, keyLength), data_blob((uint8_t *)value, valueLength));
		return 0;
	}
	catch_exceptions("db_put", -1);
}


extern "C"
int db_flush(database *db)
{
	if (db == nullptr)  return -1;

	try {
		return 0;
	}
	catch_exceptions("db_flush", -1);
}
