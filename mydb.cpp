
#include "mydb.hpp"
#include <cassert>

//----------------------------------------------------------------------------------------------------------------------

mydb_database::mydb_database(const string &storageFileName) : _fileStorage(storageFileName)
{
    _fileStorage.load();
    _rootPage = _fileStorage.loadPage(_fileStorage.rootPageId());
}


mydb_database::mydb_database(const string &storageFileName,
                             size_t maxFileSizeBytes,
                             const mydb_internal_config &config) : _fileStorage(storageFileName)
{
    _fileStorage.initializeEmpty(maxFileSizeBytes, config);

    _rootPage = _fileStorage.loadUninitializedPage(_fileStorage.rootPageId(), false);
    _fileStorage.writePage(_rootPage);
}


void mydb_database::insert(const db_data_entry &element)
{
    //db_page *page1 = _fileStorage.allocLoadPage(false);
    _rootPage->insert(0, element);

    _fileStorage.writePage(_rootPage);
    //delete _rootPage;
}


void mydb_database::insert(binary_data key, binary_data value)
{
    this->insert(db_data_entry(key, value));
}


mydb_database::~mydb_database()
{
    delete _rootPage;
}
