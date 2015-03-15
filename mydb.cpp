
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
    _rootPage = _fileStorage.loadPage(_fileStorage.rootPageId());
}


void mydb_database::insert(const db_data_entry &element)
{
    insert(element.key(), element.value());
}


void mydb_database::insert(binary_data key, binary_data value)
{
    db_page *page1 = _fileStorage.allocLoadPage();
    db_page *page2 = _fileStorage.allocLoadPage();
    _fileStorage.freePage(page1);
    db_page *page3 = _fileStorage.allocLoadPage();
    db_page *page4 = _fileStorage.allocLoadPage();
    _fileStorage.freePage(page2);

    delete page1;
    delete page2;
    delete page3;
    delete page4;
}


mydb_database::~mydb_database()
{
    delete _rootPage;
}
