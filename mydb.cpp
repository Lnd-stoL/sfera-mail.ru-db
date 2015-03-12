
#include "mydb.hpp"

//----------------------------------------------------------------------------------------------------------------------

binary_data::binary_data (size_t length) : _length (length)
{
    _dataPtr = malloc (length);
}


binary_data::binary_data (void *dataPtr, size_t length) : _length (length)
{
    _dataPtr = dataPtr;
}


binary_data::binary_data (string str) : _length (str.size())
{
    _dataPtr = malloc (_length);
    std::copy (str.begin(), str.end(), (char *)_dataPtr);
}


size_t binary_data::length() const
{
    return _length;
}


void *binary_data::dataPtr() const
{
    return _dataPtr;
}


//----------------------------------------------------------------------------------------------------------------------


const binary_data &db_data_entry::key() const
{
    return _key;
}


const binary_data &db_data_entry::value() const
{
    return _value;
}


//----------------------------------------------------------------------------------------------------------------------

mydb_database::mydb_database (const string &storageFileName) : _fileStorage (storageFileName)
{
    _fileStorage.load();
}


mydb_database::mydb_database (const string &storageFileName, size_t maxFileSizeBytes,
                              const mydb_internal_config &config) : _fileStorage (storageFileName)
{
    _fileStorage.initialize (maxFileSizeBytes, config);
}


void mydb_database::insert (const db_data_entry &element)
{
    insert (element.key(), element.value());
}


void mydb_database::insert (binary_data key, binary_data value)
{
    db_page *page1 = _fileStorage.allocPage();
    db_page *page2 = _fileStorage.allocPage();
    _fileStorage.freePage (page1);
    db_page *page3 = _fileStorage.allocPage();
    db_page *page4 = _fileStorage.allocPage();
    _fileStorage.freePage (page2);

    delete page1;
    delete page2;
    delete page3;
    delete page4;
}
