
#ifndef MYDB_INCLUDED
#define MYDB_INCLUDED

//----------------------------------------------------------------------------------------------------------------------

#include <string>
using std::string;

#include "db_file.hpp"

//----------------------------------------------------------------------------------------------------------------------

class binary_data
{
private:
    size_t  _length;
    void   *_dataPtr;

public:
    binary_data (size_t length);
    binary_data (void* dataPtr, size_t length);
    binary_data (string str);

    size_t length()  const;
    void * dataPtr() const;
};

//----------------------------------------------------------------------------------------------------------------------

class db_data_entry
{
private:
    binary_data  _key;
    binary_data  _value;

public:
    const binary_data&  key()   const;
    const binary_data&  value() const;
};

//----------------------------------------------------------------------------------------------------------------------

class mydb_database
{
private:
    db_file  _fileStorage;


public:
    mydb_database (const string &storageFileName);
    mydb_database (const string &storageFileName, size_t maxFileSizeBytes, const mydb_internal_config& config);

    void insert (const db_data_entry &element);
    void insert (binary_data key, binary_data value);
};

//----------------------------------------------------------------------------------------------------------------------

#endif
