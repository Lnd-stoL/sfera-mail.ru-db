
#include "mydb.hpp"

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


