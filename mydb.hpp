
#ifndef MYDB_INCLUDED
#define MYDB_INCLUDED

//----------------------------------------------------------------------------------------------------------------------

#include <string>
using std::string;

#include "db_file.hpp"

//----------------------------------------------------------------------------------------------------------------------

class mydb_database
{
private:
    db_file  _fileStorage;


public:
    mydb_database (const string &storageFileName);
    mydb_database (const string &storageFileName, size_t maxFileSizeBytes, const mydb_internal_config& config);
};

//----------------------------------------------------------------------------------------------------------------------

#endif
