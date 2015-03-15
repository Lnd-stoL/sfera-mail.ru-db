
#ifndef _MYDB_INCLUDED_
#define _MYDB_INCLUDED_

//----------------------------------------------------------------------------------------------------------------------

#include "db_file.hpp"

//----------------------------------------------------------------------------------------------------------------------

class mydb_database
{
private:
    db_file  _fileStorage;
    db_page  *_rootPage = nullptr;


public:
    mydb_database (const string &storageFileName);
    mydb_database (const string &storageFileName, size_t maxFileSizeBytes, const mydb_internal_config& config);
    ~mydb_database();

    void insert (const db_data_entry &element);
    void insert (binary_data key, binary_data value);
};

//----------------------------------------------------------------------------------------------------------------------

#endif
