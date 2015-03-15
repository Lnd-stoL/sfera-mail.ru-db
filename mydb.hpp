
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


private:
    binary_data _lookupInPage(int pageId, binary_data key);
    void _insertInPage(int pageId, const db_data_entry &element);

    db_page *_loadPage(int pageId);
    void _unloadPage(db_page *page);
    binary_data _copyDataFromLoadedPage(binary_data src);

    static bool _binaryKeyComparer(binary_data key1, binary_data key2);
    static bool _keysEqual(binary_data key1, binary_data key2);


public:
    mydb_database(const string &storageFileName);
    mydb_database(const string &storageFileName, size_t maxFileSizeBytes, const mydb_internal_config& config);
    ~mydb_database();

    void insert(const db_data_entry &element);
    void insert(binary_data key, binary_data value);

    binary_data get(binary_data key);
};

//----------------------------------------------------------------------------------------------------------------------

#endif
