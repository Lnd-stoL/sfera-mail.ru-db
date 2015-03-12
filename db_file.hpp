
#ifndef DB_FILE_INCLUDED
#define DB_FILE_INCLUDED

//----------------------------------------------------------------------------------------------------------------------

#include <string>
using std::string;

#include <unistd.h>
#include <fcntl.h>

#include "db_page.hpp"

//----------------------------------------------------------------------------------------------------------------------

class mydb_internal_config
{
private:
    size_t _pageSize;

public:
    mydb_internal_config (size_t pageSize);
    mydb_internal_config() { }

    size_t pageSize() const;
};

//----------------------------------------------------------------------------------------------------------------------

class db_file
{
private:
    int _unixFD = -1;
    mydb_internal_config  _basicConfig;

    size_t     _maxPageCount;
    size_t     _pagesMetaTableByteSize;
    __int32_t  _lastFreePage = -1;
    size_t     _pagesStartOffset;
    size_t     _pagesMetaTableStartOffset;

    unsigned char *_pagesMetaTable = nullptr;


private:
    off_t _write (off_t offset, void *data, size_t length);
    off_t _read (off_t offset, void *data, size_t length);

    void _calcPagesMetaTableByteSize();
    size_t _getNextFreePageIndex();
    void _writePageMetaInfo (size_t pageIndex, bool allocated);


public:
    db_file (const string &fileName);
    ~db_file();

    void initialize (size_t maxDataSizeBytes, const mydb_internal_config& config);
    void load();

    db_page * loadPage (size_t pageIndex);
    db_page * allocPage();
    void freePage (db_page *page);
};

//----------------------------------------------------------------------------------------------------------------------

#endif
