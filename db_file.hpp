
#ifndef DB_FILE_INCLUDED
#define DB_FILE_INCLUDED

//----------------------------------------------------------------------------------------------------------------------

#include <string>
using std::string;

#include <unistd.h>
#include <fcntl.h>

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

class db_page;

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
    db_page *_rootNode = nullptr;


private:
    void _calcPagesMetaTableByteSize();
    size_t _getNextFreePageIndex();
    void _updatePageMetaInfo(size_t pageIndex, bool allocated);


public:
    db_file (const string &fileName);
    ~db_file();

    void initialize(size_t maxDataSizeBytes, const mydb_internal_config& config);
    void load();

    off_t rawWrite (off_t offset, void *data, size_t length) const;
    off_t rawRead (off_t offset, void *data, size_t length)  const;
    off_t pageInFileOffset(__uint32_t pageIndex)  const;

    db_page * loadPage (__uint32_t pageIndex);
    db_page * allocPage();
    void freePage (db_page *page);

    const mydb_internal_config& config() const  { return _basicConfig; }
};

//----------------------------------------------------------------------------------------------------------------------

#endif
