
#ifndef DB_FILE_INCLUDED
#define DB_FILE_INCLUDED

//----------------------------------------------------------------------------------------------------------------------

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
    size_t _actualFileSize = 0;
    mydb_internal_config  _basicConfig;

    size_t _maxPageCount           = 0;
    size_t _pagesMetaTableByteSize = 0;
    int    _lastFreePage           = -1;
    off_t  _pagesStartOffset       = 0;
    off_t  _pagesMetaTableStartOffset;
    int    _rootPageId = 0;

    uint8_t *_pagesMetaTable = nullptr;


private:
    void  _calcPagesMetaTableByteSize();
    int   _getNextFreePageIndex();
    void  _updatePageMetaInfo(int pageIndex, bool allocated);
    void  _extentFileTo(size_t neededSize);
    off_t _pageByteOffset(int index);

    inline size_t _pageSize() const {
        return _basicConfig.pageSize();
    }


public:
    db_file (const string &fileName);
    ~db_file();

    void initializeEmpty(size_t maxDataSizeBytes, const mydb_internal_config &config);
    void load();

    off_t rawFileWrite(off_t offset, void *data, size_t length) const;
    off_t rawFileRead(off_t offset, void *data, size_t length)  const;

    db_page * loadPage(int pageIndex);
    void writePage(db_page *page);
    int allocPage();
    db_page * allocLoadPage();
    void freePage(db_page *page);

    const mydb_internal_config& config() const  { return _basicConfig; }
    int rootPageId() const  { return _rootPageId; }
};

//----------------------------------------------------------------------------------------------------------------------

#endif
