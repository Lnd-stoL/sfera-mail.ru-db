
#ifndef DB_FILE_INCLUDED
#define DB_FILE_INCLUDED

//----------------------------------------------------------------------------------------------------------------------

#include "db_page.hpp"
#include "syscall_checker.h"

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
    off_t _pageByteOffset(int index) const;
    db_page * _loadPage(int index) const;

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

    template<class ...pageInitArgs_t>
    db_page * allocLoadPage(pageInitArgs_t... pageInitArgs);

    template<class ...pageInitArgs_t>
    db_page * loadUninitializedPage(int pageId, pageInitArgs_t... pageInitArgs);

    db_page * loadPage(int pageIndex) const;
    void writePage(db_page *page);
    int allocPage();
    void freePage(db_page *page);

    void changeRootPage(int pageIndex);

    const mydb_internal_config& config() const  { return _basicConfig; }
    int rootPageId() const  { return _rootPageId; }
};

//----------------------------------------------------------------------------------------------------------------------

template<class ...pageInitArgs_t>
db_page * db_file::allocLoadPage(pageInitArgs_t... pageInitArgs)
{
    int pageIndex = this->allocPage();
    return loadUninitializedPage(pageIndex, std::forward<pageInitArgs_t>(pageInitArgs)...);
}


template<class ...pageInitArgs_t>
db_page * db_file::loadUninitializedPage(int pageIndex, pageInitArgs_t... pageInitArgs)
{
    auto page = _loadPage(pageIndex);
    page->initializeEmpty(std::forward<pageInitArgs_t>(pageInitArgs)...);
    return page;
}

//----------------------------------------------------------------------------------------------------------------------

#endif
