
#ifndef _DB_FILE_INCLUDED_
#define _DB_FILE_INCLUDED_

//----------------------------------------------------------------------------------------------------------------------

#include "db_page.hpp"
#include "syscall_checker.h"

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    struct db_file_storage_config
    {
        size_t pageSize       = 4096;
        size_t maxStorageSize = 0;
    };

//----------------------------------------------------------------------------------------------------------------------

    class db_file_storage
    {
    private:
        int    _unixFD = -1;
        size_t _actualFileSize = 0;

        off_t  _pageSize_InfileOffset     = 0;
        off_t  _lastFreePage_InfileOffset = 0;
        off_t  _rootPageId_InfileOffset   = 0;

        size_t _pageSize          = 0;
        size_t _maxPageCount      = 0;
        int    _lastFreePage      = -1;
        off_t  _pagesStartOffset  = 0;

        size_t _pagesMetaTableSize = 0;
        off_t  _pagesMetaTableStartOffset = 0;
        uint8_t *_pagesMetaTable = nullptr;

        db_page* _rootPage = nullptr;


    private:
        void _initializeEmpty(size_t maxStorageSize);
        void _load();
        void _initPagesMetaTableByteSize();

        void  _ensureFileSize(size_t neededSize);
        off_t _rawFileWrite(off_t offset, const void *data, size_t length) const;
        off_t _rawFileRead(off_t offset, void *data, size_t length)  const;

        int   _getNextFreePageIndex();
        void  _updatePageMetaInfo(int pageIndex, bool allocated);
        void  _diskWriteRootPageId();
        off_t _pageOffset(int pageID) const;


    private:
        db_file_storage() { };

    public:
        ~db_file_storage();
        static db_file_storage* openExisting(std::string const &fileName);
        static db_file_storage* createEmpty(std::string const &fileName, db_file_storage_config const &config);

        db_page* fetchPage(int pageId);
        db_page* allocatePage(bool isLeaf);
        void releasePage(db_page *page);

        void writePage(db_page *page);
        void writeAndRelease(db_page *page);

        void deallocatePage(db_page *page);
        void deallocateAndRelease(db_page *page);

        db_page* allocateNewRootPage();
        void changeRootPage(db_page *page);
        inline db_page* rootPage() const  { return _rootPage; }
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif
