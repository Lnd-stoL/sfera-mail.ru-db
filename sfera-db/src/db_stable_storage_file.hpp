
#ifndef SFERA_DB_DB_STABLE_STORAGE_FILE_H
#define SFERA_DB_DB_STABLE_STORAGE_FILE_H

//----------------------------------------------------------------------------------------------------------------------

#include "raw_file.hpp"
#include "db_page.hpp"
#include "db_data_storage_config.hpp"

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    class db_stable_storage_file
    {
    private:
        raw_file *_file = nullptr;

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

        int _rootPageId = -1;


    private:
        void _initializeEmpty(size_t maxStorageSize);
        void _load();
        void _initPagesMetaTableByteSize();

        int   _getNextFreePageIndex();
        void  _updatePageMetaInfo(int pageIndex, bool allocated);
        void  _diskWriteRootPageId();
        off_t _pageOffset(int pageID) const;

    private:
        db_stable_storage_file() { };

    public:
        ~db_stable_storage_file();
        static db_stable_storage_file * openExisting(std::string const &fileName);
        static db_stable_storage_file * createEmpty(std::string const &fileName, db_data_storage_config const &config);

        db_page*loadPage(int pageId);
        db_page* allocatePage(bool isLeaf);

        void writePage(db_page *page);
        void deallocatePage(int pageId);
        void changeRootPage(int pageId);

        inline int rootPageId() const  { return _rootPageId; }
    };


}

//----------------------------------------------------------------------------------------------------------------------

#endif    //SFERA_DB_DB_STABLE_STORAGE_FILE_H
