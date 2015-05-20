
#ifndef _DB_FILE_INCLUDED_
#define _DB_FILE_INCLUDED_

//----------------------------------------------------------------------------------------------------------------------

#include "pages_cache.hpp"
#include "db_stable_storage_file.hpp"
#include "db_binlog_logger.hpp"

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
    struct db_data_storage_open_params
    {
        size_t cacheSizeInPages = 256;
    };

    //----------------------------------------------------------------------------------------------------------------------

    class db_data_storage
    {
    private:
        static const std::string StableStorageFileName;
        static const std::string LogFileName;


    private:
        pages_cache *_pagesCache = nullptr;
        db_stable_storage_file *_stableStorageFile = nullptr;
        db_binlog_logger *_binlog = nullptr;

        db_operation *_currentOperation = nullptr;
        uint64_t _lastKnownOpId = 0;


    private:
        void _initializeCache(size_t sizeInPages);

    private:
        db_data_storage() { }

    public:
        ~db_data_storage();
        static db_data_storage * openExisting(std::string const &dirPath,
                                              const db_data_storage_open_params &params = db_data_storage_open_params());
        static db_data_storage * createEmpty(std::string const &dirPath, db_data_storage_config const &config);
        static bool exists(const std::string &path);

        db_page* fetchPage(int pageId);
        db_page* allocatePage(bool isLeaf);
        void releasePage(db_page *page);

        void writePage(db_page *page);
        void writeAndRelease(db_page *page);

        void deallocatePage(int pageId);
        void deallocateAndRelease(db_page *page);

        void onOperationStart(db_operation *op);
        void onOperationEnd();

        void changeRootPage(int pageId);
        inline int rootPageId() const  { return _stableStorageFile->rootPageId(); }

        inline const pages_cache& pagesCache() const  { return *_pagesCache; }
        inline uint64_t lastKnownOpId() const  { return _lastKnownOpId; }
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif
