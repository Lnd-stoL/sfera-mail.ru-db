
#ifndef _DATABASE_INCLUDED_
#define _DATABASE_INCLUDED_

//----------------------------------------------------------------------------------------------------------------------

#include "db_file_storage.hpp"

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    struct database_config
    {
        size_t maxDBSize      = 0;
        size_t pageSizeBytes  = 4096;
        size_t cacheSizePages = 16;
    };

//----------------------------------------------------------------------------------------------------------------------

    class database
    {
    private:
        struct record_internal_id
        {
            int pageId;
            int inPagePosition;

            record_internal_id() : pageId(-1), inPagePosition(-1)  { }
            record_internal_id(int pi, int rp) : pageId(pi), inPagePosition(rp)  { }

            bool valid() const  { return inPagePosition >= 0; }
        };


        struct record_lookup_result
        {
            record_internal_id requestedRecord;
            record_internal_id parentRecord;

            record_lookup_result() { }
            record_lookup_result(const record_internal_id &rr, const record_internal_id &pr) :
                    requestedRecord(rr), parentRecord(pr)  { }

            bool empty() const  { return !requestedRecord.valid(); }
        };

//----------------------------------------------------------------------------------------------------------------------

    private:
        const std::string MainStorageFileName = "data.sdbs";
        const std::string LogFileName         = "binlog.sdbl";

    private:
        db_file_storage *_fileStorage = nullptr;

    private:
        record_lookup_result _rKeyLookup(int pageId, int parentPageId, int parentRecordPos, data_blob key);
        void _rKeyInsertionLookup(int pageId, int parentPageId, const key_value &element);
        bool _rKeyErasingLookup(int pageId, int parentPageId, int parentRecordPos, const data_blob &element);
        void _tryInsertInPage(int pageId, int parentPageId, const key_value &element);
        db_page* _splitPage(db_page *page, int parentPageId, const key_value &element);
        bool _makePageMinimallyFilled(db_page *page, int parentPageId, int parentRecordPos);
        void _makeNewRoot(key_value element, int leftLink, int rightLink);
        void _checkAndRemoveEmptyRoot();
        bool _removeFromLeaf(db_page *page, int recPos, int parentPageId, int parentRecordPos);
        bool _removeFromNode(db_page *nodePage, int parentPageId, int parentRecPos, int recPos);
        bool _rRemoveFromNodeR(int pageId, int parentPageId, int parentRecPos, key_value_copy &element,
                               bool canRebalance);
        bool _testRebalanceAfterDelete(int pageId, int parentPageId, int parentRecPos);
        db_page * _findNeighbours(const record_internal_id& parentRecord, int& leftPrevPageId, int& rightNextPageId);
        bool _tryTakeFromNearest(db_page *page, db_page *parentPage, int parentRecPos,
                                 db_page *leftPrevPage, db_page *rightNextPage);
        void _mergePages(db_page *page, int parentRecordPos, db_page *parentPage, db_page *rightNextPage,
                         db_page *leftPrevPage);

        data_blob _copyDataFromLoadedPage(data_blob src) const;
        key_value _copyDataFromLoadedPage(key_value src) const;
        void _dump(std::ostringstream &info, int pageId) const;
        void _rDumpSortedKeys(std::ostringstream &info, int pageId) const;

        static bool _binaryKeyComparer(data_blob key1, data_blob key2);
        static bool _keysEqual(data_blob key1, data_blob key2);


    public:
        static database* createEmpty(const std::string &path, const database_config &config);
        static database* openExisting(const std::string &path);
        ~database();

        void insert(data_blob key, data_blob value);
        data_blob get(data_blob key);
        void remove(data_blob key);

        string dump() const;
        string dumpSortedKeys() const;
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif
