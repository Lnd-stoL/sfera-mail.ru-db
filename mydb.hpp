
#ifndef _MYDB_INCLUDED_
#define _MYDB_INCLUDED_

//----------------------------------------------------------------------------------------------------------------------

#include "db_file.hpp"
#include <sstream>

//----------------------------------------------------------------------------------------------------------------------

class mydb_database
{
private:
    struct record_internal_id
    {
        int pageId;
        int inPagePosition;

        record_internal_id() : pageId(-1), inPagePosition(-1)  { }
        record_internal_id(int pi, int rp) : pageId(pi), inPagePosition(rp)  { }

        bool exists() const  { return inPagePosition >= 0; }
    };


    struct record_lookup_result
    {
        record_internal_id requestedRecord;
        record_internal_id parentRecord;

        record_lookup_result() { }
        record_lookup_result(const record_internal_id &rr, const record_internal_id &pr) :
                requestedRecord(rr), parentRecord(pr)  { }

        bool empty() const  { return !requestedRecord.exists(); }
    };


private:
    db_file  _fileStorage;
    db_page  *_rootPage = nullptr;


private:
    record_lookup_result _rKeyLookup(int pageId, int parentPageId, int parentRecordPos, binary_data key);
    void _rKeyInsertionLookup(int pageId, int parentPageId, const db_data_entry &element);
    bool _rKeyErasingLookup(int pageId, int parentPageId, int parentRecordPos, const binary_data &element);
    void _tryInsertInPage(int pageId, int parentPageId, const db_data_entry &element);
    void _tryEraseFromPage(int pageId, int parentPageId, int position);
    db_page* _splitPage(db_page *page, int parentPageId, const db_data_entry &element);
    bool _makePageMinimallyFull(db_page *page, int parentPageId, int parentRecordPos, const binary_data &element);
    void _newRoot(db_data_entry element, int leftLink, int rightLink);
    void _removeEmptyRoot();
    bool _removeFromLeaf(const record_lookup_result &lookupResult);
    void _removeFromNode(const record_lookup_result &lookupResult);
    db_page * _findNeighbours(const record_internal_id& parentRecord, int& leftPrevPageId, int& rightNextPageId);
    bool _tryTakeFromNearest(db_page *page, db_page *parentPage, int parentRecPos,
                             db_page *leftPrevPage, db_page *rightNextPage);

    db_page *_loadPage(int pageId) const;
    void _unloadPage(db_page *page) const;
    binary_data _copyDataFromLoadedPage(binary_data src) const;
    db_data_entry _copyDataFromLoadedPage(db_data_entry src) const;
    void _dump(std::ostringstream &info, int pageId) const;
    void _dumpSortedKeys(std::ostringstream &info, int pageId) const;

    static bool _binaryKeyComparer(binary_data key1, binary_data key2);
    static bool _keysEqual(binary_data key1, binary_data key2);


public:
    mydb_database(const string &storageFileName);
    mydb_database(const string &storageFileName, size_t maxFileSizeBytes, const mydb_internal_config& config);
    ~mydb_database();

    void insert(const db_data_entry &element);
    void insert(binary_data key, binary_data value);
    binary_data get(binary_data key);
    void remove(binary_data key);

    string dump() const;
    string dumpSortedKeys() const;
};

//----------------------------------------------------------------------------------------------------------------------

#endif
