
#include "mydb.hpp"

#include <cassert>
#include <algorithm>
#include <cstring>

//----------------------------------------------------------------------------------------------------------------------

mydb_database::mydb_database(const string &storageFileName) : _fileStorage(storageFileName)
{
    _fileStorage.load();
    _rootPage = _fileStorage.loadPage(_fileStorage.rootPageId());
}


mydb_database::mydb_database(const string &storageFileName,
                             size_t maxFileSizeBytes,
                             const mydb_internal_config &config) : _fileStorage(storageFileName)
{
    _fileStorage.initializeEmpty(maxFileSizeBytes, config);

    _rootPage = _fileStorage.loadUninitializedPage(_fileStorage.rootPageId(), false);
    _fileStorage.writePage(_rootPage);
}


void mydb_database::insert(const db_data_entry &element)
{
    _insertInPage(_fileStorage.rootPageId(), element);
}


void mydb_database::insert(binary_data key, binary_data value)
{
    insert(db_data_entry(key, value));
}


mydb_database::~mydb_database()
{
    delete _rootPage;
}


binary_data mydb_database::get(binary_data key)
{
    return _lookupInPage(_fileStorage.rootPageId(), key);
}


binary_data mydb_database::_lookupInPage(int pageId, binary_data key)
{
    db_page *page = _loadPage(pageId);
    auto keyIt = std::lower_bound(page->begin(), page->end(), key, _binaryKeyComparer);

    if (keyIt == page->end()) {
        if (!page->hasLinks()) {
            return binary_data();
        }
    } else {

        if (_keysEqual(key, *keyIt)) {
            return _copyDataFromLoadedPage(keyIt.value());
        } else {
            if (!page->hasLinks()) {
                return binary_data();
            }
        }
    }

    assert(page->hasLinks());
    int nextPageId = keyIt.link();
    _unloadPage(page);

    return _lookupInPage(nextPageId, key);
}


bool mydb_database::_binaryKeyComparer(binary_data key1, binary_data key2)
{
    int cr = memcmp(key1.byteDataPtr(), key2.byteDataPtr(), std::min(key1.length(), key2.length()));

    if (cr < 0)  return true;
    if (cr == 0) return key1.length() < key2.length();
    return false;
}


bool mydb_database::_keysEqual(binary_data key1, binary_data key2)
{
    if (key1.length() != key2.length())  return false;
    return memcmp(key1.byteDataPtr(), key2.byteDataPtr(), key1.length()) == 0;
}


db_page *mydb_database::_loadPage(int pageId)
{
    if (pageId == _fileStorage.rootPageId()) {
        return _rootPage;
    }

    return _fileStorage.loadPage(pageId);
}


void mydb_database::_unloadPage(db_page *page)
{
    if (page != _rootPage) {
        delete page;
    }
}


binary_data mydb_database::_copyDataFromLoadedPage(binary_data src)
{
    uint8_t *copyPtr = (uint8_t *)malloc(src.length());
    std::copy(src.byteDataPtr(), src.byteDataEnd(), copyPtr);
    return binary_data(copyPtr, src.length());
}


void mydb_database::_insertInPage(int pageId, const db_data_entry &element)
{
    db_page *page = _loadPage(pageId);
    auto insertionIt = std::lower_bound(page->begin(), page->end(), element.key(), _binaryKeyComparer);

    page->insert(insertionIt, element);

    _fileStorage.writePage(page);
    _unloadPage(page);
}
