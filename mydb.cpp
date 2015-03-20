
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
    _rKeyInsertionLookup(_fileStorage.rootPageId(), -1, element);
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
    record_lookup_result lookupResult = _rKeyLookup(_fileStorage.rootPageId(), -1, -1, key);
    if (lookupResult.empty())  return binary_data();

    db_page *page = _loadPage(lookupResult.requestedRecord.pageId);
    auto value = _copyDataFromLoadedPage(page->value(lookupResult.requestedRecord.inPagePosition));
    _unloadPage(page);

    return value;
}


mydb_database::record_lookup_result
mydb_database::_rKeyLookup(int pageId, int parentPageId, int parentRecordPos, binary_data key)
{
    db_page *page = _loadPage(pageId);
    auto keyIt = std::lower_bound(page->begin(), page->end(), key, _binaryKeyComparer);

    if (keyIt == page->end()) {
        if (!page->hasLinks()) {
            return record_lookup_result();
        }
    } else {

        if (_keysEqual(key, *keyIt)) {
            return record_lookup_result(record_internal_id(pageId, keyIt.position()),
                    record_internal_id(parentPageId, parentRecordPos));
        } else {
            if (!page->hasLinks()) {
                return record_lookup_result();
            }
        }
    }

    assert(page->hasLinks());
    int parRecPos = keyIt.position();
    int nextPageId = keyIt.link();
    _unloadPage(page);

    return _rKeyLookup(nextPageId, pageId, parRecPos, key);
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


db_page *mydb_database::_loadPage(int pageId) const
{
    if (pageId == _fileStorage.rootPageId()) {
        return _rootPage;
    }

    return _fileStorage.loadPage(pageId);
}


void mydb_database::_unloadPage(db_page *page) const
{
    if (page == nullptr)  return;
    assert(!page->wasChanged());

    if (page != _rootPage) {
        delete page;
    }
}


binary_data mydb_database::_copyDataFromLoadedPage(binary_data src) const
{
    uint8_t *copyPtr = (uint8_t *)malloc(src.length());
    std::copy(src.byteDataPtr(), src.byteDataEnd(), copyPtr);
    return binary_data(copyPtr, src.length());
}


void mydb_database::_rKeyInsertionLookup(int pageId, int parentPageId, const db_data_entry &element)
{
    db_page *page = _loadPage(pageId);
    if (page->isFull()) {
        page = _splitPage(page, parentPageId, element);
    }

    auto keyIt = std::lower_bound(page->begin(), page->end(), element.key(), _binaryKeyComparer);

    if (keyIt != page->end() && _keysEqual(element.key(), *keyIt)) {
        if (page->canReplace(keyIt.position(), element.value())) {
            page->replace(keyIt.position(), element.value());
            _fileStorage.writePage(page);
            _unloadPage(page);
        } else {
            _unloadPage(page);
            throw std::runtime_error("Failed to update value"); // todo handle equal key error
        }
        return;
    }

    if (!page->hasLinks()) {

        int nextPageId = page->index();
        _unloadPage(page);
        _tryInsertInPage(nextPageId, parentPageId, element);
        return;
    }

    int nextPageId = keyIt.link();
    int parPageId = page->index();
    _unloadPage(page);

    return _rKeyInsertionLookup(nextPageId, parPageId, element);
}


void mydb_database::_newRoot(db_data_entry element, int leftLink, int rightLink)
{
    db_page *rootPage = _fileStorage.allocLoadPage(true);
    rootPage->insert(0, element, leftLink);
    rootPage->relink(1, rightLink);

    _fileStorage.writePage(rootPage);
    _fileStorage.changeRootPage(rootPage);
    _rootPage = rootPage;
}


void mydb_database::_tryInsertInPage(int pageId, int parentPageId, const db_data_entry &element)
{
    db_page *page = _loadPage(pageId);
    assert(!page->hasLinks());

    if (!page->possibleToInsert(element)) {
        _unloadPage(page);
        std::runtime_error("Impossible insert");
    }

    auto insertionIt = std::lower_bound(page->begin(), page->end(), element.key(), _binaryKeyComparer);
    page->insert(insertionIt, element);
    _fileStorage.writePage(page);
    _unloadPage(page);
}


db_page *mydb_database::_splitPage(db_page *page, int parentPageId, const db_data_entry &element)
{
    db_page *rightPage = _fileStorage.allocLoadPage(page->hasLinks());
    int medianKeyPosition = -1;
    db_page *leftPage = page->splitEquispace(rightPage, medianKeyPosition);
    db_data_entry medianElement = _copyDataFromLoadedPage(page->record(medianKeyPosition));
    _unloadPage(page);

    if (parentPageId == -1) {
        _newRoot(medianElement, leftPage->index(), rightPage->index());
    } else {

        db_page *parentPage = _loadPage(parentPageId);
        auto insertionIt = std::lower_bound(parentPage->begin(), parentPage->end(), medianElement.key(), _binaryKeyComparer);
        parentPage->relink(insertionIt.position(), rightPage->index());
        parentPage->insert(insertionIt, medianElement, leftPage->index());
        _fileStorage.writePage(parentPage);
        _unloadPage(parentPage);
    }

    _fileStorage.writePage(leftPage);
    _fileStorage.writePage(rightPage);

    if (_binaryKeyComparer(element.key(), medianElement.key())) {
        _unloadPage(rightPage);
        return leftPage;
    } else {
        _unloadPage(leftPage);
        return rightPage;
    }
}


db_data_entry mydb_database::_copyDataFromLoadedPage(db_data_entry src) const
{
    return db_data_entry(_copyDataFromLoadedPage(src.key()), _copyDataFromLoadedPage(src.value()));
}


string mydb_database::dump() const
{
    std::ostringstream info;
    info << "root: " << _rootPage->index() << std::endl;
    info << "dumping tree =========================================================================" << std::endl;

    _dump(info, _fileStorage.rootPageId());
    info << "======================================================================================" << std::endl;
    return info.str();
}


void mydb_database::_dump(std::ostringstream &info, int pageId) const
{
    db_page *page = _loadPage(pageId);
    info << "page #" << pageId <<  ": (has_links=" << page->hasLinks() << "; is_full="
        << page->isFull()  << ") " << std::endl;

    for (auto elementIt = page->begin(); elementIt != page->end(); ++elementIt) {
        if (page->hasLinks())  info << "\t[" << elementIt.link() << "] " << std::endl;
        info << "\t" << (*elementIt).toString() << " : " << elementIt.value().toString() << std::endl;
    }
    if (page->hasLinks()) {
        info << "\t[" << page->lastLink() << "] " << std::endl;
    }

    info << std::endl;
    if (page->hasLinks()) {
        for (auto elementIt = page->begin(); elementIt != page->end(); ++elementIt) {
            _dump(info, elementIt.link());
        }
        _dump(info, page->lastLink());
    }

    _unloadPage(page);
}


void mydb_database::remove(binary_data key)
{
    record_lookup_result lookupResult = _rKeyLookup(_fileStorage.rootPageId(), -1, -1, key);
    if (lookupResult.empty()) {
        throw 1; // todo report deleting not existing key
    }

    db_page *page = _loadPage(lookupResult.requestedRecord.pageId);
    bool hasLinks = page->hasLinks();
    _unloadPage(page);

    if (hasLinks)  _removeFromNode(lookupResult);
    else           _removeFromLeaf(lookupResult);
}


void mydb_database::_removeFromLeaf(const mydb_database::record_lookup_result &lookupResult)
{
    db_page *page = _loadPage(lookupResult.requestedRecord.pageId);
    page->remove(lookupResult.requestedRecord.inPagePosition);
    if (page->isMinimallyFilled()) {
        _fileStorage.writePage(page);
        _unloadPage(page);
        return;
    }

    int rightNextPageId = -1, leftPrevPageId = -1;
    db_page *parentPage = _findNeighbours(lookupResult.parentRecord, leftPrevPageId, rightNextPageId);
    assert(rightNextPageId != -1 || leftPrevPageId != -1);

    bool rotationSucceeded = true;
    db_page *leftPrevPage = nullptr, *rightNextPage = nullptr;

    if (leftPrevPageId != -1) {
        leftPrevPage = _loadPage(leftPrevPageId);
        int leftPrevMedianPos = (int)leftPrevPage->recordCount() - 1;
        if (leftPrevPage->willRemainMinimallyFilledWithout(leftPrevMedianPos)) {

            db_data_entry medianElement = _copyDataFromLoadedPage(leftPrevPage->record(leftPrevMedianPos));
            leftPrevPage->remove(leftPrevMedianPos);
            _unloadPage(leftPrevPage);

            page->insert(0, parentPage->record(lookupResult.parentRecord.inPagePosition));
            /*parentPage->replace(lookupResult.parentRecord.inPagePosition, medianElement,
                    parentPage->link(lookupResult.parentRecord.inPagePosition));*/
        } else {
            rotationSucceeded = false;
        }
    } else {     // we assume here that rightNextPageId != -1

        rightNextPage = _loadPage(leftPrevPageId);
        int rightNextMedianPos = 0;
        if (rightNextPage->willRemainMinimallyFilledWithout(rightNextMedianPos)) {

            db_data_entry medianElement = _copyDataFromLoadedPage(rightNextPage->record(rightNextMedianPos));
            rightNextPage->remove(rightNextMedianPos);
            _unloadPage(rightNextPage);

            page->insert((int)page->recordCount(), parentPage->record(lookupResult.parentRecord.inPagePosition));
            /*parentPage->replace(lookupResult.parentRecord.inPagePosition, medianElement,
                    parentPage->link(lookupResult.parentRecord.inPagePosition));*/
        } else {
            rotationSucceeded = false;
        }
    }


    if (!rotationSucceeded) {
        if (leftPrevPageId != -1) {
            throw 100;
        }
    }

    _fileStorage.writePage(parentPage);
    _fileStorage.writePage(page);
    if (leftPrevPageId  != -1)  _fileStorage.writePage(leftPrevPage);
    if (rightNextPageId != -1)  _fileStorage.writePage(rightNextPage);

    _unloadPage(leftPrevPage);
    _unloadPage(rightNextPage);
    _unloadPage(parentPage);
    _unloadPage(page);
}


void mydb_database::_removeFromNode(const mydb_database::record_lookup_result &lookupResult)
{
    throw 15;
}


db_page * mydb_database::_findNeighbours(const mydb_database::record_internal_id &parentRecord,
                                         int &leftPrevPageId, int &rightNextPageId)
{
    db_page *parentPage = _loadPage(parentRecord.pageId);

    if (parentRecord.inPagePosition != parentPage->recordCount()) {
        rightNextPageId = parentPage->link(parentRecord.inPagePosition + 1);
    }
    if (parentRecord.inPagePosition != 0) {
        leftPrevPageId = parentPage->link(parentRecord.inPagePosition - 1);
    }

    return parentPage;
}


string mydb_database::dumpSortedKeys() const
{
    std::ostringstream info;
    _dumpSortedKeys(info, _fileStorage.rootPageId());
    return info.str();
}


void mydb_database::_dumpSortedKeys(std::ostringstream &info, int pageId) const
{
    db_page *page = _loadPage(pageId);
    info << "\tpage #" << pageId <<  ": (has_links=" << page->hasLinks() << ")" << std::endl;

    for (auto elementIt = page->begin(); elementIt != page->end(); ++elementIt) {
        if (page->hasLinks()) {
            info << "\t[" << elementIt.link() << "] " << std::endl;
            _dumpSortedKeys(info, elementIt.link());
        }
        info << (*elementIt).toString() << std::endl;
        //info << "\t" << (*elementIt).toString() << " : " << elementIt.value().toString() << std::endl;
    }
    if (page->hasLinks()) {
        info << "\t[" << page->lastLink() << "] " << std::endl;
        _dumpSortedKeys(info, page->lastLink());
    }

    _unloadPage(page);
}
