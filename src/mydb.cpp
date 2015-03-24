
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
    auto keyIt = std::lower_bound(page->begin(), page->end(), element.key(), _binaryKeyComparer);

    if (keyIt != page->end() && _keysEqual(element.key(), *keyIt)) {    // something like value update
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

    if (page->isFull()) {
        page = _splitPage(page, parentPageId, element);
        keyIt = std::lower_bound(page->begin(), page->end(), element.key(), _binaryKeyComparer);
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


bool mydb_database::_rKeyErasingLookup(int pageId, int parentPageId, int parentRecordPos, const binary_data &elementKey)
{
    db_page *page = _loadPage(pageId);
    auto keyIt = std::lower_bound(page->begin(), page->end(), elementKey, _binaryKeyComparer);

    if (keyIt != page->end() && _keysEqual(elementKey, *keyIt)) {
        if (!page->hasLinks()) {
            return _removeFromLeaf(page, keyIt.position(), parentPageId, parentRecordPos);
        } else {
            return _removeFromNode(page, parentPageId, parentRecordPos, keyIt.position());
        }
    }

    if (!page->hasLinks()) {
        throw std::runtime_error("Deleting not existing element");
    }

    int nextPageId = keyIt.link();
    int nextParPageId = page->index();
    _unloadPage(page);

    if (_rKeyErasingLookup(nextPageId, nextParPageId, keyIt.position(), elementKey)) {    // we need to check the page
        return _testRebalanceAfterDelete(nextParPageId, parentPageId, parentRecordPos);
    }
    return false;
}


void mydb_database::_newRoot(db_data_entry element, int leftLink, int rightLink)
{
    db_page *rootPage = _fileStorage.allocLoadPage(true);
    rootPage->insert(0, element, leftLink);
    rootPage->relink(1, rightLink);

    _fileStorage.writePage(rootPage);
    _fileStorage.changeRootPage(rootPage->index());
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


bool mydb_database::_makePageMinimallyFilled(db_page *page, int parentPageId, int parentRecordPos)
{
    int rightNextPageId = -1, leftPrevPageId = -1;
    db_page *parentPage = _findNeighbours(record_internal_id(parentPageId, parentRecordPos),
            leftPrevPageId, rightNextPageId);

    assert(rightNextPageId != -1 || leftPrevPageId != -1);
    db_page *rightNextPage = rightNextPageId == -1 ? nullptr : _loadPage(rightNextPageId);
    db_page *leftPrevPage  = leftPrevPageId  == -1 ? nullptr : _loadPage(leftPrevPageId);

    bool ret = false;
    bool rotationSucceeded = _tryTakeFromNearest(page, parentPage, parentRecordPos, leftPrevPage, rightNextPage);
    if (!rotationSucceeded) {    // so merge the nodes
        _mergePages(page, parentRecordPos, parentPage, rightNextPage, leftPrevPage);
        ret = true;
    }

    _fileStorage.writePage(parentPage);
    _fileStorage.writePage(page);
    _unloadPage(leftPrevPage);
    _unloadPage(rightNextPage);
    _unloadPage(parentPage);

    return ret;
}


void mydb_database::_mergePages(db_page *page, int parentRecordPos, db_page *parentPage, db_page *rightNextPage,
                                db_page *leftPrevPage)
{
    size_t basicResultSize = page->usedBytes() + parentPage->usedBytesFor(parentRecordPos);

    if (rightNextPage != nullptr && rightNextPage->usedBytes() + basicResultSize < page->size()) {
        int linked = -1;
        if (page->hasLinks())  linked = page->lastLink();
        page->append(parentPage->record(parentRecordPos), linked);

        for (int i = 0; i < rightNextPage->recordCount(); ++i) {
            page->append(rightNextPage->record(i), page->hasLinks() ? rightNextPage->link(i) : -1);
        }
        parentPage->relink(parentRecordPos+1, page->index());
        if (page->hasLinks()) page->relink((int)page->recordCount(), rightNextPage->lastLink());
        _fileStorage.freePage(rightNextPage);
        parentPage->remove(parentRecordPos);

    } else if (leftPrevPage->usedBytes() + basicResultSize < page->size()) {

        int linked = -1;
        if (page->hasLinks())  linked = leftPrevPage->lastLink();
        page->insert(0, parentPage->record(parentRecordPos-1), linked);

        for (int i = (int)leftPrevPage->recordCount()-1; i >= 0; --i) {
            page->insert(0, leftPrevPage->record(i), page->hasLinks() ? leftPrevPage->link(i) : -1);
        }
        _fileStorage.freePage(leftPrevPage);
        parentPage->remove(parentRecordPos-1);
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
        << page->isFull()  << "; is_minimally_filled=" << page->isMinimallyFilled()  << ") " << std::endl;

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
    _rKeyErasingLookup(_fileStorage.rootPageId(), -1, -1, key);
}


bool mydb_database::_removeFromLeaf(db_page *page, int recPos, int parentPageId, int parentRecordPos)
{
    assert(!page->hasLinks());

    page->remove(recPos);
    if (page->isMinimallyFilled()) {
        _fileStorage.writePage(page);
        _unloadPage(page);
        return false;
    }

    bool ret = _makePageMinimallyFilled(page, parentPageId, parentRecordPos);
    _unloadPage(page);
    return ret;
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
    }
    if (page->hasLinks()) {
        info << "\t[" << page->lastLink() << "] " << std::endl;
        _dumpSortedKeys(info, page->lastLink());
    }

    _unloadPage(page);
}


bool mydb_database::_tryTakeFromNearest(db_page *page, db_page *parentPage, int parentRecPos,
                                        db_page *leftPrevPage, db_page *rightNextPage)
{
    if (leftPrevPage != nullptr) {
        int leftPrevMedianPos = (int)leftPrevPage->recordCount() - 1;
        if (leftPrevPage->willRemainMinimallyFilledWithout(leftPrevMedianPos)) {

            db_data_entry medianElement = _copyDataFromLoadedPage(leftPrevPage->record(leftPrevMedianPos));
            int leftLastLink = leftPrevPage->hasLinks() ? leftPrevPage->lastLink() : -1;
            int leftMedLink = leftPrevPage->hasLinks() ? leftPrevPage->link(leftPrevMedianPos) : -1;

            leftPrevPage->remove(leftPrevMedianPos);
            if (leftPrevPage->hasLinks()) leftPrevPage->relink((int)leftPrevPage->recordCount(), leftMedLink);
            _fileStorage.writePage(leftPrevPage);

            page->insert(0, parentPage->record(parentRecPos-1), leftLastLink);
            parentPage->replace(parentRecPos-1, medianElement,
                    parentPage->link(parentRecPos-1));

            medianElement.free();
            return true;
        }
    } else {     // I assume here that rightNextPageId != -1

        int rightNextMedianPos = 0;
        if (rightNextPage->willRemainMinimallyFilledWithout(rightNextMedianPos)) {

            db_data_entry medianElement = _copyDataFromLoadedPage(rightNextPage->record(rightNextMedianPos));
            int rightMedLink = rightNextPage->hasLinks() ? rightNextPage->link(rightNextMedianPos) : -1;
            rightNextPage->remove(rightNextMedianPos);
            _fileStorage.writePage(rightNextPage);

            page->insert((int)page->recordCount(), parentPage->record(parentRecPos), page->hasLinks() ? page->lastLink() : -1);
            if (page->hasLinks()) page->relink((int)page->recordCount(), rightMedLink);
            parentPage->replace(parentRecPos, medianElement,
                    parentPage->link(parentRecPos));

            medianElement.free();
            return true;
        }
    }

    return false;
}


void mydb_database::_removeEmptyRoot()
{
    assert(_rootPage->hasLinks());
    if (_rootPage->recordCount() > 0) return;

    _fileStorage.changeRootPage(_rootPage->lastLink());
    _fileStorage.freePage(_rootPage);
    delete _rootPage;
    _rootPage = _fileStorage.loadPage(_fileStorage.rootPageId());
}


bool mydb_database::_removeFromNode(db_page *nodePage, int parentPageId, int parentRecPos, int recPos)
{
    db_data_entry mostLeftElement;
    int nextPageId = nodePage->link(recPos+1);
    int nextParPageId = nodePage->index();
    _unloadPage(nodePage);

    _rRemoveFromNodeR(nextPageId, nextParPageId, recPos+1, mostLeftElement, false);

    nodePage = _loadPage(nextParPageId);
    nodePage->replace(recPos, mostLeftElement, nodePage->link(recPos));
    _fileStorage.writePage(nodePage);
    _unloadPage(nodePage);
    mostLeftElement.free();

    if (_testRebalanceAfterDelete(nextPageId, nextParPageId, recPos+1)) {
        return  _testRebalanceAfterDelete(nextParPageId, parentPageId, parentRecPos);
    }
}


bool mydb_database::_rRemoveFromNodeR(int pageId, int parentPageId, int parentRecPos,
                                      db_data_entry &element, bool canRebalance)
{
    db_page *page = _loadPage(pageId);
    auto keyIt = page->begin();

    if (!page->hasLinks()) {
        element = _copyDataFromLoadedPage(page->record(keyIt.position()));
        page->remove(keyIt.position());
        if (page->isMinimallyFilled()) {
            _fileStorage.writePage(page);
            _unloadPage(page);
            return false;
        }

        if (canRebalance) {
            bool ret = _makePageMinimallyFilled(page, parentPageId, parentRecPos);
            _unloadPage(page);
            return ret;
        } else {
            _fileStorage.writePage(page);
            _unloadPage(page);
            return false;
        }
    }

    int nextPageId = keyIt.link();
    int nextParPageId = page->index();
    _unloadPage(page);

    if (_rRemoveFromNodeR(nextPageId, nextParPageId, keyIt.position(), element, true)) {
        if (canRebalance) return _testRebalanceAfterDelete(pageId, parentPageId, parentRecPos);
    }
    return false;
}


bool mydb_database::_testRebalanceAfterDelete(int pageId, int parentPageId, int parentRecPos)
{
    if (pageId != _fileStorage.rootPageId()) {
        db_page *page = _loadPage(pageId);
        bool ret = false;
        if (!page->isMinimallyFilled()) {
            ret = _makePageMinimallyFilled(page, parentPageId, parentRecPos);
        }
        _unloadPage(page);
        return ret;

    } else _removeEmptyRoot();
    return false;
}
