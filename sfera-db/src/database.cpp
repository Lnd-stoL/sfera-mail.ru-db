
#include "database.hpp"
#include "syscall_checker.hpp"

#include <cassert>
#include <algorithm>
#include <cstring>
#include <sstream>

#include <sys/stat.h>
#include <iostream>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

auto database::createEmpty(const std::string &path, database_config const &config) -> database *
{
    struct stat dirstat = {};
    if (::stat(path.c_str(), &dirstat) != 0 || !S_ISDIR(dirstat.st_mode)) {
        syscall_check(::mkdir(path.c_str(), 0777));
    }

    db_data_storage_config dbStorageCfg;
    dbStorageCfg.maxStorageSize = config.maxDBSize;
    dbStorageCfg.pageSize = config.pageSizeBytes;
    dbStorageCfg.cacheSizeInPages = config.cacheSizePages;

    database *db = new database();
    db->_dataStorage = db_data_storage::createEmpty(path, dbStorageCfg);

    db_page *rootPage = db->_dataStorage->allocatePage(true);
    db->_dataStorage->changeRootPage(rootPage->id());
    rootPage->wasSaved(0); // TODO: dirty hack
    db->_dataStorage->releasePage(rootPage);

    return db;
}


auto database::openExisting(const std::string &path) -> database *
{
    database *db = new database();
    db->_dataStorage = db_data_storage::openExisting(path);
    db->_currentOperationId = db->_dataStorage->lastKnownOpId() + 1;

    return db;
}


void database::insert(data_blob key, data_blob value)
{
    db_operation operation(_currentOperationId++);
    _dataStorage->onOperationStart(&operation);

    _rKeyInsertionLookup(_dataStorage->rootPageId(), -1, key_value(key, value));

    _dataStorage->onOperationEnd();
}


database::~database()
{
    delete _dataStorage;
}


data_blob_copy database::get(data_blob key)
{
    db_operation operation(_currentOperationId++);
    _dataStorage->onOperationStart(&operation);

    auto result = _lookupByKey(key);

    _dataStorage->onOperationEnd();
    return result;
}


bool database::_binaryKeyComparer(data_blob key1, data_blob key2)
{
    int cr = memcmp(key1.dataPtr(), key2.dataPtr(), std::min(key1.length(), key2.length()));

    if (cr < 0) return true;
    if (cr == 0) return key1.length() < key2.length();
    return false;
}


bool database::_keysEqual(data_blob key1, data_blob key2)
{
    if (key1.length() != key2.length()) return false;
    return memcmp(key1.dataPtr(), key2.dataPtr(), key1.length()) == 0;
}


void database::_rKeyInsertionLookup(int pageId, int parentPageId, const key_value &element)
{
    db_page *page = _dataStorage->fetchPage(pageId);
    auto keyIt = std::lower_bound(page->keysBegin(), page->keysEnd(), element.key, _binaryKeyComparer);

    if (keyIt != page->keysEnd() && _keysEqual(element.key, *keyIt)) {    // something like value update
        if (page->canReplace(keyIt.position(), element.value)) {
            page->replace(keyIt.position(), element.value);
            _dataStorage->writePage(page);
            _dataStorage->releasePage(page);
        } else {
            _dataStorage->releasePage(page);
            throw std::runtime_error("Failed to update value"); // todo handle equal key error
        }

        return;
    }

    if (page->isFull()) {
        page = _splitPage(page, parentPageId, element);
        keyIt = std::lower_bound(page->keysBegin(), page->keysEnd(), element.key, _binaryKeyComparer);
    }

    if (!page->hasChildren()) {
        int nextPageId = page->id();
        _dataStorage->releasePage(page);
        _tryInsertInPage(nextPageId, parentPageId, element);
        return;
    }

    int nextPageId = keyIt.child();
    int parPageId = page->id();
    _dataStorage->releasePage(page);

    return _rKeyInsertionLookup(nextPageId, parPageId, element);
}


bool database::_rKeyErasingLookup(int pageId, int parentPageId, int parentRecordPos, const data_blob &elementKey)
{
    db_page *page = _dataStorage->fetchPage(pageId);
    auto keyIt = std::lower_bound(page->keysBegin(), page->keysEnd(), elementKey, _binaryKeyComparer);

    if (keyIt != page->keysEnd() && _keysEqual(elementKey, *keyIt)) {
        if (!page->hasChildren()) {
            return _removeFromLeaf(page, keyIt.position(), parentPageId, parentRecordPos);
        } else {
            return _removeFromNode(page, parentPageId, parentRecordPos, keyIt.position());
        }
    }

    if (!page->hasChildren()) {
        return false;
        //throw std::runtime_error("Deleting not existing element");
    }

    int nextPageId = keyIt.child();
    int nextParPageId = page->id();
    _dataStorage->releasePage(page);

    if (_rKeyErasingLookup(nextPageId, nextParPageId, keyIt.position(),
                           elementKey)) {    // we need to check the page
        return _testRebalanceAfterDelete(nextParPageId, parentPageId, parentRecordPos);
    }
    return false;
}


void database::_makeNewRoot(key_value element, int leftLink, int rightLink)
{
    db_page *newRootPage = _dataStorage->allocatePage(false);
    newRootPage->insert(0, element, leftLink);
    newRootPage->reconnect(1, rightLink);

    _dataStorage->changeRootPage(newRootPage->id());
    _dataStorage->writePage(newRootPage);
    _dataStorage->releasePage(newRootPage);
}


void database::_tryInsertInPage(int pageId, int parentPageId, const key_value &element)
{
    db_page *page = _dataStorage->fetchPage(pageId);
    assert(!page->hasChildren());

    if (!page->possibleToInsert(element)) {
        _dataStorage->releasePage(page);
        throw std::runtime_error("Impossible insert");
    }

    auto insertionIt = std::lower_bound(page->keysBegin(), page->keysEnd(), element.key, _binaryKeyComparer);
    page->insert(insertionIt, element);
    _dataStorage->writePage(page);
    _dataStorage->releasePage(page);
}


db_page *database::_splitPage(db_page *page, int parentPageId, const key_value &element)
{
    db_page *rightPage = _dataStorage->allocatePage(!page->hasChildren());
    key_value_copy medianElement = page->splitEquispace(rightPage);
    db_page *leftPage = page;

    if (parentPageId == -1) {
        _makeNewRoot(medianElement, leftPage->id(), rightPage->id());
    } else {

        db_page *parentPage = _dataStorage->fetchPage(parentPageId);
        auto insertionIt = std::lower_bound(parentPage->keysBegin(), parentPage->keysEnd(), medianElement.key,
                                            _binaryKeyComparer);
        parentPage->reconnect(insertionIt.position(), rightPage->id());
        parentPage->insert(insertionIt, medianElement, leftPage->id());
        _dataStorage->writePage(parentPage);
        _dataStorage->releasePage(parentPage);
    }

    _dataStorage->writePage(leftPage);
    _dataStorage->writePage(rightPage);

    bool keyWithMedianComparisonResult = _binaryKeyComparer(element.key, medianElement.key);
    medianElement.release();

    if (keyWithMedianComparisonResult) {
        _dataStorage->releasePage(rightPage);
        return leftPage;
    } else {
        _dataStorage->releasePage(leftPage);
        return rightPage;
    }
}


bool database::_makePageMinimallyFilled(db_page *page, int parentPageId, int parentRecordPos)
{
    int rightNextPageId = -1, leftPrevPageId = -1;
    db_page *parentPage = _findPageNeighbours(record_internal_id(parentPageId, parentRecordPos),
                                              leftPrevPageId, rightNextPageId);

    assert(rightNextPageId != -1 || leftPrevPageId != -1);
    db_page *rightNextPage = rightNextPageId == -1 ? nullptr : _dataStorage->fetchPage(rightNextPageId);
    db_page *leftPrevPage = leftPrevPageId == -1 ? nullptr : _dataStorage->fetchPage(leftPrevPageId);

    bool ret = false;
    bool rotationSucceeded = _tryTakeFromNearest(page, parentPage, parentRecordPos, leftPrevPage, rightNextPage);
    if (!rotationSucceeded) {    // so merge the nodes
        _mergePages(page, parentRecordPos, parentPage, rightNextPage, leftPrevPage);
        ret = true;
    }

    _dataStorage->writePage(parentPage);
    _dataStorage->writePage(page);

    _dataStorage->releasePage(parentPage);
    if (leftPrevPage != nullptr) _dataStorage->releasePage(leftPrevPage);
    if (rightNextPage != nullptr) _dataStorage->releasePage(rightNextPage);

    return ret;
}


void database::_mergePages(db_page *page, int parentRecordPos, db_page *parentPage, db_page *rightNextPage,
                           db_page *leftPrevPage)
{
    assert(page != rightNextPage && page != leftPrevPage);   // avoid self merging

    size_t basicResultSize = page->usedBytes() + parentPage->usedBytesFor(parentRecordPos);

    if (rightNextPage != nullptr && rightNextPage->usedBytes() + basicResultSize < page->size()) {
        int linked = -1;
        if (page->hasChildren()) linked = page->lastRightChild();
        page->append(parentPage->recordAt(parentRecordPos), linked);

        for (int i = 0; i < rightNextPage->recordCount(); ++i) {
            page->append(rightNextPage->recordAt(i), page->hasChildren() ? rightNextPage->childAt(i) : -1);
        }

        parentPage->remove(parentRecordPos);
        parentPage->reconnect(parentRecordPos, page->id());
        if (page->hasChildren()) page->reconnect((int) page->recordCount(), rightNextPage->lastRightChild());
        _dataStorage->deallocatePage(rightNextPage->id());

    } else if (leftPrevPage->usedBytes() + basicResultSize < page->size()) {

        int linked = -1;
        if (page->hasChildren()) linked = leftPrevPage->lastRightChild();
        page->insert(0, parentPage->recordAt(parentRecordPos - 1), linked);

        for (int i = (int) leftPrevPage->recordCount() - 1; i >= 0; --i) {
            page->insert(0, leftPrevPage->recordAt(i), page->hasChildren() ? leftPrevPage->childAt(i) : -1);
        }
        _dataStorage->deallocatePage(leftPrevPage->id());
        parentPage->remove(parentRecordPos - 1);
    } else {
        assert(0); // todo handle this situation properly (can't merge pages though they are both not minimally filled)
    }
}


string database::dumpTree() const
{
    std::ostringstream info;
    info << "dumping tree =========================================================================" << std::endl;

    _dump(info, _dataStorage->rootPageId());
    info << "======================================================================================" << std::endl;
    return info.str();
}


void database::_dump(std::ostringstream &info, int pageId) const
{
    db_page *page = _dataStorage->fetchPage(pageId);
    info << "page #" << pageId << ": (has_links=" << page->hasChildren() << "; is_full="
    << page->isFull() << "; is_minimally_filled=" << page->isMinimallyFilled() << ") " << std::endl;

    for (auto elementIt = page->keysBegin(); elementIt != page->keysEnd(); ++elementIt) {
        if (page->hasChildren()) info << "\t[" << elementIt.child() << "] " << std::endl;
        info << "\t" << (*elementIt).toString() << " : " << elementIt.value().toString() << std::endl;
    }
    if (page->hasChildren()) {
        info << "\t[" << page->lastRightChild() << "] " << std::endl;
    }

    info << std::endl;
    if (page->hasChildren()) {
        for (auto elementIt = page->keysBegin(); elementIt != page->keysEnd(); ++elementIt) {
            _dump(info, elementIt.child());
        }
        _dump(info, page->lastRightChild());
    }

    _dataStorage->releasePage(page);
}


void database::remove(data_blob key)
{
    db_operation operation(_currentOperationId++);
    _dataStorage->onOperationStart(&operation);

    _rKeyErasingLookup(_dataStorage->rootPageId(), -1, -1, key);

    _dataStorage->onOperationEnd();
}


bool database::_removeFromLeaf(db_page *page, int recPos, int parentPageId, int parentRecordPos)
{
    assert(!page->hasChildren());

    page->remove(recPos);
    if (page->isMinimallyFilled() ||
        page->id() == _dataStorage->rootPageId()
        /* root page doesn't have to be minimally filled */) {

        _dataStorage->writePage(page);
        _dataStorage->releasePage(page);
        return false;
    }

    bool ret = _makePageMinimallyFilled(page, parentPageId, parentRecordPos);
    _dataStorage->releasePage(page);
    return ret;
}


db_page *database::_findPageNeighbours(const record_internal_id &parentRecord, int &leftPrevPageId,
                                       int &rightNextPageId)
{
    db_page *parentPage = _dataStorage->fetchPage(parentRecord.pageId);

    if (parentRecord.inPagePosition != parentPage->recordCount()) {
        rightNextPageId = parentPage->childAt(parentRecord.inPagePosition + 1);
    }
    if (parentRecord.inPagePosition != 0) {
        leftPrevPageId = parentPage->childAt(parentRecord.inPagePosition - 1);
    }


    assert(rightNextPageId != leftPrevPageId);
    return parentPage;
}


string database::dumpSortedKeys() const
{
    std::ostringstream info;
    _rDumpSortedKeys(info, _dataStorage->rootPageId());
    return info.str();
}


void database::_rDumpSortedKeys(std::ostringstream &info, int pageId) const
{
    db_page *page = _dataStorage->fetchPage(pageId);
    info << "\tpage #" << pageId << ": (has_links=" << page->hasChildren() << ")" << std::endl;

    for (auto elementIt = page->keysBegin(); elementIt != page->keysEnd(); ++elementIt) {
        if (page->hasChildren()) {
            info << "\t[" << elementIt.child() << "] " << std::endl;
            _rDumpSortedKeys(info, elementIt.child());
        }
        info << (*elementIt).toString() << std::endl;
    }
    if (page->hasChildren()) {
        info << "\t[" << page->lastRightChild() << "] " << std::endl;
        _rDumpSortedKeys(info, page->lastRightChild());
    }

    _dataStorage->releasePage(page);
}


bool database::_tryTakeFromNearest(db_page *page, db_page *parentPage, int parentRecPos,
                                   db_page *leftPrevPage, db_page *rightNextPage)
{
    if (leftPrevPage != nullptr) {
        int leftPrevMedianPos = (int) leftPrevPage->recordCount() - 1;
        if (leftPrevPage->willRemainMinimallyFilledWithout(leftPrevMedianPos)) {

            key_value_copy medianElement(leftPrevPage->recordAt(leftPrevMedianPos));
            int leftLastLink = leftPrevPage->hasChildren() ? leftPrevPage->lastRightChild() : -1;
            int leftMedLink = leftPrevPage->hasChildren() ? leftPrevPage->childAt(leftPrevMedianPos) : -1;

            leftPrevPage->remove(leftPrevMedianPos);
            if (leftPrevPage->hasChildren())
                leftPrevPage->reconnect((int) leftPrevPage->recordCount(), leftMedLink);
            _dataStorage->writePage(leftPrevPage);

            page->insert(0, parentPage->recordAt(parentRecPos - 1), leftLastLink);
            parentPage->replace(parentRecPos - 1, medianElement,
                                parentPage->childAt(parentRecPos - 1));

            medianElement.release();
            return true;
        }
    } else {     // I assume here that rightNextPageId != -1

        int rightNextMedianPos = 0;
        if (rightNextPage->willRemainMinimallyFilledWithout(rightNextMedianPos)) {

            key_value_copy medianElement(rightNextPage->recordAt(rightNextMedianPos));
            int rightMedLink = rightNextPage->hasChildren() ? rightNextPage->childAt(rightNextMedianPos) : -1;
            rightNextPage->remove(rightNextMedianPos);
            _dataStorage->writePage(rightNextPage);

            page->insert((int) page->recordCount(), parentPage->recordAt(parentRecPos),
                         page->hasChildren() ? page->lastRightChild() : -1);
            if (page->hasChildren()) page->reconnect((int) page->recordCount(), rightMedLink);
            parentPage->replace(parentRecPos, medianElement,
                                parentPage->childAt(parentRecPos));

            medianElement.release();
            return true;
        }
    }

    return false;
}


void database::_checkAndRemoveEmptyRoot()
{
    db_page *rootPage = _dataStorage->fetchPage(_dataStorage->rootPageId());
    assert(rootPage->hasChildren());
    if (rootPage->recordCount() > 0) {
        _dataStorage->releasePage(rootPage);
        return;
    }

    int actualRootId = rootPage->lastRightChild();
    _dataStorage->deallocateAndRelease(rootPage);
    _dataStorage->changeRootPage(actualRootId);
}


bool database::_removeFromNode(db_page *nodePage, int parentPageId, int parentRecPos, int recPos)
{
    key_value_copy mostLeftElement;
    int nextPageId = nodePage->childAt(recPos + 1);
    int nextParPageId = nodePage->id();
    _dataStorage->releasePage(nodePage);

    _rRemoveFromNodeR(nextPageId, nextParPageId, recPos + 1, mostLeftElement, false);

    nodePage = _dataStorage->fetchPage(nextParPageId);
    nodePage->replace(recPos, mostLeftElement, nodePage->childAt(recPos));
    _dataStorage->writePage(nodePage);
    _dataStorage->releasePage(nodePage);
    mostLeftElement.release();

    if (_testRebalanceAfterDelete(nextPageId, nextParPageId, recPos + 1)) {
        return _testRebalanceAfterDelete(nextParPageId, parentPageId, parentRecPos);
    }

    return false;
}


bool database::_rRemoveFromNodeR(int pageId, int parentPageId, int parentRecPos,
                                 key_value_copy &element, bool canRebalance)
{
    db_page *page = _dataStorage->fetchPage(pageId);
    auto keyIt = page->keysBegin();

    if (!page->hasChildren()) {
        element = page->recordAt(keyIt.position());
        page->remove(keyIt.position());
        if (page->isMinimallyFilled()) {
            _dataStorage->writePage(page);
            _dataStorage->releasePage(page);
            return false;
        }

        if (canRebalance) {
            bool ret = _makePageMinimallyFilled(page, parentPageId, parentRecPos);
            _dataStorage->releasePage(page);
            return ret;
        } else {
            _dataStorage->writePage(page);
            _dataStorage->releasePage(page);
            return false;
        }
    }

    int nextPageId = keyIt.child();
    int nextParPageId = page->id();
    _dataStorage->releasePage(page);

    if (_rRemoveFromNodeR(nextPageId, nextParPageId, keyIt.position(), element, true)) {
        if (canRebalance) return _testRebalanceAfterDelete(pageId, parentPageId, parentRecPos);
    }
    return false;
}


bool database::_testRebalanceAfterDelete(int pageId, int parentPageId, int parentRecPos)
{
    if (pageId != _dataStorage->rootPageId()) {
        db_page *page = _dataStorage->fetchPage(pageId);
        bool ret = false;
        if (!page->isMinimallyFilled()) {
            ret = _makePageMinimallyFilled(page, parentPageId, parentRecPos);
        }
        _dataStorage->releasePage(page);
        return ret;

    } else _checkAndRemoveEmptyRoot();

    return false;
}


data_blob_copy database::_lookupByKey(data_blob key)
{
    int nextPageId = _dataStorage->rootPageId();

    while (true) {
        db_page *page = _dataStorage->fetchPage(nextPageId);
        auto keyIt = std::lower_bound(page->keysBegin(), page->keysEnd(), key, _binaryKeyComparer);

        if (keyIt == page->keysEnd()) {    // key not found
            if (!page->hasChildren()) {
                _dataStorage->releasePage(page);
                return data_blob_copy();
            }
        } else {

            if (_keysEqual(key, *keyIt)) {
                _dataStorage->releasePage(page);
                return data_blob_copy(keyIt.value());
            } else {
                if (!page->hasChildren()) {   // also not found
                    _dataStorage->releasePage(page);
                    return data_blob_copy();
                }
            }
        }

        nextPageId = keyIt.child();
        _dataStorage->releasePage(page);
    }
}


string database::dumpCacheStatistics() const
{
    std::ostringstream str;
    auto cacheStatistics = _dataStorage->pagesCache().statictics();

    str << "fetches: " << cacheStatistics.fetchesCount << std::endl;
    str << "misses: " << cacheStatistics.missesCount << std::endl;
    str << "evictions: " << cacheStatistics.ecivtionsCount << std::endl;
    str << "failed evictions: " << cacheStatistics.failedEvictions << std::endl;

    return str.str();
}


bool database::exists(const std::string &path)
{
    return db_data_storage::exists(path);
}

//----------------------------------------------------------------------------------------------------------------------
}