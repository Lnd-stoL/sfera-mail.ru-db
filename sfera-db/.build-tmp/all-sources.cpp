
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
    _dataStorage->writeAndRelease(page);
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
#include "db_binlog_logger.hpp"
#include "db_stable_storage_file.hpp"

#include <iostream>
#include <cassert>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

binlog_record::binlog_record(type_t t, uint64_t lsn) :
        _type(t),
        _lsn(lsn)
{
    _length = _headerSize + sizeof(_length);
}


void binlog_record::_fillHeader(uint8_t *header)
{
    uint8_t *headerWriter = header;

    *(uint32_t *) headerWriter = magicHeader;
    headerWriter += sizeof(magicHeader);
    *(uint32_t *) headerWriter = _length;
    headerWriter += sizeof(_length);
    *(uint64_t *) headerWriter = _lsn;
    headerWriter += sizeof(_lsn);
    *(type_t *) headerWriter = _type;
    headerWriter += sizeof(_type);

    assert(headerWriter - header == _headerSize);
}


void binlog_record::_unpackHeader(uint8_t *header)
{
    uint8_t *headerReader = header;

    uint32_t magic = *(uint32_t *) headerReader;
    headerReader += sizeof(magicHeader);
    if (magic != magicHeader) {
        _type = UNKNOWN;
        return;
    }

    _length = *(uint32_t *) headerReader;
    headerReader += sizeof(_length);
    _lsn = *(uint64_t *) headerReader;
    headerReader += sizeof(_lsn);
    _type = *(type_t *) headerReader;
    headerReader += sizeof(_type);

    assert(headerReader - header == _headerSize);
}


void binlog_record::writeTo(raw_file *file)
{
    uint8_t header[_headerSize];
    _fillHeader(header);

    std::pair<const void *, size_t> buffers[] = {
            {header,   sizeof(header)},
            {&_length, sizeof(_length)}
    };

    file->appedAll(buffers, 2);
}


bool binlog_record::readFrom(raw_file *file)
{
    uint8_t header[_headerSize];
    file->readAll(header, _headerSize);
    _unpackHeader(header);

    if (_type == UNKNOWN) {
        assert(0);
        return false;
    }

    ::lseek(file->unixFD(), sizeof(_length), SEEK_CUR);  // this is to read the whole record
    return true;
}


binlog_record::type_t
binlog_record::fetchType(raw_file *file)
{
    ::lseek(file->unixFD(), _typeOffset(), SEEK_CUR);
    type_t recType;
    file->readAll(&recType, sizeof(recType));
    ::lseek(file->unixFD(), -_typeOffset() - sizeof(recType), SEEK_CUR);

    return recType;
}

//----------------------------------------------------------------------------------------------------------------------

binlog_operation_record::binlog_operation_record(binlog_record::type_t t, uint64_t lsn, db_operation *op) :
        binlog_record(t, lsn),
        _operation(op)
{
    auto pagesCount = _operation->pagesWriteSet().size();
    _length += sizeof(uint32_t) /* pagesCount */ +
               (_operation->pagesWriteSet().begin()->second->size() + sizeof(int)) * pagesCount;
}


void binlog_operation_record::writeTo(raw_file *file)
{
    uint8_t header[_headerSize];
    _fillHeader(header);

    auto pages = _operation->pagesWriteSet();
    uint32_t pagesCount = (uint32_t) pages.size();
    uint32_t pagesIds[pagesCount];

    uint32_t buffersCount = (uint32_t) pages.size() + 4;
    auto buffers = new std::pair<const void *, size_t>[buffersCount];
    buffers[0] = {header, sizeof(header)};
    buffers[1] = {&pagesCount, sizeof(pagesCount)};
    buffers[2] = {pagesIds, sizeof(pagesIds)};

    auto pagesIt = pages.cbegin();
    for (int i = 0; i < pages.size(); ++i, ++pagesIt) {
        pagesIt->second->prepareForWriting();
        buffers[i + 3] = {pagesIt->second->bytes(), pagesIt->second->size()};
        pagesIds[i] = pagesIt->second->id();
    }
    buffers[buffersCount - 1] = {&_length, sizeof(_length)};

    file->appedAll(buffers, buffersCount);
    delete buffers;
}


bool binlog_operation_record::readFrom(raw_file *file)
{
    if (!binlog_record::readFrom(file)) return false;
    ::lseek(file->unixFD(), -sizeof(_length), SEEK_CUR);  // this is to undo last length skipping

    uint32_t pageCount = 0;
    file->readAll(&pageCount, sizeof(pageCount));

    uint32_t pagesIds[pageCount];
    file->readAll(pagesIds, sizeof(pagesIds));

    uint32_t pageSize = (uint32_t) (_length - _headerSize - sizeof(_length) - sizeof(pageCount) - sizeof(pagesIds));
    pageSize /= pageCount;

    for (int i = 0; i < pageCount; ++i) {

        uint8_t *pageBytes = (uint8_t *) ::malloc(pageSize);
        file->readAll(pageBytes, pageSize);
        db_page *nextPage = db_page::load(pagesIds[i], data_blob(pageBytes, pageSize));
        _operation->writesPage(nextPage);
    }

    ::lseek(file->unixFD(), sizeof(_length), SEEK_CUR);  // skip the length finally
    return true;
}

//----------------------------------------------------------------------------------------------------------------------

db_binlog_logger *db_binlog_logger::createEmpty(const std::string &path)
{
    db_binlog_logger *binlog = new db_binlog_logger();
    binlog->_file = raw_file::createNew(path);
    binlog->logCheckpoint();

    return binlog;
}


db_binlog_logger *db_binlog_logger::openExisting(const std::string &path, const db_binlog_recovery& recoveryTool)
{
    db_binlog_logger *binlog = new db_binlog_logger();
    binlog->_file = raw_file::openExisting(path);

    if (!recoveryTool.closedProperly()) {
        throw std::runtime_error("can't open binlog before it is repaired");
    }

    binlog->_currentLSN = recoveryTool.lastLsn() + 1;
    binlog->logCheckpoint();

    return binlog;
}


void db_binlog_logger::_writeNextRecord(binlog_record &rec)
{
    rec.writeTo(_file);
    ++_currentLSN;
}


db_binlog_logger::~db_binlog_logger()
{
    binlog_record opClose(binlog_record::LOG_CLOSED, _currentLSN);
    _writeNextRecord(opClose);
}


void db_binlog_logger::logOperation(db_operation *operation)
{
    binlog_operation_record rec(binlog_record::OPERATION, _currentLSN, operation);
    _writeNextRecord(rec);
}


void db_binlog_logger::logCheckpoint()
{
    binlog_record checkpointRec(binlog_record::CHECKPOINT, _currentLSN);
    _writeNextRecord(checkpointRec);
}

//----------------------------------------------------------------------------------------------------------------------

db_binlog_recovery::db_binlog_recovery(const std::string &path)
{
    if (!raw_file::exists(path)) {
        _file = nullptr;
        return;
    }

    _file = raw_file::openExisting(path, true);
    ::lseek(_file->unixFD(), -sizeof(uint32_t), SEEK_END);
    uint32_t lastMsgLen = 0;
    _file->readAll(&lastMsgLen, sizeof(lastMsgLen));
    ::lseek(_file->unixFD(), -((off_t) lastMsgLen), SEEK_CUR);

    binlog_record lastRecord;
    if (!lastRecord.readFrom(_file)) {
        return;
    }

    if (lastRecord.type() == binlog_record::LOG_CLOSED) {
        _closedProperly = true;
        _lastLsn = lastRecord.lsn();
    }
}


db_binlog_recovery::~db_binlog_recovery()
{
    releaseLog();
}


void db_binlog_recovery::doRecovery(db_stable_storage_file *stableStorage)
{
    _findBackCheckpoint();

    while (!_file->eof()) {
        binlog_record::type_t nextRecordType = binlog_record::fetchType(_file);
        if (_file->eof()) break;
        if (nextRecordType == binlog_record::LOG_CLOSED) break;
        assert(nextRecordType == binlog_record::OPERATION);

        db_operation nextOperation(0);
        binlog_operation_record nextOperationRec(&nextOperation);
        nextOperationRec.readFrom(_file);

        auto pagesSet = nextOperation.pagesWriteSet();
        for (auto nextPagePair : pagesSet) {
            db_page *nextPage = nextPagePair.second;
            db_page *stablePageVersion = stableStorage->loadPage(nextPage->id());

            if (stablePageVersion == nullptr ||
                stablePageVersion->lastModifiedOpId() < nextPage->lastModifiedOpId()) {

                std::cerr << "[recovery] page #" << nextPage->id() << " fixed from " <<
                (stablePageVersion == nullptr ? 0 : stablePageVersion->lastModifiedOpId()) <<
                " to " << nextPage->lastModifiedOpId() << std::endl;

                stableStorage->writePage(nextPage);  // replace the page in stable storage
                _lastOpId = std::max(_lastOpId, nextPage->lastModifiedOpId());
            }

            delete nextPage;
            delete stablePageVersion;
        }

        _lastLsn = nextOperationRec.lsn();
    }

    _closedProperly = true;
}


void db_binlog_recovery::_findBackCheckpoint()
{
    ::lseek(_file->unixFD(), 0, SEEK_END);

    while (::lseek(_file->unixFD(), -sizeof(uint32_t), SEEK_CUR) != -1) {
        uint32_t msgLen = 0;
        _file->readAll(&msgLen, sizeof(msgLen));
        off_t msgBegin = ::lseek(_file->unixFD(), -(off_t) msgLen, SEEK_CUR);

        binlog_record record;
        if (!record.readFrom(_file)) {
            _findBackLastValidRecord();      // in case if last log message was damaged
            record.readFrom(_file);
            return;
        }

        if (record.type() == binlog_record::CHECKPOINT) {
            break;
        }

        ::lseek(_file->unixFD(), msgBegin, SEEK_SET);
    }
}


void db_binlog_recovery::_findBackLastValidRecord()
{
    const size_t readBufferSize = 512 * sizeof(uint32_t);
    off_t currentPos = ::lseek(_file->unixFD(), 0, SEEK_END);

    while ((currentPos = ::lseek(_file->unixFD(), -readBufferSize, SEEK_CUR)) >= 0) {
        uint8_t readBuffer[readBufferSize];
        _file->readAll(readBuffer, readBufferSize);

        uint32_t *readBufferPtr = (uint32_t *)readBuffer + (readBufferSize/sizeof(uint32_t)) - 1;
        while (readBufferPtr >= (uint32_t *)readBuffer) {
            for (; *readBufferPtr != binlog_record::magicHeader && readBufferPtr >= (uint32_t *)readBuffer; --readBufferPtr);

            if (*readBufferPtr == binlog_record::magicHeader) {
                uint32_t msgLength = 0;
                off_t currentMagicPos = currentPos + (uint8_t *)readBufferPtr - readBuffer;
                _file->readAll(currentMagicPos + sizeof(binlog_record::magicHeader), &msgLength, sizeof(msgLength));
                //todo: test EOF

                uint32_t doubleCheckLen = 0;
                _file->readAll(currentPos + msgLength - sizeof(msgLength), &doubleCheckLen, sizeof(doubleCheckLen));
                if (doubleCheckLen == msgLength) {
                    ::lseek(_file->unixFD(), currentMagicPos, SEEK_SET);
                    return;
                }
            }
        }
    }
}


void db_binlog_recovery::releaseLog()
{
    if (_file != nullptr) {
        delete _file;
        _file = nullptr;
    }
}

//----------------------------------------------------------------------------------------------------------------------
}

#include "db_containers.hpp"

#include <stdlib.h>
#include <string.h>

//----------------------------------------------------------------------------------------------------------------------

sfera_db::data_blob::data_blob(uint8_t *dataPtr, size_t length) : _length (length), _dataPtr(dataPtr)
{ }


std::string sfera_db::data_blob::toString() const
{
    std::string str(_length, ' ');
    std::copy(this->dataPtr(), this->dataEndPtr(), str.begin());

    return str;
}


sfera_db::data_blob sfera_db::data_blob::fromCopyOf(const std::string &str)
{
    char *strCopy = strdup(str.c_str());
    return data_blob((uint8_t *)strCopy, str.length());
}


sfera_db::data_blob_copy::data_blob_copy(sfera_db::data_blob src)
{
    _length = src.length();
    _dataPtr = (uint8_t *)malloc(src.length());
    std::copy(src.dataPtr(), src.dataEndPtr(), _dataPtr);
}


void sfera_db::data_blob_copy::release()
{
    free(_dataPtr);
}


sfera_db::key_value_copy::key_value_copy(sfera_db::key_value const &src) : key(src.key), value(src.value)
{
}


void sfera_db::key_value_copy::release()
{
    key.release();
    value.release();
}

#include "db_data_storage.hpp"
#include "syscall_checker.hpp"

#include <iostream>
#include <cassert>
#include <limits>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

const std::string db_data_storage::StableStorageFileName = "data.sdbs";
const std::string db_data_storage::LogFileName           = "log.sdbl";

//----------------------------------------------------------------------------------------------------------------------

auto db_data_storage::openExisting(const std::string &dirPath, const db_data_storage_open_params &params)
-> db_data_storage *
{
    auto dbDataStorage = new db_data_storage();
    dbDataStorage->_stableStorageFile = db_stable_storage_file::openExisting(dirPath + "/" + StableStorageFileName);

    db_binlog_recovery binlogRecovery(dirPath + "/" + dbDataStorage->LogFileName);
    if (!binlogRecovery.closedProperly()) {
        std::cerr << "warning: database wasn't closed peoperly last time -> applying recovery ..." << std::endl;
        binlogRecovery.doRecovery(dbDataStorage->_stableStorageFile);
        std::cerr << "recovery completed" << std::endl;
    }
    binlogRecovery.releaseLog();

    dbDataStorage->_lastKnownOpId = binlogRecovery.lastOpId();
    dbDataStorage->_initializeCache(params.cacheSizeInPages);
    dbDataStorage->_binlog = db_binlog_logger::openExisting(dirPath + "/" + dbDataStorage->LogFileName, binlogRecovery);

    return dbDataStorage;
}


auto db_data_storage::createEmpty(const std::string &dirPath, db_data_storage_config const &config) -> db_data_storage *
{
    auto dbDataStorage = new db_data_storage();
    dbDataStorage->_stableStorageFile = db_stable_storage_file::createEmpty(dirPath + "/" +
                                                                            dbDataStorage->StableStorageFileName,
                                                                            config);
    dbDataStorage->_initializeCache(config.cacheSizeInPages);
    dbDataStorage->_binlog = db_binlog_logger::createEmpty(dirPath + "/" + dbDataStorage->LogFileName);

    return dbDataStorage;
}


db_data_storage::~db_data_storage()
{
    _pagesCache->clearCache();

    delete _pagesCache;
    delete _stableStorageFile;
    delete _binlog;
}


void db_data_storage::writePage(db_page *page)
{
    assert( page != nullptr );
    assert( page->wasChanged() );

    if (!_currentOperation->writesPage(page)) {     // instead immidiate writing add the page to the current operation's write set
        _pagesCache->pin(page);                     // because of no steal
        _pagesCache->makeDirty(page);
    }

    page->wasSaved(_currentOperation->id());
}


db_page* db_data_storage::allocatePage(bool isLeaf)
{
    db_page *page = _stableStorageFile->allocatePage(isLeaf);
    _pagesCache->cacheAndPin(page);

    return page;
}


void db_data_storage::deallocatePage(int pageId)
{
    _stableStorageFile->deallocatePage(pageId);
    if (_currentOperation != nullptr) _currentOperation->invalidatePage(pageId);
    _pagesCache->invalidateCachedPage(pageId);
}


db_page* db_data_storage::fetchPage(int pageId)
{
    db_page *cachedVersion = _pagesCache->fetchAndPin(pageId);
    if (cachedVersion == nullptr) {
        cachedVersion = _stableStorageFile->loadPage(pageId);
        _pagesCache->cacheAndPin(cachedVersion);
    }

    return cachedVersion;
}


void db_data_storage::changeRootPage(int pageId)
{
    _stableStorageFile->changeRootPage(pageId);
}


void db_data_storage::releasePage(db_page *page)
{
    assert( page != nullptr );
    assert( !page->wasChanged() );  // normally the page has to be written to disk before releasing if it has been changed

    //_pagesCache->unpinIfClean(page);        // do nothing on dirty pages here because of no steal
    _pagesCache->unpin(page);
}


void db_data_storage::deallocateAndRelease(db_page *page)
{
    this->deallocatePage(page->id());
    this->releasePage(page);
}


void db_data_storage::writeAndRelease(db_page *page)
{
    this->writePage(page);
    this->releasePage(page);
}


void db_data_storage::_initializeCache(size_t sizeInPages)
{
    _pagesCache = new pages_cache(sizeInPages,
                                 [this](db_page *page) {
                                     _stableStorageFile->writePage(page);
                                 });
}


void db_data_storage::onOperationStart(db_operation *op)
{
    _currentOperation = op;
}


void db_data_storage::onOperationEnd()
{
    if (!_currentOperation->isReadOnly()) {
        auto activeWriteSet = _currentOperation->pagesWriteSet();
        for (auto pageWritten : activeWriteSet) {
            _pagesCache->unpin(pageWritten.second);
        }

        _binlog->logOperation(_currentOperation);
    }

    _currentOperation = nullptr;
}


bool db_data_storage::exists(const std::string &path)
{
    return raw_file::exists(path + "/" + StableStorageFileName);
}

//----------------------------------------------------------------------------------------------------------------------
}
#include "db_operation.hpp"
#include <cassert>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

bool db_operation::writesPage(db_page *page)
{
    auto pageIt = _pagesWriteSet.find(page->id());
    if (pageIt == _pagesWriteSet.end()) {
        _pagesWriteSet.insert(std::make_pair(page->id(), page));
        return false;
    }

    assert( pageIt->second == page );
    return true;
}


void db_operation::invalidatePage(int pageId)
{
    auto pageIt = _pagesWriteSet.find(pageId);
    if (pageIt == _pagesWriteSet.end()) return;

    _pagesWriteSet.erase(pageIt);
}


bool db_operation::isReadOnly()
{
    return _pagesWriteSet.empty();
}

//----------------------------------------------------------------------------------------------------------------------
}
//----------------------------------------------------------------------------------------------------------------------
// page layout brief description
//----------------------------------------------------------------------------------------------------------------------
//
//  offset |  size  |  field meaning
//  --------------------------------------------------------------------------------------------------------------------
//  0       | uint64 | last operation id that modified the page
//  8       | uint16 | record count the page contains (record = key+valueAt entry and a childAt to the child btree node)
//  10      | uint16 | data_block_end (the data contains of keys and values BLOBs and is placed to the end of the page)
//  12      | byte   | meta information (actually a byte indicating if the page is btree leaf [0] or not [1])
//  13      | ------ | data entry index blocks:
//                      block consists of three integers ( uint16 ) + one 32-bit integer
//                          at 0 - key_value blob offset within the page
//                          at 2 - key length in bytes
//                          at 4 - valueAt length in bytes
//                          at 6 - [if not a leaf] ID of a page which is a btree child node coming BEFORE the key
//   ===== FREE SPACE =====
//   ===== ACTUAL VALUES AND KEYS BINARY DATA ===== - from data_block_end to the end of the page
//
//----------------------------------------------------------------------------------------------------------------------

#include "db_page.hpp"

#include <cassert>
#include <algorithm>
#include <stdlib.h>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

db_page::key_iterator::key_iterator(const db_page *page, int position) : _page (page), _position (position)
{ }


bool db_page::key_iterator::operator!=(const db_page::key_iterator &rhs) const
{
    assert(_page == rhs._page);
    return _position != rhs._position;
}


bool db_page::key_iterator::operator==(const db_page::key_iterator &rhs) const
{
    return !(this->operator!=(rhs));
}


db_page::key_iterator&
db_page::key_iterator::operator=(const db_page::key_iterator &rhs)
{
    _page = rhs._page;
    _position = rhs._position;
    return *this;
}


db_page::key_iterator
db_page::key_iterator::operator++(int i)
{
    db_page::key_iterator tmp = *this;
    this->operator+=(1);
    return tmp;
}


db_page::key_iterator
db_page::key_iterator::operator++()
{
    this->operator+=(1);
    return *this;
}


data_blob
db_page::key_iterator::operator*()
{
    return _page->keyAt(_position);
}


int db_page::key_iterator::operator-(db_page::key_iterator const &rhs)
{
    assert(_page == rhs._page);
    return _position - rhs._position;
}


db_page::key_iterator&
db_page::key_iterator::operator += (int offset)
{
    int newPosition = _position + offset;
    if (newPosition >= _page->recordCount()) {
        *this = _page->keysEnd();
    } else {
        _position = newPosition;
    }

    return *this;
}


data_blob
db_page::key_iterator::value() const
{
    return _page->valueAt(_position);
}


int db_page::key_iterator::child() const
{
    return _page->childAt(_position);
}

//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------

db_page* db_page::load(int index, data_blob pageBytes)
{
    auto dbPage = new db_page(index, pageBytes);
    dbPage->_load();
    return dbPage;
}


db_page* db_page::createEmpty(int index, data_blob pageBytes, bool isLeaf)
{
    auto dbPage = new db_page(index, pageBytes);
    dbPage->_initializeEmpty(!isLeaf);
    return dbPage;
}


db_page::db_page(int index, data_blob pageBytes) :
    _index(index),
    _pageSize(pageBytes.length()),
    _pageBytes(pageBytes.dataPtr())
{  }


void db_page::_initializeEmpty(bool hasLinks)
{
    _indexTable = _pageBytes + 2 * sizeof(uint16_t) + sizeof(uint64_t) + 1;

    _hasChildren = hasLinks;
    _recordIndexSize = _calcRecordIndexSize();
    _wasChanged = true;

    //_pageBytesUint64(sizeof(uint64_t), 0);                      // last modified operation id
    //_pageBytesUint16(sizeof(uint64_t), 0);                      // record count

    _dataBlockEndOffset = _pageSize;
    //_pageBytesUint16(0, (uint16_t) _dataBlockEndOffset);    // data block end offset
    _pageBytes[2*sizeof(uint16_t) + sizeof(uint64_t)] = (uint8_t) hasLinks;
}


bool db_page::isFull() const
{
    return double(usedBytes()) / _pageSize * 100 >= maximallyFullPercent;
}


size_t db_page::recordCount() const
{
    return _recordCount;
}


bool db_page::hasChildren() const
{
    return _hasChildren;
}


int db_page::childAt(int position) const
{
    assert( _pageBytes != nullptr );
    assert(_hasChildren);
    assert( position >= 0 && position <= _recordCount );

    return *(int32_t *)(_recordIndexRawPtr(position) + 3);
}


data_blob db_page::keyAt(int position) const
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount );

    auto recordIndex = _recordIndex(position);
    uint8_t *ptr = _pageBytes + recordIndex.keyValueOffset;
    return data_blob(ptr, recordIndex.keyLength);
}


data_blob db_page::valueAt(int position) const
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount );

    auto recordIndex = _recordIndex(position);
    uint8_t *ptr = _pageBytes + recordIndex.valueOffset();
    return data_blob(ptr, recordIndex.valueLength);
}


void db_page::insert(int position, key_value data, int linked)
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount+1 );
    assert( possibleToInsert(data) );
    assert( hasChildren() || linked == -1 );

    _dataBlockEndOffset -= data.summLength();
    std::copy(data.key.dataPtr(), data.key.dataEndPtr(), _pageBytes + _dataBlockEndOffset);
    std::copy(data.value.dataPtr(), data.value.dataEndPtr(), _pageBytes + _dataBlockEndOffset + data.key.length());

    _insertRecordIndex(position, record_index(_dataBlockEndOffset, data.key.length(), data.value.length()), linked);
    _wasChanged = true;
}


db_page::key_iterator
db_page::keysBegin() const
{
    return db_page::key_iterator(this, 0);
}


db_page::key_iterator
db_page::keysEnd() const
{
    return db_page::key_iterator(this, (int)_recordCount);
}


db_page::record_index
db_page::_recordIndex(int position) const
{
    auto rawPtr = _recordIndexRawPtr(position);
    return record_index(rawPtr[0], rawPtr[1], rawPtr[2]);
}


void db_page::_insertRecordIndex(int position, db_page::record_index const &recordIndex, int linked)
{
    assert( _auxInfoSize() + _recordIndexSize <= _dataBlockEndOffset );

    std::copy_backward((uint8_t *)_recordIndexRawPtr(position),
                       _pageBytes + _auxInfoSize(),
                       _pageBytes + _auxInfoSize() + _calcRecordIndexSize());

    auto rawPtr = _recordIndexRawPtr(position);
    rawPtr[0] = recordIndex.keyValueOffset;
    rawPtr[1] = recordIndex.keyLength;
    rawPtr[2] = recordIndex.valueLength;
    if (_hasChildren) reconnect(position, linked);

    _recordCount++;
}


void db_page::prepareForWriting()
{
    assert( _pageBytes != nullptr );

    _pageBytesUint64(0, _lastModifiedOpId);
    _pageBytesUint16(sizeof(uint64_t), (uint16_t)_recordCount);
    _pageBytesUint16(sizeof(uint16_t) + sizeof(uint64_t), (uint16_t)_dataBlockEndOffset);
}


void db_page::_load()
{
    _indexTable = _pageBytes + 2 * sizeof(uint16_t) + sizeof(uint64_t) + 1;
    _hasChildren = _pageBytes[2*sizeof(uint16_t) + sizeof(uint64_t)] != 0;
    _recordIndexSize = _calcRecordIndexSize();

    _lastModifiedOpId = _pageBytesUint64(0);
    _recordCount = _pageBytesUint16(sizeof(uint64_t));
    _dataBlockEndOffset = _pageBytesUint16(sizeof(uint16_t) + sizeof(uint64_t));
}


db_page::~db_page()
{
    _destructThis();
}


void db_page::insert(db_page::key_iterator position, key_value data, int childId)
{
// this is useful to test page allocation related bugs
/*
    for (int i = 0; i < _recordCount; ++i)
        if (_hasLinks && childAt(i) == childId) assert(0);
*/

    assert( position.associatedPage() == this );
    insert(position.position(), data, childId);
}


void db_page::reconnect(int position, int childId)
{
// this is useful to test page allocation related bugs
/*
    for (int i = 0; i < _recordCount; ++i)
        if (_hasLinks && childAt(i) == childId) assert(0);
*/

    assert( _pageBytes != nullptr );
    assert(_hasChildren);
    assert( position >= 0 && position <= _recordCount );

    *(int32_t *)(_recordIndexRawPtr(position) + 3) = childId;
    _wasChanged = true;
}


bool db_page::possibleToInsert(key_value element)
{
    return _freeBytes() >= element.summLength() + _calcRecordIndexSize();
}


void db_page::remove(int position)
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount );
    if (position < 0 || position >= _recordCount)  return;

    auto indexBlock = _recordIndex(position);
    std::copy_backward(_pageBytes + _dataBlockEndOffset, _pageBytes + indexBlock.keyValueOffset,
            _pageBytes + indexBlock.dataEnd());

    std::copy((uint8_t *)_recordIndexRawPtr(position+1), _pageBytes + _auxInfoSize(),
            (uint8_t *)_recordIndexRawPtr(position));
    _recordCount--;

    for (int i = 0; i < _recordCount; ++i) {
        auto nextIndex = _recordIndex(i);
        if (nextIndex.keyValueOffset < indexBlock.keyValueOffset) {
            auto rawPtr = _recordIndexRawPtr(i);
            rawPtr[0] += indexBlock.length();
        }
    }

    _dataBlockEndOffset += indexBlock.length();
    _wasChanged = true;
}


bool db_page::isMinimallyFilled() const
{
    return double(usedBytes()) / _pageSize * 100 >= minimallyFullPercent;
}


bool db_page::willRemainMinimallyFilledWithout(int position) const
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount );

    size_t realPageSize = _pageSize;
    _pageSize += _recordIndex(position).length();
    bool result = isMinimallyFilled();
    _pageSize = realPageSize;

    return result;
}


void db_page::replace(int position, data_blob newValue)
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount );

    auto index = _recordIndex(position);
    data_blob_copy key(data_blob((uint8_t *)::malloc(index.keyLength), index.keyLength));
    std::copy(_pageBytes + index.keyValueOffset, _pageBytes + index.keyValueOffset + index.keyLength, key.dataPtr());
    int linked = -1;
    if (_hasChildren) linked = childAt(position);

    remove(position);
    insert(position, key_value(key, newValue), linked);
    key.release();
}


key_value_copy db_page::splitEquispace(db_page *rightPage)
{
    assert( _pageBytes != nullptr );
    assert( _recordCount >= 3 );    // at least 3 records for correct split

    // the way is to create proxy in-memory page and copy half of records into that page
    // then replace *this* page with proxy page's content (dirty enoughf)
    // this is done because insertion in page is much faster than removing now
    db_page *leftProxyPage = new db_page(_index, data_blob((uint8_t *)::calloc(_pageSize, 1), _pageSize));
    leftProxyPage->_initializeEmpty(_hasChildren);

    size_t allocatedSpace = (_pageSize - _dataBlockEndOffset);
    size_t neededSize = (allocatedSpace - (allocatedSpace / _recordCount)) / 2;

    size_t accumulatedSize = 0;
    int copiedRecordCount = 0;

    for (; copiedRecordCount < _recordCount-2 && (accumulatedSize < neededSize || copiedRecordCount < 1); ++copiedRecordCount) {
        accumulatedSize += _recordIndex(copiedRecordCount).length();
        leftProxyPage->insert(copiedRecordCount, recordAt(copiedRecordCount));
        if (_hasChildren) leftProxyPage->reconnect(copiedRecordCount, childAt(copiedRecordCount));
    }
    if (_hasChildren) leftProxyPage->reconnect(copiedRecordCount, childAt(copiedRecordCount));

    int medianPosition = copiedRecordCount++;

    for (int i = 0; copiedRecordCount < _recordCount; ++copiedRecordCount, ++i) {
        rightPage->insert(i, recordAt(copiedRecordCount));
        if (_hasChildren) rightPage->reconnect(i, childAt(copiedRecordCount));
    }
    if (_hasChildren) rightPage->reconnect((int) rightPage->recordCount(), lastRightChild());

    key_value_copy medianElement(this->recordAt(medianPosition));
    this->moveContentFrom(leftProxyPage);
    delete leftProxyPage;

    return medianElement;
}


bool db_page::canReplace(int position, data_blob newData) const
{
    return newData.length() - _recordIndex(position).valueLength <= _freeBytes();
}


void db_page::replace(int position, const key_value &data, int linked)
{
    assert( position >= 0 && position < _recordCount );

    remove(position);
    insert(position, data, linked);
}


void db_page::append(key_value data, int linked)
{
    insert((int)_recordCount, data, linked);
}


size_t db_page::usedBytes() const
{
    return _pageSize - _freeBytes();
}


size_t db_page::usedBytesFor(int position) const
{
    assert( position >= 0 && (position < _recordCount  || position <= _recordCount && _hasChildren) );

    if (position == _recordCount)  return _recordIndexSize;
    return _recordIndexSize + _recordIndex(position).length();
}


key_value db_page::recordAt(int position) const
{
    return key_value(this->keyAt(position), this->valueAt(position));
}


void db_page::_destructThis()
{
    if (_pageBytes) {
        free(_pageBytes);
        _pageBytes = nullptr;
    }
}


void db_page::moveContentFrom(db_page *srcPage)
{
    srcPage->prepareForWriting();
    _pageBytes = srcPage->_pageBytes;
    srcPage->_pageBytes = nullptr;     // to avoid free in srcPage's destructor

    this->_load();                     // update cached members from _pageBytes
    _wasChanged = srcPage->_wasChanged;
}


void db_page::wasSaved(uint64_t opId)
{
    _lastModifiedOpId = opId;
    _wasChanged = false;
}


uint8_t *db_page::bytes() const
{
    assert( _pageBytes != nullptr );
    return _pageBytes;
}


void db_page::wasCached(pages_cache_internals::cached_page_info *cacheRelatedInfo)
{
    assert( cacheRelatedInfo != nullptr );
    _cacheRelatedInfo = cacheRelatedInfo;
}


pages_cache_internals::cached_page_info *db_page::cacheRelatedInfo() const
{
    return _cacheRelatedInfo;
}

//----------------------------------------------------------------------------------------------------------------------
}
#include "db_stable_storage_file.hpp"

#include <cassert>
#include <cstdlib>
#include <exception>
#include <stdexcept>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

auto db_stable_storage_file::openExisting(const std::string &fileName) -> db_stable_storage_file *
{
    auto dbFile = new db_stable_storage_file();

    dbFile->_file = raw_file::openExisting(fileName, false);
    dbFile->_load();
    return dbFile;
}


auto db_stable_storage_file::createEmpty(const std::string &fileName, db_data_storage_config const &config)
-> db_stable_storage_file *
{
    auto dbFile = new db_stable_storage_file();

    dbFile->_file = raw_file::createNew(fileName);
    dbFile->_pageSize = config.pageSize;
    dbFile->_initializeEmpty(config.maxStorageSize);
    return dbFile;
}


db_stable_storage_file::~db_stable_storage_file()
{
    _file->writeAll(_lastFreePage_InfileOffset, &_nextFreePage, sizeof(_nextFreePage));

    if (_pagesMetaTable) {
        ::free(_pagesMetaTable);
    }

    delete _file;  // this will also close the file
}


void db_stable_storage_file::_initializeEmpty(size_t maxStorageSize)
{
    off_t offset = _file->writeAll(_pageSize_InfileOffset, &_pageSize, sizeof(_pageSize));

    _maxPageCount = maxStorageSize / _pageSize;
    _initPagesMetaTableByteSize();

    offset = _file->writeAll(offset, &_maxPageCount, sizeof(_maxPageCount));
    _lastFreePage_InfileOffset = offset;
    offset = _file->writeAll(offset, &_nextFreePage, sizeof(_nextFreePage));
    _rootPageId_InfileOffset = offset;
    offset += sizeof(int); // root page id placeholder

    _pagesMetaTableStartOffset = (size_t) offset;
    _file->ensureSizeIsAtLeast(offset + _pagesMetaTableSize);

    _pagesStartOffset = offset + _pagesMetaTableSize;
    _pagesMetaTable = (uint8_t*) calloc(_pagesMetaTableSize, 1);
}


void db_stable_storage_file::_load()
{
    off_t offset = 0;
    offset = _file->readAll(offset, &_pageSize, sizeof(_pageSize));

    offset = _file->readAll(offset, &_maxPageCount, sizeof(_maxPageCount));
    _lastFreePage_InfileOffset = offset;
    offset = _file->readAll(offset, &_nextFreePage, sizeof(_nextFreePage));
    _nextFreePage = 0; // todo: according to new ideas in pages allocation this can't be permanently stored
    offset = _file->readAll(offset, &_rootPageId,   sizeof(_rootPageId));

    _pagesMetaTableStartOffset = offset;
    _initPagesMetaTableByteSize();
    _pagesStartOffset = offset + _pagesMetaTableSize;

    _pagesMetaTable = (uint8_t *) malloc(_pagesMetaTableSize);
    _file->readAll(_pagesMetaTableStartOffset, _pagesMetaTable, _pagesMetaTableSize);
}


void db_stable_storage_file::_initPagesMetaTableByteSize()
{
    _pagesMetaTableSize = _maxPageCount / 8 + ((_maxPageCount % 8) ? 1 : 0);
}


int db_stable_storage_file::_getNextFreePageIndex()
{
    int pageIndex = _nextFreePage;

    unsigned currentByteOffset = (unsigned) (pageIndex / 8);
    uint8_t currentInByteOffset = (uint8_t) ( pageIndex % 8 );

    while (true) {
        if (_pagesMetaTableSize > sizeof(uint64_t)) {
            unsigned currentLongOffset = (unsigned) (pageIndex / 64);
            while (*((uint64_t *)_pagesMetaTable + currentLongOffset) == 0xFFFFFFFFFFFFFFFF
                   && currentByteOffset < _pagesMetaTableSize/sizeof(uint64_t)) {
                ++currentLongOffset;
                currentInByteOffset = 0;
                pageIndex = currentLongOffset * 8 * (int)sizeof(uint64_t);
            }
        }

        while (_pagesMetaTable[currentByteOffset] == 0xFF && currentByteOffset < _pagesMetaTableSize) {
            ++currentByteOffset;
            currentInByteOffset = 0;
            pageIndex = currentByteOffset * 8;
        }
        if (currentByteOffset >= _pagesMetaTableSize) {
            throw std::runtime_error("page allocation failed: no more free space");
        }

        uint8_t currentByte = _pagesMetaTable[currentByteOffset];
        unsigned i = currentInByteOffset;
        for (; i < 8; ++i) {
            if ((currentByte  & (1 << i)) == 0)  return pageIndex;
            ++pageIndex;
        }

        currentInByteOffset = 0;
        ++currentByteOffset;
    }
}


void db_stable_storage_file::_updatePageMetaInfo(int pageIndex, bool allocated)
{
    unsigned currentByteOffset = (unsigned) pageIndex / 8;
    unsigned char currentInByteOffset = (unsigned char) ( pageIndex % 8 );

    if (allocated) {
        _pagesMetaTable[currentByteOffset] |= (unsigned char) (1 << currentInByteOffset);
    } else {
        _pagesMetaTable[currentByteOffset] &= ~ (unsigned char) (1 << currentInByteOffset);
    }

    _file->writeAll(_pagesMetaTableStartOffset + currentByteOffset, _pagesMetaTable + currentByteOffset, 1);
}


off_t db_stable_storage_file::_pageOffset(int index) const
{
    assert(index >= 0 && index < _maxPageCount);
    return _pagesStartOffset + index * _pageSize;
}


void db_stable_storage_file::writePage(db_page *page)
{
    assert( page != nullptr );

    page->prepareForWriting();
    _file->writeAll(_pageOffset(page->id()), page->bytes(), page->size());
}


db_page* db_stable_storage_file::allocatePage(bool isLeaf)
{
    int pageId = _getNextFreePageIndex();
    _nextFreePage = pageId + 1;

    _updatePageMetaInfo(pageId, true);
    //_file->ensureSizeIsAtLeast(_pageOffset(pageId) + _pageSize);

    uint8_t *rawPageBytes = (uint8_t *)::calloc(_pageSize, 1);
    db_page *page = db_page::createEmpty(pageId, data_blob(rawPageBytes, _pageSize), isLeaf);

    return page;
}


void db_stable_storage_file::deallocatePage(int pageId)
{
    assert( pageId >= 0 && pageId < _maxPageCount );

    _nextFreePage = pageId;
    _updatePageMetaInfo(pageId, false);
}


db_page* db_stable_storage_file::loadPage(int pageId)
{
    assert( pageId >= 0 && pageId < _maxPageCount );

    uint8_t *rawPageBytes = (uint8_t *)::malloc(_pageSize);
    _file->readAll(_pageOffset(pageId), rawPageBytes, _pageSize);

    if (_file->eof()) {
        ::free(rawPageBytes);
        return nullptr;
    }

    return db_page::load(pageId, data_blob(rawPageBytes, _pageSize));
}


void db_stable_storage_file::changeRootPage(int pageId)
{
    assert( pageId >= 0 && pageId < _maxPageCount );

    _rootPageId = pageId;
    _diskWriteRootPageId();
}


void db_stable_storage_file::_diskWriteRootPageId()
{
    _file->writeAll(_rootPageId_InfileOffset, &_rootPageId, sizeof(_rootPageId));
}

//----------------------------------------------------------------------------------------------------------------------
}
#include "libsfera_db.h"

#include "database.hpp"
#include "syscall_checker.hpp"

#include <iostream>

//----------------------------------------------------------------------------------------------------------------------

#define catch_exceptions(fnn,rt)  catch (const errors::syscall_result_failed &err)  { std::cerr << fnn << ": syscall_result_failed: " << err.what() << std::endl; \
 							      return rt; } \
								  catch (const std::runtime_error &err)  { std::cerr << fnn << ": runtime_error: " << err.what() << std::endl; \
 							      return rt; } \
							      catch (const std::exception &err)  { std::cerr << fnn << ": " << err.what() << std::endl; \
 							      return rt; }

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

//----------------------------------------------------------------------------------------------------------------------

extern "C"
database* dbopen(const char *file)
{
	try {
		database *db = database::openExisting(file);
		return db;
	}
	catch_exceptions("dbopen", nullptr);
}


extern "C"
database* dbcreate(const char *file, DBC *conf)
{
	try {
		database_config dbConfig;
		dbConfig.pageSizeBytes = conf->page_size;
		dbConfig.maxDBSize = conf->db_size;
		dbConfig.cacheSizePages = conf->cache_size / conf->page_size;

		database *db = database::createEmpty(file, dbConfig);
		return db;
	}
	catch_exceptions("dbcreate", nullptr);
}


extern "C"
int db_close(database *db)
{
	try {
		//std::cout << db->dumpTree() << std::endl;
		delete db;
		return 0;
	}
	catch_exceptions("db_close", -1);
}


extern "C"
int db_delete(database *db, void *key, size_t length)
{
	if (db == nullptr)  return -1;

	try {
		db->remove(data_blob((uint8_t *)key, length));
		return 0;
	}
	catch_exceptions("db_del", -1);
}


extern "C"
int db_select(database *db, void *key, size_t keyLength, void **pVal, size_t *pValLength)
{
//	if (db == nullptr || key == nullptr || keyLength == 0 || pVal == nullptr || pValLength == nullptr)
//		return -1;

	try {
		data_blob_copy result = db->get(data_blob((uint8_t *)key, keyLength));
		if (!result.valid()) {
			*pVal = nullptr;
			*pValLength = 0;
			return 1;
		}

		*pVal = result.dataPtr();
		*pValLength = result.length();
		return 0;
	}
	catch_exceptions("db_get", -1);
}


extern "C"
int db_insert(database *db, void *key, size_t keyLength, void *value, size_t valueLength)
{
	if (db == nullptr || key == nullptr || keyLength == 0 || value == nullptr || valueLength == 0)
		return -1;

	try {
		db->insert(data_blob((uint8_t *)key, keyLength), data_blob((uint8_t *)value, valueLength));
		return 0;
	}
	catch_exceptions("db_put", -1);
}


extern "C"
int db_flush(database *db)
{
	if (db == nullptr)  return -1;

	try {
		return 0;
	}
	catch_exceptions("db_flush", -1);
}

#include "pages_cache.hpp"
#include <cassert>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

pages_cache::pages_cache(size_t sizePages, std::function<void(db_page*)> pageWriter) :
    _sizePages(sizePages),
    _pageWriter(pageWriter)
{
    _cachedPages.max_load_factor(1.5);
    _cachedPages.reserve((size_t)((double)sizePages * 1.5));
}


db_page* pages_cache::fetchAndPin(int pageId)
{
    _statistics.fetchesCount++;

    auto pageIt = _cachedPages.find(pageId);
    if (pageIt == _cachedPages.end()) {
        _statistics.missesCount++;
        return nullptr;
    }

    cached_page_info& cachedPageInfo = pageIt->second;
    cachedPageInfo.pinned++;
    _lruAccess(cachedPageInfo);

    return cachedPageInfo.page;
}


void pages_cache::cacheAndPin(db_page *page)
{
    assert( page != nullptr );
    assert( _cachedPages.find(page->id()) == _cachedPages.end() );

    cached_page_info newPageInfo(page);
    newPageInfo.pinned = 1;
    _lruAdd(newPageInfo);
    page->wasCached(&(_cachedPages.emplace(page->id(), newPageInfo).first->second));
}


void pages_cache::unpin(db_page *page)
{
    assert( page != nullptr );
    assert( page->cacheRelatedInfo() != nullptr );

    //auto pageIt = _cachedPages.find(page->id());
    //if (pageIt == _cachedPages.end()) return;

    auto cachedPageInfo = page->cacheRelatedInfo();

    assert( cachedPageInfo->pinned > 0 );    // this is actual for one-threaded db
    cachedPageInfo->pinned--;
}


void pages_cache::invalidateCachedPage(int pageId)
{
    cached_page_info &pageInfo = _cachedPages[pageId];
    _lruQueue.erase(pageInfo.lruQueueIterator);

    _writeAndDestroyPage(pageInfo);
    _cachedPages.erase(pageId);
}


void pages_cache::makeDirty(db_page *page)
{
    assert( page->cacheRelatedInfo() != nullptr );
    auto cachedPageInfo = page->cacheRelatedInfo();

    //auto pageIt = _cachedPages.find(page->id());
    //assert( pageIt != _cachedPages.end() );

    cachedPageInfo->dirty = true;
}


void pages_cache::flush()
{
    for (auto cachedPage : _cachedPages) {
        if (cachedPage.second.dirty) {
            _pageWriter(cachedPage.second.page);
        }
    }
}


void pages_cache::_writeAndDestroyPage(const cached_page_info &pageInfo)
{
    if (pageInfo.dirty) {
        _pageWriter(pageInfo.page);
    }

    delete pageInfo.page;
}


void pages_cache::clearCache()
{
    for (auto cachedPage : _cachedPages) {
        _writeAndDestroyPage(cachedPage.second);
    }

    _cachedPages.clear();
}


pages_cache::~pages_cache()
{
    assert( _cachedPages.empty() );
    //clearCache();
}


bool pages_cache::_evict()
{
    _statistics.ecivtionsCount++;

    int pageId = _lruQueue.front();
    auto pageIt = _cachedPages.find(pageId);
    assert( pageIt != _cachedPages.end() );

    cached_page_info &pageInfo = pageIt->second;
    if (pageInfo.pinned > 0) {
        _statistics.failedEvictions++;
        return false;
    }

    _writeAndDestroyPage(pageInfo);
    _lruQueue.pop_front();
    _cachedPages.erase(pageIt);

    return true;
}


void pages_cache::_lruAdd(cached_page_info &pageInfo)
{
    while (_lruQueue.size() >= _sizePages && _evict());

    _lruQueue.push_back(pageInfo.page->id());
    pageInfo.lruQueueIterator = _lruQueue.cend();
    std::advance(pageInfo.lruQueueIterator, -1);
}


void pages_cache::_lruAccess(cached_page_info &pageInfo)
{
    _lruQueue.erase(pageInfo.lruQueueIterator);

    _lruQueue.push_back(pageInfo.page->id());
    pageInfo.lruQueueIterator = _lruQueue.cend();
    std::advance(pageInfo.lruQueueIterator, -1);
}


void pages_cache::pin(db_page *page)
{
    auto pageIt = _cachedPages.find(page->id());
    assert( pageIt != _cachedPages.end() );

    pageIt->second.pinned++;
}


void pages_cache::unpinIfClean(db_page *page)
{
    auto pageIt = _cachedPages.find(page->id());
    if (pageIt == _cachedPages.end()) return;
    //assert( pageIt != _cachedPages.end() );

    if (!pageIt->second.dirty) {
        pageIt->second.pinned--;
    }
}

//----------------------------------------------------------------------------------------------------------------------
}

#include "raw_file.hpp"
#include "syscall_checker.hpp"

#include <iostream>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <cassert>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

raw_file::~raw_file()
{
    if (_unixFD != -1) {
        ::close(_unixFD);
    }
}


raw_file *raw_file::createNew(const std::string &path, bool writeOnly)
{
    raw_file *file = new raw_file();

    file->_unixFD = ::creat(path.c_str(), 0777);
    if (file->_unixFD == -1) {
        delete file;
        syscall_check(-1);
    }

    if (!writeOnly) {
        ::close(file->_unixFD);

        file->_unixFD = ::open(path.c_str(), O_RDWR);
        if (file->_unixFD == -1) {
            delete file;
            syscall_check(-1);
        }
    }

    return file;
}


raw_file *raw_file::openExisting(const std::string &path, bool readOnly)
{
    auto file = new raw_file();

    file->_unixFD = ::open(path.c_str(), readOnly ? O_RDONLY : O_RDWR);
    if (file->_unixFD == -1) {
        delete file;
        syscall_check(file->_unixFD);
    }

    struct stat fileStat = {};
    int statResult = ::stat(path.c_str(), &fileStat);
    if (statResult == -1) {
        delete file;
        syscall_check(statResult);
    }

    file->_actualFileSize = size_t(fileStat.st_size);
    return file;
}


bool raw_file::exists(const std::string &path)
{
    return ::access(path.c_str(), F_OK) != -1;
}


void raw_file::ensureSizeIsAtLeast(size_t neededSize)
{
    if (_actualFileSize < neededSize) {
        syscall_check( ::ftruncate (_unixFD, neededSize) );
        _actualFileSize = neededSize;
    }
}


off_t raw_file::writeAll(off_t offset, const void *data, size_t length)
{
    _eof = false;

    for (size_t writtenBytes = 0; writtenBytes < length;) {
        ssize_t writeResult = ::pwrite(_unixFD, (uint8_t *)data + writtenBytes,
                                       length - writtenBytes, offset + writtenBytes);
        syscall_check( writeResult );
        writtenBytes += writeResult;
    }

    return offset + length;
}


off_t raw_file::readAll(off_t offset, void *data, size_t length) const
{
    for (size_t readBytes = 0; readBytes < length;) {
        ssize_t readResult = ::pread(_unixFD, (uint8_t *)data + readBytes, length - readBytes, offset + readBytes);
        syscall_check( readResult );
        if (readResult == 0) {
            _eof = true;
            return offset + length;
        }
        readBytes += readResult;
    }

    return offset + length;
}


void raw_file::appedAll(const void *data, size_t length)
{
    _eof = false;

    for (size_t writtenBytes = 0; writtenBytes < length;) {
        ssize_t writeResult = ::write(_unixFD, (uint8_t *)data + writtenBytes,
                                      length - writtenBytes);
        syscall_check( writeResult );
        writtenBytes += writeResult;
    }
}


void raw_file::appedAll(std::pair<void const *, size_t> buffers[], size_t buffersCount)
{
    assert( buffersCount <= ::sysconf(_SC_IOV_MAX) );
    _eof = false;

    struct iovec iovs[buffersCount];
    size_t summLen = 0;
    for (size_t i = 0; i < buffersCount; ++i) {
        iovs[i].iov_base = const_cast<void*> (buffers[i].first);
        iovs[i].iov_len  = buffers[i].second;

        summLen += buffers[i].second;
    }

    int writtenBuffs = 0;
    for (size_t writtenBytes = 0; writtenBytes < summLen;) {
        ssize_t writeResult = ::writev(_unixFD, &iovs[0], (int)buffersCount);
        syscall_check( writeResult );

        assert( writeResult == summLen ); // TODO: not implemented correctly

        writtenBytes += writeResult;
    }
}


size_t raw_file::readAll(void *data, size_t length)
{
    size_t readBytes = 0;

    for (; readBytes < length;) {
        ssize_t readResult = ::read(_unixFD, (uint8_t *)data + readBytes, length - readBytes);
        syscall_check( readResult );
        if (readResult == 0) {
            _eof = true;
            return readBytes;
        }
        readBytes += readResult;
    }

    return readBytes;
}


bool raw_file::eof()
{
    return _eof;
}

//----------------------------------------------------------------------------------------------------------------------
}
#include <iostream>
#include <vector>
#include <algorithm>

#include "database.hpp"

using namespace sfera_db;


void fillTestSet(std::vector<std::pair<data_blob, data_blob>> &testSet, size_t count)
{
    testSet.resize(count);

    for (size_t i = 0; i < count; ++i) {

        auto skey   = std::to_string(i) + std::string("test key") + std::string(i%20, '0') + std::to_string(i);
        auto svalue = std::string("value QWERTY test") + std::to_string(i);

        testSet[i] = std::make_pair(data_blob::fromCopyOf(skey), data_blob::fromCopyOf(svalue));
    }

    std::random_shuffle(testSet.begin(), testSet.end());
}


void testOpening(std::vector<std::pair<data_blob, data_blob>> &testSet)
{
    database *db = database::openExisting("test_db");
    //std::cout << std::endl << db->dumpTree() << std::endl;

    bool getOK = true;
    for (size_t i = 0; i < testSet.size(); ++i) {
        std::cout << "GET " << testSet[i].first.toString() << " : " << testSet[i].second.toString() << " = ";
        data_blob_copy result = db->get(testSet[i].first);
        std::cout << result.toString() << std::endl;

        if (result.toString() != testSet[i].second.toString()) {
            getOK = false;
            break;
        }

        result.release();
    }

    //std::cout << std::endl << db->dumpTree() << std::endl;
    std::cout << "GET TEST: " << getOK << std::endl;

    delete db;
}


int main (int argc, char** argv)
{
    database_config dbConfig;
    dbConfig.pageSizeBytes = 2000;
    dbConfig.maxDBSize = 100000*1024;
    dbConfig.cacheSizePages = 256;

    std::vector<std::pair<data_blob, data_blob>> testSet;
    fillTestSet(testSet, 40000);

    if (database::exists("test_db")) {
        testOpening(testSet);
        return 0;
    }

    database *db = database::createEmpty("test_db", dbConfig);

    for (int j = 0; j < 1; ++j) {

        // insertion
        for (size_t i = 0; i < testSet.size(); ++i) {
            std::cout << "INSERT " << testSet[i].first.toString() << " : " << testSet[i].second.toString() << std::endl;
            db->insert(testSet[i].first, testSet[i].second);

            //std::cout << std::endl << db->dumpTree() << std::endl;
        }

        std::random_shuffle(testSet.begin(), testSet.end());

        // getting
        bool getOK = true;
        for (size_t i = 0; i < testSet.size(); ++i) {
            std::cout << "GET " << testSet[i].first.toString() << " : " << testSet[i].second.toString() << " = ";
            data_blob_copy result = db->get(testSet[i].first);
            std::cout << result.toString() << std::endl;

            if (result.toString() != testSet[i].second.toString()) {
                getOK = false;
                break;
            }

            result.release();
        }

        std::cout << "GET TEST: " << getOK << std::endl;
        //std::cout << std::endl << db->dumpTree() << std::endl;

        std::random_shuffle(testSet.begin(), testSet.end());

        /*
    // removing
    for (size_t i = 0; i < testSet.size(); ++i) {
        std::cout << "REMOVE " << testSet[i].first.toString() << " ";
        db->remove(testSet[i].first);
        data_blob_copy result = db->get(testSet[i].first);

        std::cout << (result.valid() ? "FAILED" : "OK") << std::endl;
        result.release();
    }

    std::cout << std::endl << db->dumpTree() << std::endl;
*/

    }

    std::cout << std::endl << "=== cache statistics ===\n" << db->dumpCacheStatistics() << std::endl;
    delete db;
    return 0;
}
