
#include "database.hpp"

#include <cassert>
#include <algorithm>
#include <cstring>
#include <sstream>
#include <unistd.h>
#include <sys/stat.h>

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

//----------------------------------------------------------------------------------------------------------------------

auto database::createEmpty(const std::string &path, database_config const &config) -> database *
{
    struct stat dirstat = {};
    if (::stat(path.c_str(), &dirstat) != 0 || !S_ISDIR(dirstat.st_mode)) {
        syscall_check( ::mkdir(path.c_str(), 0777) );
    }

    db_file_storage_config dbStorageCfg;
    dbStorageCfg.maxStorageSize = config.maxDBSize;
    dbStorageCfg.pageSize = config.pageSizeBytes;

    database *db = new database();
    db->_fileStorage = db_file_storage::createEmpty(path + "/" + db->MainStorageFileName, dbStorageCfg);
    return db;
}


auto database::openExisting(const std::string &path) -> database *
{
    database *db = new database();
    db->_fileStorage = db_file_storage::openExisting(path + "/" + db->MainStorageFileName);
    return db;
}


void database::insert(data_blob key, data_blob value)
{
    _rKeyInsertionLookup(_fileStorage->rootPage()->index(), -1, key_value(key, value));
}


database::~database()
{ }


data_blob database::get(data_blob key)
{
    record_lookup_result lookupResult = _rKeyLookup(_fileStorage->rootPage()->index(), -1, -1, key);
    if (lookupResult.empty())  return data_blob();

    db_page *page = _fileStorage->fetchPage(lookupResult.requestedRecord.pageId);
    auto value = _copyDataFromLoadedPage(page->value(lookupResult.requestedRecord.inPagePosition));
    _fileStorage->releasePage(page);

    return value;
}


database::record_lookup_result
database::_rKeyLookup(int pageId, int parentPageId, int parentRecordPos, data_blob key)
{
    db_page *page = _fileStorage->fetchPage(pageId);
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
    _fileStorage->releasePage(page);

    return _rKeyLookup(nextPageId, pageId, parRecPos, key);
}


bool database::_binaryKeyComparer(data_blob key1, data_blob key2)
{
    int cr = memcmp(key1.dataPtr(), key2.dataPtr(), std::min(key1.length(), key2.length()));

    if (cr < 0)  return true;
    if (cr == 0) return key1.length() < key2.length();
    return false;
}


bool database::_keysEqual(data_blob key1, data_blob key2)
{
    if (key1.length() != key2.length())  return false;
    return memcmp(key1.dataPtr(), key2.dataPtr(), key1.length()) == 0;
}


data_blob database::_copyDataFromLoadedPage(data_blob src) const
{
    uint8_t *copyPtr = (uint8_t *)malloc(src.length());
    std::copy(src.dataPtr(), src.dataEndPtr(), copyPtr);
    return data_blob(copyPtr, src.length());
}


void database::_rKeyInsertionLookup(int pageId, int parentPageId, const key_value &element)
{
    db_page *page = _fileStorage->fetchPage(pageId);
    auto keyIt = std::lower_bound(page->begin(), page->end(), element.key, _binaryKeyComparer);

    if (keyIt != page->end() && _keysEqual(element.key, *keyIt)) {    // something like value update
        if (page->canReplace(keyIt.position(), element.value)) {
            page->replace(keyIt.position(), element.value);
            _fileStorage->writePage(page);
            _fileStorage->releasePage(page);
        } else {
            _fileStorage->releasePage(page);
            throw std::runtime_error("Failed to update value"); // todo handle equal key error
        }
        return;
    }

    if (page->isFull()) {
        page = _splitPage(page, parentPageId, element);
        keyIt = std::lower_bound(page->begin(), page->end(), element.key, _binaryKeyComparer);
    }

    if (!page->hasLinks()) {

        int nextPageId = page->index();
        _fileStorage->releasePage(page);
        _tryInsertInPage(nextPageId, parentPageId, element);
        return;
    }

    int nextPageId = keyIt.link();
    int parPageId = page->index();
    _fileStorage->releasePage(page);

    return _rKeyInsertionLookup(nextPageId, parPageId, element);
}


bool database::_rKeyErasingLookup(int pageId, int parentPageId, int parentRecordPos, const data_blob &elementKey)
{
    db_page *page = _fileStorage->fetchPage(pageId);
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
    _fileStorage->releasePage(page);

    if (_rKeyErasingLookup(nextPageId, nextParPageId, keyIt.position(), elementKey)) {    // we need to check the page
        return _testRebalanceAfterDelete(nextParPageId, parentPageId, parentRecordPos);
    }
    return false;
}


void database::_makeNewRoot(key_value element, int leftLink, int rightLink)
{
    db_page* newRootPage = _fileStorage->allocateNewRootPage();
    newRootPage->insert(0, element, leftLink);
    newRootPage->relink(1, rightLink);

    _fileStorage->writePage(newRootPage);
}


void database::_tryInsertInPage(int pageId, int parentPageId, const key_value &element)
{
    db_page *page = _fileStorage->fetchPage(pageId);
    assert(!page->hasLinks());

    if (!page->possibleToInsert(element)) {
        _fileStorage->releasePage(page);
        std::runtime_error("Impossible insert");
    }

    auto insertionIt = std::lower_bound(page->begin(), page->end(), element.key, _binaryKeyComparer);
    page->insert(insertionIt, element);
    _fileStorage->writePage(page);
    _fileStorage->releasePage(page);
}


db_page *database::_splitPage(db_page *page, int parentPageId, const key_value &element)
{
    db_page *rightPage = _fileStorage->allocatePage(!page->hasLinks());
    int medianKeyPosition = -1;
    db_page *leftPage = page->splitEquispace(rightPage, medianKeyPosition);
    key_value medianElement = _copyDataFromLoadedPage(page->record(medianKeyPosition));
    _fileStorage->releasePage(page);

    if (parentPageId == -1) {
        _makeNewRoot(medianElement, leftPage->index(), rightPage->index());
    } else {

        db_page *parentPage = _fileStorage->fetchPage(parentPageId);
        auto insertionIt = std::lower_bound(parentPage->begin(), parentPage->end(), medianElement.key, _binaryKeyComparer);
        parentPage->relink(insertionIt.position(), rightPage->index());
        parentPage->insert(insertionIt, medianElement, leftPage->index());
        _fileStorage->writePage(parentPage);
        _fileStorage->releasePage(parentPage);
    }

    _fileStorage->writePage(leftPage);
    _fileStorage->writePage(rightPage);

    if (_binaryKeyComparer(element.key, medianElement.key)) {
        _fileStorage->releasePage(rightPage);
        return leftPage;
    } else {
        _fileStorage->releasePage(leftPage);
        return rightPage;
    }
}


bool database::_makePageMinimallyFilled(db_page *page, int parentPageId, int parentRecordPos)
{
    int rightNextPageId = -1, leftPrevPageId = -1;
    db_page *parentPage = _findNeighbours(record_internal_id(parentPageId, parentRecordPos),
            leftPrevPageId, rightNextPageId);

    assert(rightNextPageId != -1 || leftPrevPageId != -1);
    db_page *rightNextPage = rightNextPageId == -1 ? nullptr : _fileStorage->fetchPage(rightNextPageId);
    db_page *leftPrevPage  = leftPrevPageId  == -1 ? nullptr : _fileStorage->fetchPage(leftPrevPageId);

    bool ret = false;
    bool rotationSucceeded = _tryTakeFromNearest(page, parentPage, parentRecordPos, leftPrevPage, rightNextPage);
    if (!rotationSucceeded) {    // so merge the nodes
        _mergePages(page, parentRecordPos, parentPage, rightNextPage, leftPrevPage);
        ret = true;
    }

    _fileStorage->writePage(parentPage);
    _fileStorage->writePage(page);
    _fileStorage->releasePage(leftPrevPage);
    _fileStorage->releasePage(rightNextPage);
    _fileStorage->releasePage(parentPage);

    return ret;
}


void database::_mergePages(db_page *page, int parentRecordPos, db_page *parentPage, db_page *rightNextPage,
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
        _fileStorage->deallocatePage(rightNextPage);
        parentPage->remove(parentRecordPos);

    } else if (leftPrevPage->usedBytes() + basicResultSize < page->size()) {

        int linked = -1;
        if (page->hasLinks())  linked = leftPrevPage->lastLink();
        page->insert(0, parentPage->record(parentRecordPos-1), linked);

        for (int i = (int)leftPrevPage->recordCount()-1; i >= 0; --i) {
            page->insert(0, leftPrevPage->record(i), page->hasLinks() ? leftPrevPage->link(i) : -1);
        }
        _fileStorage->deallocatePage(leftPrevPage);
        parentPage->remove(parentRecordPos-1);
    }
}


key_value database::_copyDataFromLoadedPage(key_value src) const
{
    return key_value(_copyDataFromLoadedPage(src.key), _copyDataFromLoadedPage(src.value));
};


string database::dump() const
{
    std::ostringstream info;
    info << "root: " << _fileStorage->rootPage()->index() << std::endl;
    info << "dumping tree =========================================================================" << std::endl;

    _dump(info, _fileStorage->rootPage()->index());
    info << "======================================================================================" << std::endl;
    return info.str();
}


void database::_dump(std::ostringstream &info, int pageId) const
{
    db_page *page = _fileStorage->fetchPage(pageId);
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

    _fileStorage->releasePage(page);
}


void database::remove(data_blob key)
{
    _rKeyErasingLookup(_fileStorage->rootPage()->index(), -1, -1, key);
}


bool database::_removeFromLeaf(db_page *page, int recPos, int parentPageId, int parentRecordPos)
{
    assert(!page->hasLinks());

    page->remove(recPos);
    if (page->isMinimallyFilled()) {
        _fileStorage->writePage(page);
        _fileStorage->releasePage(page);
        return false;
    }

    bool ret = _makePageMinimallyFilled(page, parentPageId, parentRecordPos);
    _fileStorage->releasePage(page);
    return ret;
}


db_page * database::_findNeighbours(const database::record_internal_id &parentRecord,
                                         int &leftPrevPageId, int &rightNextPageId)
{
    db_page *parentPage = _fileStorage->fetchPage(parentRecord.pageId);

    if (parentRecord.inPagePosition != parentPage->recordCount()) {
        rightNextPageId = parentPage->link(parentRecord.inPagePosition + 1);
    }
    if (parentRecord.inPagePosition != 0) {
        leftPrevPageId = parentPage->link(parentRecord.inPagePosition - 1);
    }

    return parentPage;
}


string database::dumpSortedKeys() const
{
    std::ostringstream info;
    _rDumpSortedKeys(info, _fileStorage->rootPage()->index());
    return info.str();
}


void database::_rDumpSortedKeys(std::ostringstream &info, int pageId) const
{
    db_page *page = _fileStorage->fetchPage(pageId);
    info << "\tpage #" << pageId <<  ": (has_links=" << page->hasLinks() << ")" << std::endl;

    for (auto elementIt = page->begin(); elementIt != page->end(); ++elementIt) {
        if (page->hasLinks()) {
            info << "\t[" << elementIt.link() << "] " << std::endl;
            _rDumpSortedKeys(info, elementIt.link());
        }
        info << (*elementIt).toString() << std::endl;
    }
    if (page->hasLinks()) {
        info << "\t[" << page->lastLink() << "] " << std::endl;
        _rDumpSortedKeys(info, page->lastLink());
    }

    _fileStorage->releasePage(page);
}


bool database::_tryTakeFromNearest(db_page *page, db_page *parentPage, int parentRecPos,
                                        db_page *leftPrevPage, db_page *rightNextPage)
{
    if (leftPrevPage != nullptr) {
        int leftPrevMedianPos = (int)leftPrevPage->recordCount() - 1;
        if (leftPrevPage->willRemainMinimallyFilledWithout(leftPrevMedianPos)) {

            key_value_copy medianElement(leftPrevPage->record(leftPrevMedianPos));
            int leftLastLink = leftPrevPage->hasLinks() ? leftPrevPage->lastLink() : -1;
            int leftMedLink = leftPrevPage->hasLinks() ? leftPrevPage->link(leftPrevMedianPos) : -1;

            leftPrevPage->remove(leftPrevMedianPos);
            if (leftPrevPage->hasLinks()) leftPrevPage->relink((int)leftPrevPage->recordCount(), leftMedLink);
            _fileStorage->writePage(leftPrevPage);

            page->insert(0, parentPage->record(parentRecPos-1), leftLastLink);
            parentPage->replace(parentRecPos-1, medianElement,
                    parentPage->link(parentRecPos-1));

            medianElement.release();
            return true;
        }
    } else {     // I assume here that rightNextPageId != -1

        int rightNextMedianPos = 0;
        if (rightNextPage->willRemainMinimallyFilledWithout(rightNextMedianPos)) {

            key_value_copy medianElement(rightNextPage->record(rightNextMedianPos));
            int rightMedLink = rightNextPage->hasLinks() ? rightNextPage->link(rightNextMedianPos) : -1;
            rightNextPage->remove(rightNextMedianPos);
            _fileStorage->writePage(rightNextPage);

            page->insert((int)page->recordCount(), parentPage->record(parentRecPos), page->hasLinks() ? page->lastLink() : -1);
            if (page->hasLinks()) page->relink((int)page->recordCount(), rightMedLink);
            parentPage->replace(parentRecPos, medianElement,
                    parentPage->link(parentRecPos));

            medianElement.release();
            return true;
        }
    }

    return false;
}


void database::_checkAndRemoveEmptyRoot()
{
    assert( _fileStorage->rootPage()->hasLinks() );
    if (_fileStorage->rootPage()->recordCount() > 0) return;

    int actualRootId = _fileStorage->rootPage()->lastLink();
    db_page *oldRoot = _fileStorage->rootPage();

    _fileStorage->changeRootPage(_fileStorage->fetchPage(actualRootId));
    _fileStorage->deallocateAndRelease(oldRoot);
}


bool database::_removeFromNode(db_page *nodePage, int parentPageId, int parentRecPos, int recPos)
{
    key_value_copy mostLeftElement;
    int nextPageId = nodePage->link(recPos+1);
    int nextParPageId = nodePage->index();
    _fileStorage->releasePage(nodePage);

    _rRemoveFromNodeR(nextPageId, nextParPageId, recPos+1, mostLeftElement, false);

    nodePage = _fileStorage->fetchPage(nextParPageId);
    nodePage->replace(recPos, mostLeftElement, nodePage->link(recPos));
    _fileStorage->writePage(nodePage);
    _fileStorage->releasePage(nodePage);
    mostLeftElement.release();

    if (_testRebalanceAfterDelete(nextPageId, nextParPageId, recPos+1)) {
        return  _testRebalanceAfterDelete(nextParPageId, parentPageId, parentRecPos);
    }

    return false;
}


bool database::_rRemoveFromNodeR(int pageId, int parentPageId, int parentRecPos,
                                 key_value_copy &element, bool canRebalance)
{
    db_page *page = _fileStorage->fetchPage(pageId);
    auto keyIt = page->begin();

    if (!page->hasLinks()) {
        element = page->record(keyIt.position());
        page->remove(keyIt.position());
        if (page->isMinimallyFilled()) {
            _fileStorage->writePage(page);
            _fileStorage->releasePage(page);
            return false;
        }

        if (canRebalance) {
            bool ret = _makePageMinimallyFilled(page, parentPageId, parentRecPos);
            _fileStorage->releasePage(page);
            return ret;
        } else {
            _fileStorage->writePage(page);
            _fileStorage->releasePage(page);
            return false;
        }
    }

    int nextPageId = keyIt.link();
    int nextParPageId = page->index();
    _fileStorage->releasePage(page);

    if (_rRemoveFromNodeR(nextPageId, nextParPageId, keyIt.position(), element, true)) {
        if (canRebalance) return _testRebalanceAfterDelete(pageId, parentPageId, parentRecPos);
    }
    return false;
}


bool database::_testRebalanceAfterDelete(int pageId, int parentPageId, int parentRecPos)
{
    if (pageId != _fileStorage->rootPage()->index()) {
        db_page *page = _fileStorage->fetchPage(pageId);
        bool ret = false;
        if (!page->isMinimallyFilled()) {
            ret = _makePageMinimallyFilled(page, parentPageId, parentRecPos);
        }
        _fileStorage->releasePage(page);
        return ret;

    } else _checkAndRemoveEmptyRoot();

    return false;
}
