
#include "db_data_storage.hpp"
#include "syscall_checker.hpp"

#include <iostream>
#include <cassert>
#include <limits>

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

//----------------------------------------------------------------------------------------------------------------------

const std::string db_data_storage::StableStorageFileName = "data.sdbs";
const std::string db_data_storage::LogFileName           = "log.sdbl";

//----------------------------------------------------------------------------------------------------------------------

auto db_data_storage::openExisting(const std::string &dirPath, const db_data_storage_open_params &params)
-> db_data_storage *
{
    auto dbDataStorage = new db_data_storage();
    dbDataStorage->_stableStorageFile = db_stable_storage_file::openExisting(dirPath + "/" + StableStorageFileName);

    db_binlog_recovery *binlog_recovery = new db_binlog_recovery(dirPath + "/" + dbDataStorage->LogFileName);
    if (!binlog_recovery->closedProperly()) {
        std::cerr << "warning: database wasn't closed peoperly last time -> applying recovery ..." << std::endl;
        binlog_recovery->doRecovery(dbDataStorage);
    }
    delete binlog_recovery;

    dbDataStorage->_initializeCache(params.cacheSizeInPages);
    dbDataStorage->_binlog = db_binlog_logger::openExisting(dirPath + "/" + dbDataStorage->LogFileName);

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

    _currentOperation->writesPage(page);    // instead add the page to the current operation's write set
    _pagesCache->makeDirty(page);
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

    _pagesCache->unpinIfClean(page);        // do nothing on dirty pages here because of no steal
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
