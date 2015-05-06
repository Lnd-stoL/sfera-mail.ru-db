
#include "db_data_storage.hpp"
#include "syscall_checker.hpp"

#include <iostream>
#include <cassert>
#include <limits>

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

//----------------------------------------------------------------------------------------------------------------------

auto db_data_storage::openExisting(const std::string &dirPath, const db_data_storage_open_params &params)
-> db_data_storage *
{
    auto dbDataStorage = new db_data_storage();
    dbDataStorage->_stableStorageFile = db_stable_storage_file::openExisting(dirPath + "/" +
                                                                                     dbDataStorage->StableStorageFileName);
    dbDataStorage->_initializeCache(params.cacheSizeInPages);
    return dbDataStorage;
}


auto db_data_storage::createEmpty(const std::string &dirPath, db_data_storage_config const &config) -> db_data_storage *
{
    auto dbDataStorage = new db_data_storage();
    dbDataStorage->_stableStorageFile = db_stable_storage_file::createEmpty(dirPath + "/" +
                                                                            dbDataStorage->StableStorageFileName,
                                                                            config);
    dbDataStorage->_initializeCache(config.cacheSizeInPages);
    return dbDataStorage;
}


db_data_storage::~db_data_storage()
{
    _pagesCache->clearCache();
    delete _pagesCache;
    delete _stableStorageFile;
}


void db_data_storage::writePage(db_page *page)
{
    assert( page != nullptr );
    assert( page->wasChanged() );

    _pagesCache->makeDirty(page);
    page->wasSaved();
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


void db_data_storage::_realWritePage(db_page *page)
{
    _stableStorageFile->writePage(page);
}


void db_data_storage::_initializeCache(size_t sizeInPages)
{
    _pagesCache = new pages_cache(sizeInPages,
                                 [this](db_page *page) {
                                     this->_realWritePage(page);
                                 });
}
