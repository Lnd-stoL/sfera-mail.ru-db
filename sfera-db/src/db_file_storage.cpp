
#include "db_file_storage.hpp"
#include "syscall_checker.h"

#include <iostream>
#include <cassert>
#include <limits>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

//----------------------------------------------------------------------------------------------------------------------

auto db_file_storage::openExisting(const std::string &fileName) -> db_file_storage*
{
    auto dbFile = new db_file_storage();

    dbFile->_unixFD = ::open(fileName.c_str(), O_RDWR);
    if (dbFile->_unixFD == -1) {
        delete dbFile;
        syscall_check(dbFile->_unixFD);
    }

    struct stat fileStat = {};
    int statResult = ::stat(fileName.c_str(), &fileStat);
    if (statResult == -1) {
        delete dbFile;
        syscall_check(statResult);
    }

    dbFile->_actualFileSize = size_t(fileStat.st_size);

    dbFile->_load();
    return dbFile;
}


auto db_file_storage::createEmpty(const std::string &fileName, db_file_storage_config const &config) -> db_file_storage *
{
    auto dbFile = new db_file_storage();

    dbFile->_unixFD = ::creat(fileName.c_str(), 0777);
    if (dbFile->_unixFD == -1) {
        delete dbFile;
        syscall_check(-1);
    }
    ::close(dbFile->_unixFD);

    dbFile->_unixFD = ::open(fileName.c_str(), O_RDWR);
    if (dbFile->_unixFD == -1) {
        delete dbFile;
        syscall_check(-1);
    }

    dbFile->_pageSize = config.pageSize;
    dbFile->_initializeEmpty(config.maxStorageSize);
    return dbFile;
}


db_file_storage::~db_file_storage()
{
    _pagesCache.clearCache();

    if (_pagesMetaTable) {
        ::free(_pagesMetaTable);
    }

    if (_unixFD != -1) {
        _rawFileWrite(_lastFreePage_InfileOffset, &_lastFreePage, sizeof(int32_t));
        ::close(_unixFD);
    }
}


void db_file_storage::_initializeEmpty(size_t maxStorageSize)
{
    off_t offset = _rawFileWrite(_pageSize_InfileOffset, &_pageSize, sizeof(_pageSize));

    _maxPageCount = maxStorageSize / _pageSize;
    _initPagesMetaTableByteSize();

    offset = _rawFileWrite(offset, &_maxPageCount, sizeof(_maxPageCount));
    _lastFreePage_InfileOffset = offset;
    offset = _rawFileWrite(offset, &_lastFreePage, sizeof(_lastFreePage));
    _rootPageId_InfileOffset = offset;
    offset += sizeof(int); // root page id placeholder

    _pagesMetaTableStartOffset = (size_t) offset;
    _ensureFileSize(offset + _pagesMetaTableSize);

    _pagesStartOffset = offset + _pagesMetaTableSize;
    _pagesMetaTable = (uint8_t*) calloc(_pagesMetaTableSize, 1);
}


void db_file_storage::_load()
{
    off_t offset = 0;
    offset = _rawFileRead(offset, &_pageSize, sizeof(_pageSize));

    offset = _rawFileRead(offset, &_maxPageCount, sizeof(_maxPageCount));
    offset = _rawFileRead(offset, &_lastFreePage, sizeof(_lastFreePage));
    offset = _rawFileRead(offset, &_rootPageId,   sizeof(_rootPageId));

    _pagesMetaTableStartOffset = offset;
    _initPagesMetaTableByteSize();
    _pagesStartOffset = offset + _pagesMetaTableSize;

    _pagesMetaTable = (uint8_t *) malloc(_pagesMetaTableSize);
    _rawFileRead(_pagesMetaTableStartOffset, _pagesMetaTable, _pagesMetaTableSize);
}


off_t db_file_storage::_rawFileWrite(off_t offset, const void *data, size_t length) const
{
    for (size_t writtenBytes = 0; writtenBytes < length;) {
        ssize_t writeResult = ::pwrite(_unixFD, (uint8_t *)data + writtenBytes,
                                      length - writtenBytes, offset + writtenBytes);
        if (writeResult <= 0) {
            std::cerr << "db_file_storage[rawFileWrite()]: error writing to file (pwrite returned <= 0): errno=" << errno << std::endl;
            return 0;
        }
        writtenBytes += writeResult;
    }

    return offset + length;
}


off_t db_file_storage::_rawFileRead(off_t offset, void *data, size_t length) const
{
    for (size_t readBytes = 0; readBytes < length;) {
        ssize_t readResult = ::pread(_unixFD, (uint8_t *)data + readBytes, length - readBytes, offset + readBytes);
        if (readResult <= 0) {
            std::cerr << "db_file_storage[rawFileRead()]: error reading from file (pread returned <= 0): errno=" << errno << std::endl;
            return 0;
        }
        readBytes += readResult;
    }

    return offset + length;
}


void db_file_storage::_initPagesMetaTableByteSize()
{
    _pagesMetaTableSize = _maxPageCount / 8 + ((_maxPageCount % 8) ? 1 : 0);
}


int db_file_storage::_getNextFreePageIndex()
{
    int pageIndex = _lastFreePage + 1;

    unsigned currentByteOffset = (unsigned) (pageIndex / 8);
    unsigned char currentInByteOffset = (unsigned char) ( pageIndex % 8 );

    while (_pagesMetaTable[currentByteOffset] == 0xFF) {
        ++currentByteOffset;
        currentInByteOffset = 0;
        pageIndex = currentByteOffset * 8;
    }

    unsigned char currentByte = _pagesMetaTable[currentByteOffset];
    for (unsigned i = currentInByteOffset; i < 8; ++i) {
        if ((currentByte  & (1 << i)) == 0)  break;
        ++pageIndex;
    }

    return pageIndex;
}


void db_file_storage::_updatePageMetaInfo(int pageIndex, bool allocated)
{
    unsigned currentByteOffset = (unsigned) pageIndex / 8;
    unsigned char currentInByteOffset = (unsigned char) ( pageIndex % 8 );

    if (allocated) {
        _pagesMetaTable[currentByteOffset] |= (unsigned char) (1 << currentInByteOffset);
    } else {
        _pagesMetaTable[currentByteOffset] &= ~ (unsigned char) (1 << currentInByteOffset);
    }

    _rawFileWrite(_pagesMetaTableStartOffset + currentByteOffset, _pagesMetaTable + currentByteOffset, 1);
}


void db_file_storage::_ensureFileSize(size_t neededSize)
{
    if (_actualFileSize < neededSize) {
        syscall_check( ::ftruncate (_unixFD, neededSize) );
        _actualFileSize = neededSize;
    }
}


off_t db_file_storage::_pageOffset(int index) const
{
    assert(index >= 0 && index < _maxPageCount);
    return _pagesStartOffset + index * _pageSize;
}


void db_file_storage::writePage(db_page *page)
{
    assert(page != nullptr);

#ifndef _NDEBUG
        if (!page->wasChanged()) {
            std::cerr << "warning: writing non changed page #" << page->id() << std::endl;
            return;
        }
#endif

    _pagesCache.makeDirty(page);
    page->wasSaved();
}


db_page* db_file_storage::allocatePage(bool isLeaf)
{
    int pageId = _getNextFreePageIndex();

    if (_lastFreePage + 1 == pageId)  {
        _lastFreePage = pageId;
    }

    _updatePageMetaInfo(pageId, true);
    _ensureFileSize(_pageOffset(pageId) + _pageSize);

    uint8_t *rawPageBytes = (uint8_t *)::calloc(_pageSize, 1);
    db_page *page = db_page::createEmpty(pageId, data_blob(rawPageBytes, _pageSize), isLeaf);
    _pagesCache.cacheAndPin(page);

    return page;
}


void db_file_storage::deallocatePage(int pageId)
{
    assert( pageId >= 0 && pageId < _maxPageCount );

    _lastFreePage = pageId - 1;
    _updatePageMetaInfo(pageId, false);
    _pagesCache.invalidateCachedPage(pageId);
}


db_page* db_file_storage::fetchPage(int pageId)
{
    assert( pageId >= 0 && pageId < _maxPageCount );

    db_page *cachedVersion = _pagesCache.fetchAndPin(pageId);
    if (cachedVersion == nullptr) {

        uint8_t *rawPageBytes = (uint8_t *)::malloc(_pageSize);
        _rawFileRead(_pageOffset(pageId), rawPageBytes, _pageSize);

        cachedVersion = db_page::load(pageId, data_blob(rawPageBytes, _pageSize));
        _pagesCache.cacheAndPin(cachedVersion);
    }

    return cachedVersion;
}


void db_file_storage::changeRootPage(int pageId)
{
    assert( pageId >= 0 && pageId < _maxPageCount );

    _rootPageId = pageId;
    _diskWriteRootPageId();
}


void db_file_storage::_diskWriteRootPageId()
{
    _rawFileWrite(_rootPageId_InfileOffset, &_rootPageId, sizeof(_rootPageId));
}


void db_file_storage::releasePage(db_page *page)
{
    assert( page != nullptr );
    assert( !page->wasChanged() );    // normally the page has to be written to disk before releasing if it has been changed

    _pagesCache.unpin(page);
}


void db_file_storage::deallocateAndRelease(db_page *page)
{
    this->deallocatePage(page->id());
    this->releasePage(page);
}


void db_file_storage::writeAndRelease(db_page *page)
{
    this->writePage(page);
    this->releasePage(page);
}


void db_file_storage::_realWritePage(db_page *page)
{
    page->prepareForWriting();
    _rawFileWrite(_pageOffset(page->id()), page->bytes(), page->size());
}
