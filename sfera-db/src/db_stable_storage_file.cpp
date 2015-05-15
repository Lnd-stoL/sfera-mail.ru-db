
#include "db_stable_storage_file.hpp"

#include <cassert>
#include <cstdlib>
#include <exception>
#include <stdexcept>

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

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
