
#include "db_file.hpp"

#include <iostream>
#include <cassert>

using std::cout;
using std::endl;

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <glob.h>

//----------------------------------------------------------------------------------------------------------------------

mydb_internal_config::mydb_internal_config(size_t pageSize) : _pageSize (pageSize)
{ }


size_t mydb_internal_config::pageSize() const
{
    return _pageSize;
}

//----------------------------------------------------------------------------------------------------------------------

db_file::db_file(const string &fileName)
{
    int normalOpenFlags = O_RDWR;

    _unixFD = open(fileName.c_str(), normalOpenFlags);            // assuming the file exists already
    if (_unixFD == -1) {                                          // if not create it

        _unixFD = open(fileName.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0777);
        if (_unixFD == -1) {
            cout << "error opening or creating database file '" << fileName << "'" << endl;
        } else {
            close(_unixFD);
            _unixFD = open(fileName.c_str(), normalOpenFlags);    // reopen it for RW
        }
    }

    struct stat fileStat;
    stat(fileName.c_str(), &fileStat);
    _actualFileSize = (size_t)fileStat.st_size;
}


db_file::~db_file()
{
    if (_pagesMetaTable) {
        free(_pagesMetaTable);
    }

    if (_unixFD != -1) {
        rawFileWrite(sizeof(_basicConfig) + sizeof(int32_t), &_lastFreePage, sizeof(int32_t));
        close (_unixFD);
    }
}


void db_file::initializeEmpty(size_t maxDataSizeBytes, const mydb_internal_config &config)
{
    _basicConfig = config;
    off_t offset = rawFileWrite(0, &_basicConfig, sizeof(_basicConfig));

    _maxPageCount = maxDataSizeBytes / config.pageSize();
    _calcPagesMetaTableByteSize();

    offset = rawFileWrite(offset, &_maxPageCount, sizeof(int32_t));
    offset = rawFileWrite(offset, &_lastFreePage, sizeof(int32_t));
    off_t rootPageIdOffset = offset;
    offset = rawFileWrite(offset, &_rootPageId,   sizeof(int32_t));

    _pagesMetaTableStartOffset = (size_t) offset;
    _extentFileTo(offset + _pagesMetaTableByteSize);

    _pagesStartOffset = offset + _pagesMetaTableByteSize;
    _pagesMetaTable = (unsigned char *)calloc(_pagesMetaTableByteSize, 1);

    _rootPageId = this->allocPage();
    rawFileWrite(rootPageIdOffset, &_rootPageId, sizeof(int32_t));
}


off_t db_file::rawFileWrite(off_t offset, void *data, size_t length) const
{
    for (size_t writtenBytes = 0; writtenBytes < length;) {
        ssize_t writeResult = pwrite (_unixFD, (uint8_t *)data + writtenBytes,
                                      length - writtenBytes, offset + writtenBytes);
        if (writeResult <= 0) {
            std::cerr << "db_file[rawFileWrite()]: error writing to file (pwrite returned <= 0): errno=" << errno << endl;
            return 0;
        }
        writtenBytes += writeResult;
    }

    return offset + length;
}


off_t db_file::rawFileRead(off_t offset, void *data, size_t length) const
{
    for (size_t readBytes = 0; readBytes < length;) {
        ssize_t readResult = pread(_unixFD, (uint8_t *)data + readBytes, length - readBytes, offset + readBytes);
        if (readResult <= 0) {
            std::cerr << "db_file[rawFileRead()]: error reading from file (pread returned <= 0): errno=" << errno << endl;
            return 0;
        }
        readBytes += readResult;
    }

    return offset + length;
}


void db_file::load()
{
    assert(_maxPageCount == 0 && "The DB can be loaded only once");

    off_t offset = 0;
    offset = rawFileRead(offset, &_basicConfig, sizeof(_basicConfig));

    offset = rawFileRead(offset, &_maxPageCount, sizeof(int32_t));
    offset = rawFileRead(offset, &_lastFreePage, sizeof(int32_t));
    offset = rawFileRead(offset, &_rootPageId,   sizeof(int32_t));

    _pagesMetaTableStartOffset = (size_t) offset;
    _calcPagesMetaTableByteSize();
    _pagesStartOffset = offset + _pagesMetaTableByteSize;

    _pagesMetaTable = (unsigned char *) malloc(_pagesMetaTableByteSize);
    rawFileRead(_pagesMetaTableStartOffset, _pagesMetaTable, _pagesMetaTableByteSize);
}


db_page * db_file::loadPage(int pageIndex) const
{
    db_page *page = _loadPage(pageIndex);
    page->load();
    return page;
}


int db_file::allocPage()
{
    int pageIndex = _getNextFreePageIndex();

    if (_lastFreePage + 1 == pageIndex)  {
        _lastFreePage = pageIndex;
    }

    _updatePageMetaInfo(pageIndex, true);

    _extentFileTo(_pageByteOffset(pageIndex) + _pageSize());
    return pageIndex;
}


void db_file::freePage(db_page *page)
{
    assert(page != nullptr);

    _lastFreePage = page->index() - 1;
    _updatePageMetaInfo(page->index(), false);
}


void db_file::_calcPagesMetaTableByteSize()
{
    _pagesMetaTableByteSize = _maxPageCount / 8 + ((_maxPageCount % 8) ? 1 : 0);
}


int db_file::_getNextFreePageIndex()
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


void db_file::_updatePageMetaInfo(int pageIndex, bool allocated)
{
    unsigned currentByteOffset = (unsigned) pageIndex / 8;
    unsigned char currentInByteOffset = (unsigned char) ( pageIndex % 8 );

    if (allocated) {
        _pagesMetaTable[currentByteOffset] |= (unsigned char) (1 << currentInByteOffset);
    } else {
        _pagesMetaTable[currentByteOffset] &= ~ (unsigned char) (1 << currentInByteOffset);
    }

    rawFileWrite(_pagesMetaTableStartOffset + currentByteOffset, _pagesMetaTable + currentByteOffset, 1);
}


void db_file::_extentFileTo(size_t neededSize)
{
    if (_actualFileSize < neededSize) {
        syscall_check( ftruncate (_unixFD, neededSize) );
        _actualFileSize = neededSize;
    }
}


off_t db_file::_pageByteOffset(int index) const
{
    assert(index >= 0 && index < _maxPageCount);
    return _pagesStartOffset + index * _pageSize();
}


void db_file::writePage(db_page *page)
{
    assert(page != nullptr);

    if (!page->wasChanged()) {
        std::cout << "warning: writing non changed page #" << page->index() << std::endl;
        return;
    }

    page->prepareForWriting();
    rawFileWrite(_pageByteOffset(page->index()), page->bytes(), page->size());
    page->wasSaved();
}


db_page *db_file::_loadPage(int pageIndex) const
{
    assert(pageIndex >= 0 && pageIndex < _maxPageCount);

    uint8_t *rawPageBytes = (uint8_t *)malloc(_pageSize());
    rawFileRead(_pageByteOffset(pageIndex), rawPageBytes, _pageSize());

    return new db_page(pageIndex, binary_data(rawPageBytes, _pageSize()));
}


void db_file::changeRootPage(db_page *page)
{
    assert(page != nullptr);

    _rootPageId = page->index();
    rawFileWrite(2*sizeof(int32_t), &_rootPageId, sizeof(int32_t));
}
