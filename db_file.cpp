
#include "db_file.hpp"

#include <iostream>
#include <glob.h>

using std::cout;
using std::endl;

//----------------------------------------------------------------------------------------------------------------------

mydb_internal_config::mydb_internal_config (size_t pageSize) : _pageSize (pageSize)
{

}


size_t mydb_internal_config::pageSize() const
{
    return _pageSize;
}

//----------------------------------------------------------------------------------------------------------------------

db_file::db_file (const string &fileName)
{
    int normalOpenFlags = O_RDWR;

    _unixFD = open (fileName.c_str(), normalOpenFlags);            // assuming the file exists already
    if (_unixFD == -1) {                                           // if not create it

        _unixFD = open (fileName.c_str(), O_WRONLY | O_CREAT | O_EXCL);
        if (_unixFD == -1) {
            cout << "error opening or creating database file '" << fileName << "'" << endl;
        } else {
            close (_unixFD);
            _unixFD = open (fileName.c_str(), normalOpenFlags);    // reopen it for RW
        }
    }
}


db_file::~db_file()
{
    if (_pagesMetaTable) {
        free (_pagesMetaTable);
    }

    if (_unixFD != -1) {
        _write (sizeof (_basicConfig) + sizeof (__int32_t), &_lastFreePage, sizeof (__int32_t));
        close (_unixFD);
    }
}


void db_file::initialize (size_t maxDataSizeBytes, const mydb_internal_config& config)
{
    _basicConfig = config;
    off_t offset = _write (0, &_basicConfig, sizeof (_basicConfig));

    _maxPageCount = maxDataSizeBytes / config.pageSize();
    _calcPagesMetaTableByteSize();

    offset = _write (offset, &_maxPageCount, sizeof (__int32_t));
    offset = _write (offset, &_lastFreePage, sizeof (__int32_t));
    _pagesMetaTableStartOffset = (size_t) offset;
    ftruncate (_unixFD, offset + _pagesMetaTableByteSize);

    _pagesStartOffset = offset + _pagesMetaTableByteSize;
    _pagesMetaTable = (unsigned char *) calloc (_pagesMetaTableByteSize, 1);
}


off_t db_file::_write (off_t offset, void *data, size_t length)
{
    for (size_t writtenBytes = 0; writtenBytes < length;) {
        ssize_t writeResult = pwrite (_unixFD, data + writtenBytes, length - writtenBytes, offset + writtenBytes);

        if (writeResult == -1) {
            cout << "error writing to file: " << errno << endl;
            return 0;
        }
        writtenBytes += writeResult;
    }

    return offset + length;
}


off_t db_file::_read (off_t offset, void *data, size_t length)
{
    for (size_t readBytes = 0; readBytes < length;) {
        ssize_t readResult = pread (_unixFD, data + readBytes, length - readBytes, offset + readBytes);
        if (readResult == -1 || readResult == 0)  return 0;
        readBytes += readResult;
    }

    return offset + length;
}


void db_file::load()
{
    off_t offset = 0;
    offset = _read (offset, &_basicConfig, sizeof (_basicConfig));

    offset = _read (offset, &_maxPageCount, sizeof (__int32_t));
    offset = _read (offset, &_lastFreePage, sizeof (__int32_t));

    _pagesMetaTableStartOffset = (size_t) offset;
    _calcPagesMetaTableByteSize();
    _pagesStartOffset = offset + _pagesMetaTableByteSize;

    _pagesMetaTable = (unsigned char *) malloc (_pagesMetaTableByteSize);
    _read (_pagesMetaTableStartOffset, _pagesMetaTable, _pagesMetaTableByteSize);
}


db_page * db_file::loadPage (size_t pageIndex)
{
    return new db_page (pageIndex);
}


db_page * db_file::allocPage()
{
    size_t pageIndex = _getNextFreePageIndex();

    if (_lastFreePage+1 == pageIndex)  {
        _lastFreePage = (__int32_t) pageIndex;
    }

    _writePageMetaInfo (pageIndex, true);
    return loadPage (pageIndex);
}


void db_file::freePage (db_page *page)
{
    _lastFreePage = (__int32_t) page->index() - 1;
    _writePageMetaInfo (page->index(), false);
}


void db_file::_calcPagesMetaTableByteSize()
{
    _pagesMetaTableByteSize = _maxPageCount / 8 + ((_maxPageCount % 8) ? 1 : 0);
}


size_t db_file::_getNextFreePageIndex()
{
    size_t pageIndex = (size_t) _lastFreePage + 1;

    unsigned currentByteOffset = (unsigned) pageIndex / 8;
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


void db_file::_writePageMetaInfo (size_t pageIndex, bool allocated)
{
    unsigned currentByteOffset = (unsigned) pageIndex / 8;
    unsigned char currentInByteOffset = (unsigned char) ( pageIndex % 8 );

    if (allocated) {
        _pagesMetaTable[currentByteOffset] |= (unsigned char) (1 << currentInByteOffset);
    } else {
        _pagesMetaTable[currentByteOffset] &= ~ (unsigned char) (1 << currentInByteOffset);
    }

    _write (_pagesMetaTableStartOffset + currentByteOffset, _pagesMetaTable + currentByteOffset, 1);
}
