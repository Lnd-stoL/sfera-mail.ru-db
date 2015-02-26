
#include "db_file.hpp"
#include "db_page.hpp"

#include <iostream>
#include <fcntl.h>
#include <glob.h>
#include <stdio.h>
#include <sys/stat.h>

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

        _unixFD = open (fileName.c_str(), O_WRONLY | O_CREAT);
        if (_unixFD == -1) {
            cout << "error opening or creating database file '" << fileName << "'" << endl;
        } else {
            close (_unixFD);
            _unixFD = open (fileName.c_str(), normalOpenFlags);    // reopen it for RW
        }
    }

    struct stat buf;
    fstat (_unixFD, &buf);
    _realFileSize = (size_t) buf.st_size;
}


db_file::~db_file()
{
    if (_unixFD != -1) {
        close (_unixFD);
    }
}


void db_file::initialize (size_t maxDataSizeBytes, const mydb_internal_config& config)
{
    _basicConfig = config;
    _write (0, (unsigned char *) &_basicConfig, sizeof (_basicConfig));

    off_t offset = sizeof (_basicConfig);
    size_t maxPagesNum = maxDataSizeBytes / config.pageSize();
    size_t pagesTableBytes = maxPagesNum / 8;
    offset += _write (offset, (unsigned char *) &pagesTableBytes, sizeof (pagesTableBytes));
    ftruncate (_unixFD, offset + pagesTableBytes);
}


off_t db_file::_write (off_t offset, unsigned char *data, size_t length)
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


void db_file::_read (off_t offset, unsigned char *data, size_t length)
{
    for (size_t readBytes = 0; readBytes < length;) {
        ssize_t readResult = pread (_unixFD, data + readBytes, length - readBytes, offset + readBytes);
        if (readResult == -1 || readResult == 0)  return;
        readBytes += readResult;
    }
}


void db_file::load()
{
    _read (0, (unsigned char *) &_basicConfig, sizeof (_basicConfig));
}
