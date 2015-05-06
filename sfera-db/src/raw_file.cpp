
#include "raw_file.hpp"
#include "syscall_checker.hpp"

#include <iostream>

#include <fcntl.h>
#include <sys/stat.h>

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

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
    return false;
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
        readBytes += readResult;
    }

    return offset + length;
}
