
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