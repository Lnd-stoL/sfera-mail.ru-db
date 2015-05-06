
#ifndef SFERA_DB_RAW_FILE_H
#define SFERA_DB_RAW_FILE_H

//----------------------------------------------------------------------------------------------------------------------

#include <unistd.h>
#include <string>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    class raw_file
    {
    private:
        int _unixFD = -1;
        size_t _actualFileSize = 0;


    private:
        raw_file() { };

    public:
        ~raw_file();
        static raw_file* createNew(const std::string& path, bool writeOnly = false);
        static raw_file* openExisting(const std::string& path, bool readOnly = false);
        static bool exists(const std::string& path);

        void  ensureSizeIsAtLeast(size_t neededSize);
        off_t writeAll(off_t offset, const void *data, size_t length);
        off_t readAll(off_t offset, void *data, size_t length) const;

        inline size_t actualSize() const  { return _actualFileSize; }
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif    //SFERA_DB_RAW_FILE_H
