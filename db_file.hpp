
#ifndef DB_FILE_INCLUDED
#define DB_FILE_INCLUDED

//----------------------------------------------------------------------------------------------------------------------

#include <string>
using std::string;

#include <unistd.h>
#include <fcntl.h>

//----------------------------------------------------------------------------------------------------------------------

class mydb_internal_config
{
private:
    size_t _pageSize;

public:
    mydb_internal_config (size_t pageSize);
    mydb_internal_config() { }

    size_t pageSize() const;
};

//----------------------------------------------------------------------------------------------------------------------

class db_file
{
private:
    int _unixFD = -1;
    mydb_internal_config  _basicConfig;
    size_t _realFileSize = 0;


private:
    off_t _write (off_t offset, unsigned char *data, size_t length);
    void _read (off_t offset, unsigned char *data, size_t length);


public:
    db_file (const string &fileName);
    ~db_file();

    void initialize (size_t maxDataSizeBytes, const mydb_internal_config& config);
    void load();
};

//----------------------------------------------------------------------------------------------------------------------

#endif
