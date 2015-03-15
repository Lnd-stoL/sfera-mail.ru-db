
#ifndef DB_PAGE_INCLUDED
#define DB_PAGE_INCLUDED

//----------------------------------------------------------------------------------------------------------------------

#include <string>
#include <vector>
using std::string;

#include <unistd.h>
#include <fcntl.h>

#include "mydb.hpp"

//----------------------------------------------------------------------------------------------------------------------

class btree_node
{
private:
    bool  _isLeaf;

    std::vector<db_data_entry>  _records;
    std::vector<__uint32_t>     _children;


public:
    bool isLeaf() const       { return _isLeaf;   }
    bool isLeaf(bool isleaf)  { _isLeaf = isleaf; }

    std::vector<__uint32_t>&    children()   { return _children; }
    std::vector<db_data_entry>& records()    { return _records;  }
};

//----------------------------------------------------------------------------------------------------------------------

class db_page
{
private:
    __uint32_t  _index;
    const db_file &_dataFile;
    btree_node  _btrnode;

    __uint8_t  *_pageRawData = nullptr;


private:
    void _readBtreeNode();
    void _writeBtreeNode();

public:
    db_page (__uint32_t index, const db_file &dataFile);
    ~db_page();

    void initialize();
    void loadNode();
    void writeToFile();

    size_t index() const;
    btree_node& btreeNode()  { return _btrnode; }
};

//----------------------------------------------------------------------------------------------------------------------

#endif
