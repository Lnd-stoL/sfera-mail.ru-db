#include <glob.h>
#include "db_page.hpp"
#include "db_file.hpp"

//----------------------------------------------------------------------------------------------------------------------



//----------------------------------------------------------------------------------------------------------------------

size_t db_page::index() const
{
    return _index;
}


db_page::db_page(__uint32_t index, const db_file &dataFile) : _index(index), _dataFile(dataFile)
{
    _pageRawData = (__uint8_t *)malloc(dataFile.config().pageSize());
    dataFile.rawRead(dataFile.pageInFileOffset(index), _pageRawData, dataFile.config().pageSize());
    _readBtreeNode();
}


void db_page::_readBtreeNode()
{
    __uint16_t inPageOffet = 1;    // sizeof metadata
    __uint8_t pageMetaInfo = *_pageRawData;
    _btrnode.isLeaf(pageMetaInfo & 1 != 0);

    __uint16_t recordCount = *(__uint16_t *)(_pageRawData + inPageOffet);  inPageOffet += sizeof(recordCount);
    _btrnode.records().resize(recordCount);
    if (_btrnode.isLeaf())  _btrnode.children().resize(recordCount+1);

    for (__uint16_t i = 0; i < recordCount; ++i) {

        if (_btrnode.isLeaf()) {
            __uint32_t  childId = *(__uint32_t *)(_pageRawData + inPageOffet);  inPageOffet += sizeof(childId);
            _btrnode.children()[i] = childId;
        }
        __uint16_t keyLength = *(__uint16_t *)(_pageRawData + inPageOffet);  inPageOffet += sizeof(keyLength);
        __uint16_t valLength = *(__uint16_t *)(_pageRawData + inPageOffet);  inPageOffet += sizeof(valLength);

        db_data_entry record = { binary_data(_pageRawData + inPageOffet, keyLength),
                                 binary_data(_pageRawData + inPageOffet + keyLength, valLength) };

        inPageOffet += keyLength + valLength;
        _btrnode.records()[i] = record;
    }
}


void db_page::_writeBtreeNode()
{
    __uint16_t inPageOffet = 1;                          // sizeof metadata
    _pageRawData[0] &= (__uint8_t) _btrnode.isLeaf();    // meta info
}


db_page::~db_page()
{
    free(_pageRawData);
}


void db_page::writeToFile()
{
    _writeBtreeNode();
    _dataFile.rawWrite(_dataFile.pageInFileOffset(_index), _pageRawData, _dataFile.config().pageSize());
}


void db_page::initialize()
{

}


void db_page::loadNode()
{

}
