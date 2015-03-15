
#include "db_page.hpp"

//----------------------------------------------------------------------------------------------------------------------

db_page::db_page(int index, binary_data pageBytes) :
        _index (index),
        _pageSize (pageBytes.length()),
        _pageBytes (pageBytes.byteDataPtr())
{
    _recordCount = _pageBytesUint16(0);
    _dataBlockEndOff = _pageBytesUint16(sizeof(uint16_t));
    _hasLinks = _pageBytes[2*sizeof(uint16_t)] != 0;
    _indexTable = _pageBytes + 2 * sizeof(uint16_t) + 1;
}


void db_page::initializeEmpty(bool hasLinks)
{
    _pageBytesUint16(0, 0);            // record count
    _pageBytesUint16(0, (uint16_t)_pageSize);    // data block end offset
    _pageBytes[2*sizeof(uint16_t)] = (uint8_t) hasLinks;
}


bool db_page::isFull() const
{
    return double(_freeBytes()) <= double(_pageSize) * 0.5;
}


size_t db_page::recordCount() const
{
    return _recordCount;
}


bool db_page::hasLinks() const
{
    return _hasLinks;
}


int db_page::link(int position) const
{
    if (!hasLinks())  return 0;
    return _recordIndex(position)[4];
}


db_data_entry db_page::record(int position) const
{
    return db_data_entry(key(position), value(position));
}


binary_data db_page::key(int position) const
{
    auto recordIndex = _recordIndex(position);
    uint8_t *ptr = _pageBytes + recordIndex[0];
    return binary_data(ptr, recordIndex[1]);
}


binary_data db_page::value(int position) const
{
    auto recordIndex = _recordIndex(position);
    uint8_t *ptr = _pageBytes + recordIndex[2];
    return binary_data(ptr, recordIndex[3]);
}


void db_page::insert(int position, db_data_entry data, int linked)
{
    // todo here
}


db_page::key_iterator db_page::begin() const
{
    return db_page::key_iterator(*this, 0);
}


db_page::key_iterator db_page::end() const
{
    return db_page::key_iterator(*this, (int)recordCount());
}

//----------------------------------------------------------------------------------------------------------------------

db_page::key_iterator::key_iterator(const db_page &page, int position) : _page (page), _position (position)
{ }


bool db_page::key_iterator::operator != (const db_page::key_iterator &rhs)
{
    return _position != rhs._position;
}


db_page::key_iterator db_page::key_iterator::operator ++ (int i)
{
    return this->operator+=(1);
}


binary_data db_page::key_iterator::operator * ()
{
    return _page.key(_position);
}


int db_page::key_iterator::operator - (db_page::key_iterator const &rhs)
{
    return _position - rhs._position;
}


db_page::key_iterator db_page::key_iterator::operator += (int offset)
{
    int newPosition = (int)_position + offset;
    if (newPosition >= _page.recordCount() || newPosition < 0) {
        return _page.end();
    }

    return db_page::key_iterator(_page, newPosition);
}
