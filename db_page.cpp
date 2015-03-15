
#include "db_page.hpp"

#include <cassert>
#include <algorithm>

//----------------------------------------------------------------------------------------------------------------------
// page layout brief description
//----------------------------------------------------------------------------------------------------------------------
//
//  offset |  size  |  field meaning
//  --------------------------------------------------------------------------------------------------------------------
//  0      | uint16 | record count the page contains (record = key+value entry and a link to the child btree node)
//  2      | uint16 | data_block_end (the data contains of keys and values BLOBs and is placed to the end of the page)
//  4      | byte   | meta information (actually a byte indicating if the page is btree leaf [0] or not [1])
//  5      | ------ | data entry index blocks:
//                      block consists of three integers ( uint16 ) + one 32-bit integer
//                          at 0 - key_value blob offset within the page
//                          at 2 - key length in bytes
//                          at 4 - value length in bytes
//                          at 6 - [if not a leaf] ID of a page which is a btree child node coming BEFORE the key
//   ===== FREE SPACE =====
//   ===== ACTUAL VALUES AND KEYS BINARY DATA ===== - from data_block_end to the end of the page
//
//----------------------------------------------------------------------------------------------------------------------

db_page::key_iterator::key_iterator(const db_page *page, int position) : _page (page), _position (position)
{ }


bool db_page::key_iterator::operator != (const db_page::key_iterator &rhs) const
{
    assert(_page == rhs._page);
    return _position != rhs._position;
}


bool db_page::key_iterator::operator == (const db_page::key_iterator &rhs) const
{
    return !(this->operator!=(rhs));
}


db_page::key_iterator& db_page::key_iterator::operator = (const db_page::key_iterator &rhs)
{
    _page = rhs._page;
    _position = rhs._position;
}


db_page::key_iterator db_page::key_iterator::operator ++ (int i)
{
    db_page::key_iterator tmp = *this;
    tmp.operator+=(1);
    return tmp;
}


db_page::key_iterator db_page::key_iterator::operator ++ ()
{
    this->operator+=(1);
}


binary_data db_page::key_iterator::operator * ()
{
    return _page->key(_position);
}


int db_page::key_iterator::operator - (db_page::key_iterator const &rhs)
{
    assert(_page == rhs._page);
    return _position - rhs._position;
}


db_page::key_iterator db_page::key_iterator::operator += (int offset)
{
    int newPosition = (int)_position + offset;
    if (newPosition >= _page->recordCount() || newPosition < 0) {
        *this = _page->end();
    }

    *this = db_page::key_iterator(_page, newPosition);
}


binary_data db_page::key_iterator::value() const
{
    return _page->value(_position);
}


int db_page::key_iterator::link() const
{
    return _page->link(_position);
}

//----------------------------------------------------------------------------------------------------------------------

db_page::db_page(int index, binary_data pageBytes) :
        _index (index),
        _pageSize (pageBytes.length()),
        _pageBytes (pageBytes.byteDataPtr())
{
    _indexTable = _pageBytes + 2 * sizeof(uint16_t) + 1;
}


void db_page::initializeEmpty(bool hasLinks)
{
    _wasChanged = true;
    _pageBytesUint16(0, 0);                      // record count

    _dataBlockEndOffset = _pageSize;
    _pageBytesUint16(0, (uint16_t) _dataBlockEndOffset);    // data block end offset
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
    assert(_hasLinks);
    if (!hasLinks())  return 0;
    return _recordIndexRawPtr(position)[3];
}


db_data_entry db_page::record(int position) const
{
    return db_data_entry(key(position), value(position));
}


binary_data db_page::key(int position) const
{
    auto recordIndex = _recordIndex(position);
    uint8_t *ptr = _pageBytes + recordIndex.keyValueOffset;
    return binary_data(ptr, recordIndex.keyLength);
}


binary_data db_page::value(int position) const
{
    auto recordIndex = _recordIndex(position);
    uint8_t *ptr = _pageBytes + recordIndex.valueOffset();
    return binary_data(ptr, recordIndex.valueLength);
}


void db_page::insert(int position, db_data_entry data, int linked)
{
    size_t requiredByteCount = data.length() + _recordIndexSize();
    assert(_freeBytes() >= requiredByteCount);

    _dataBlockEndOffset -= data.length();
    std::copy(data.key().byteDataPtr(), data.key().byteDataEnd(), _pageBytes + _dataBlockEndOffset);
    std::copy(data.value().byteDataPtr(), data.value().byteDataEnd(), _pageBytes + _dataBlockEndOffset + data.key().length());

    _insertRecordIndex(position, record_index(_dataBlockEndOffset, data.key().length(), data.value().length()));
    _wasChanged = true;
}


db_page::key_iterator db_page::begin() const
{
    return db_page::key_iterator(this, 0);
}


db_page::key_iterator db_page::end() const
{
    return db_page::key_iterator(this, (int)_recordCount);
}


db_page::record_index db_page::_recordIndex(int position) const
{
    auto rawPtr = _recordIndexRawPtr(position);
    return record_index(rawPtr[0], rawPtr[1], rawPtr[2]);
}


void db_page::_insertRecordIndex(int position, db_page::record_index const &recordIndex)
{
    assert(_auxInfoSize() + _recordIndexSize() <= _dataBlockEndOffset);

    std::copy_backward((uint8_t *)_recordIndexRawPtr(position),
                       _pageBytes + _auxInfoSize(),
                       _pageBytes + _auxInfoSize() + _recordIndexSize());

    auto rawPtr = _recordIndexRawPtr(position);
    rawPtr[0] = recordIndex.keyValueOffset;
    rawPtr[1] = recordIndex.keyLength;
    rawPtr[2] = recordIndex.valueLength;

    _recordCount++;
}


void db_page::prepareForWriting()
{
    _pageBytesUint16(0,                (uint16_t)_recordCount);
    _pageBytesUint16(sizeof(uint16_t), (uint16_t)_dataBlockEndOffset);
}


void db_page::load()
{
    _recordCount = _pageBytesUint16(0);
    _dataBlockEndOffset = _pageBytesUint16(sizeof(uint16_t));
    _hasLinks = _pageBytes[2*sizeof(uint16_t)] != 0;
}


db_page::~db_page()
{
    if (_pageBytes) {
        free(_pageBytes);
        _pageBytes = nullptr;
    }
}


void db_page::insert(db_page::key_iterator position, db_data_entry data, int linked)
{
    insert(position.position(), data, linked);
}
