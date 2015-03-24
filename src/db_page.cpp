
#include "db_page.hpp"

#include <cassert>
#include <algorithm>
#include <glob.h>

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
    return *this;
}


db_page::key_iterator db_page::key_iterator::operator ++ (int i)
{
    db_page::key_iterator tmp = *this;
    this->operator+=(1);
    return tmp;
}


db_page::key_iterator db_page::key_iterator::operator ++ ()
{
    this->operator+=(1);
    return *this;
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
    return *this;
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
    _hasLinks = hasLinks;
    _wasChanged = true;
    _pageBytesUint16(0, 0);                      // record count

    _dataBlockEndOffset = _pageSize;
    _pageBytesUint16(0, (uint16_t) _dataBlockEndOffset);    // data block end offset
    _pageBytes[2*sizeof(uint16_t)] = (uint8_t) hasLinks;
}


bool db_page::isFull() const
{
    return double(usedBytes()) / _pageSize * 100 >= maximallyFullPercent;
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
    assert(_pageBytes != nullptr);
    assert(_hasLinks);
    assert(position >= 0 && position <= _recordCount);

    if (!hasLinks())  return 0;
    return *(int32_t *)(_recordIndexRawPtr(position) + 3);
}


db_data_entry db_page::record(int position) const
{
    return db_data_entry(key(position), value(position));
}


binary_data db_page::key(int position) const
{
    assert(_pageBytes != nullptr);
    assert(position >= 0 && position < _recordCount);

    auto recordIndex = _recordIndex(position);
    uint8_t *ptr = _pageBytes + recordIndex.keyValueOffset;
    return binary_data(ptr, recordIndex.keyLength);
}


binary_data db_page::value(int position) const
{
    assert(_pageBytes != nullptr);
    assert(position >= 0 && position < _recordCount);

    auto recordIndex = _recordIndex(position);
    uint8_t *ptr = _pageBytes + recordIndex.valueOffset();
    return binary_data(ptr, recordIndex.valueLength);
}


void db_page::insert(int position, db_data_entry data, int linked)
{
    assert(_pageBytes != nullptr);
    assert(position >= 0 && position < _recordCount+1);
    assert(possibleToInsert(data));
    assert(hasLinks() || linked == -1);

    _dataBlockEndOffset -= data.length();
    std::copy(data.key().byteDataPtr(), data.key().byteDataEnd(), _pageBytes + _dataBlockEndOffset);
    std::copy(data.value().byteDataPtr(), data.value().byteDataEnd(), _pageBytes + _dataBlockEndOffset + data.key().length());

    _insertRecordIndex(position, record_index(_dataBlockEndOffset, data.key().length(), data.value().length()), linked);
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


void db_page::_insertRecordIndex(int position, db_page::record_index const &recordIndex, int linked)
{
    assert(_auxInfoSize() + _recordIndexSize() <= _dataBlockEndOffset);

    std::copy_backward((uint8_t *)_recordIndexRawPtr(position),
                       _pageBytes + _auxInfoSize(),
                       _pageBytes + _auxInfoSize() + _recordIndexSize());

    auto rawPtr = _recordIndexRawPtr(position);
    rawPtr[0] = recordIndex.keyValueOffset;
    rawPtr[1] = recordIndex.keyLength;
    rawPtr[2] = recordIndex.valueLength;
    if (_hasLinks) relink(position, linked);

    _recordCount++;
}


void db_page::prepareForWriting()
{
    assert(_pageBytes != nullptr);

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
    assert(position.associatedPage() == this);
    insert(position.position(), data, linked);
}


void db_page::relink(int position, int linked)
{
    assert(_pageBytes != nullptr);
    assert(_hasLinks);
    assert(position >= 0 && position <= _recordCount);

    *(int32_t *)(_recordIndexRawPtr(position) + 3) = linked;
    _wasChanged = true;
}


bool db_page::possibleToInsert(db_data_entry element)
{
    return _freeBytes() >= element.length() + _recordIndexSize();
}


void db_page::remove(int position)
{
    assert(_pageBytes != nullptr);
    assert(position >= 0 && position < _recordCount);
    if (position < 0 || position >= _recordCount)  return;

    auto indexBlock = _recordIndex(position);
    std::copy_backward(_pageBytes + _dataBlockEndOffset, _pageBytes + indexBlock.keyValueOffset,
            _pageBytes + indexBlock.dataEnd());

    std::copy((uint8_t *)_recordIndexRawPtr(position+1), _pageBytes + _auxInfoSize(),
            (uint8_t *)_recordIndexRawPtr(position));
    _recordCount--;

    for (int i = 0; i < _recordCount; ++i) {
        auto nextIndex = _recordIndex(i);
        if (nextIndex.keyValueOffset < indexBlock.keyValueOffset) {
            auto rawPtr = _recordIndexRawPtr(i);
            rawPtr[0] += indexBlock.length();
        }
    }

    _dataBlockEndOffset += indexBlock.length();
    _wasChanged = true;
}


bool db_page::isMinimallyFilled() const
{
    return double(usedBytes()) / _pageSize * 100 >= minimallyFullPercent;
}


bool db_page::willRemainMinimallyFilledWithout(int position) const
{
    assert(_pageBytes != nullptr);
    assert(position >= 0 && position < _recordCount);

    size_t realPageSize = _pageSize;
    _pageSize += _recordIndex(position).length();
    bool result = isMinimallyFilled();
    _pageSize = realPageSize;

    return result;
}


void db_page::replace(int position, binary_data newValue)
{
    assert(_pageBytes != nullptr);
    assert(position >= 0 && position < _recordCount);

    auto index = _recordIndex(position);
    binary_data key (malloc(index.keyLength), index.keyLength);
    std::copy(_pageBytes + index.keyValueOffset, _pageBytes + index.keyValueOffset + index.keyLength, key.byteDataPtr());
    int linked = -1;
    if (_hasLinks) linked = link(position);

    remove(position);
    insert(position, db_data_entry(key, newValue), linked);
    key.free();
}


db_page *db_page::splitEquispace(db_page *rightPage, int& medianPosition)
{
    assert(_pageBytes != nullptr);
    assert(_recordCount >= 3);    // at least 3 records for correct split

    db_page * leftPage = new db_page(_index, binary_data(malloc(_pageSize), _pageSize));
    leftPage->initializeEmpty(_hasLinks);

    size_t accumulatedSize = 0;
    size_t neededSize = (_pageSize - _freeBytes()) / 2;
    int copiedRecordCount = 0;

    for (; copiedRecordCount < _recordCount-2 && (accumulatedSize < neededSize || copiedRecordCount < 1); ++copiedRecordCount) {
        accumulatedSize += _recordIndex(copiedRecordCount).length() + _recordIndexSize();
        leftPage->insert(copiedRecordCount, record(copiedRecordCount));
        if (_hasLinks)  leftPage->relink(copiedRecordCount, link(copiedRecordCount));
    }
    if (_hasLinks) leftPage->relink(copiedRecordCount, link(copiedRecordCount));

    medianPosition = copiedRecordCount++;

    for (int i = 0; copiedRecordCount < _recordCount; ++copiedRecordCount, ++i) {
        rightPage->insert(i, record(copiedRecordCount));
        if (_hasLinks)  rightPage->relink(i, link(copiedRecordCount));
    }
    if (_hasLinks) rightPage->relink((int)rightPage->recordCount(), lastLink());

    _wasChanged = false;
    return leftPage;
}


bool db_page::canReplace(int position, binary_data newData) const
{
    return newData.length() - _recordIndex(position).valueLength <= _freeBytes();
}


void db_page::replace(int position, const db_data_entry &data, int linked)
{
    assert(position >= 0 && position < _recordCount);

    remove(position);
    insert(position, data, linked);
}


void db_page::append(db_data_entry data, int linked)
{
    insert((int)_recordCount, data, linked);
}


size_t db_page::usedBytes() const
{
    return _pageSize - _freeBytes();
}


size_t db_page::usedBytesFor(int position) const
{
    assert(position >= 0 && (position < _recordCount  || position <= _recordCount && _hasLinks));

    if (position == _recordCount)  return _recordIndexSize();
    return _recordIndexSize() + _recordIndex(position).length();
}