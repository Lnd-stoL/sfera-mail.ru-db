
//----------------------------------------------------------------------------------------------------------------------
// page layout brief description
//----------------------------------------------------------------------------------------------------------------------
//
//  offset |  size  |  field meaning
//  --------------------------------------------------------------------------------------------------------------------
//  0       | uint64 | last operation id that modified the page
//  8       | uint16 | record count the page contains (record = key+valueAt entry and a childAt to the child btree node)
//  10      | uint16 | data_block_end (the data contains of keys and values BLOBs and is placed to the end of the page)
//  12      | byte   | meta information (actually a byte indicating if the page is btree leaf [0] or not [1])
//  13      | ------ | data entry index blocks:
//                      block consists of three integers ( uint16 ) + one 32-bit integer
//                          at 0 - key_value blob offset within the page
//                          at 2 - key length in bytes
//                          at 4 - valueAt length in bytes
//                          at 6 - [if not a leaf] ID of a page which is a btree child node coming BEFORE the key
//   ===== FREE SPACE =====
//   ===== ACTUAL VALUES AND KEYS BINARY DATA ===== - from data_block_end to the end of the page
//
//----------------------------------------------------------------------------------------------------------------------

#include "db_page.hpp"

#include <cassert>
#include <algorithm>
#include <stdlib.h>

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

//----------------------------------------------------------------------------------------------------------------------

db_page::key_iterator::key_iterator(const db_page *page, int position) : _page (page), _position (position)
{ }


bool db_page::key_iterator::operator!=(const db_page::key_iterator &rhs) const
{
    assert(_page == rhs._page);
    return _position != rhs._position;
}


bool db_page::key_iterator::operator==(const db_page::key_iterator &rhs) const
{
    return !(this->operator!=(rhs));
}


db_page::key_iterator&
db_page::key_iterator::operator=(const db_page::key_iterator &rhs)
{
    _page = rhs._page;
    _position = rhs._position;
    return *this;
}


db_page::key_iterator
db_page::key_iterator::operator++(int i)
{
    db_page::key_iterator tmp = *this;
    this->operator+=(1);
    return tmp;
}


db_page::key_iterator
db_page::key_iterator::operator++()
{
    this->operator+=(1);
    return *this;
}


data_blob
db_page::key_iterator::operator*()
{
    return _page->keyAt(_position);
}


int db_page::key_iterator::operator-(db_page::key_iterator const &rhs)
{
    assert(_page == rhs._page);
    return _position - rhs._position;
}


db_page::key_iterator&
db_page::key_iterator::operator += (int offset)
{
    int newPosition = _position + offset;
    if (newPosition >= _page->recordCount()) {
        *this = _page->keysEnd();
    } else {
        _position = newPosition;
    }

    return *this;
}


data_blob
db_page::key_iterator::value() const
{
    return _page->valueAt(_position);
}


int db_page::key_iterator::child() const
{
    return _page->childAt(_position);
}

//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------

db_page* db_page::load(int index, data_blob pageBytes)
{
    auto dbPage = new db_page(index, pageBytes);
    dbPage->_load();
    return dbPage;
}


db_page* db_page::createEmpty(int index, data_blob pageBytes, bool isLeaf)
{
    auto dbPage = new db_page(index, pageBytes);
    dbPage->_initializeEmpty(!isLeaf);
    return dbPage;
}


db_page::db_page(int index, data_blob pageBytes) :
    _index(index),
    _pageSize(pageBytes.length()),
    _pageBytes(pageBytes.dataPtr())
{  }


void db_page::_initializeEmpty(bool hasLinks)
{
    _indexTable = _pageBytes + 2 * sizeof(uint16_t) + sizeof(uint64_t) + 1;

    _hasLinks = hasLinks;
    _recordIndexSize = _calcRecordIndexSize();
    _wasChanged = true;

    //_pageBytesUint64(sizeof(uint64_t), 0);                      // last modified operation id
    //_pageBytesUint16(sizeof(uint64_t), 0);                      // record count

    _dataBlockEndOffset = _pageSize;
    //_pageBytesUint16(0, (uint16_t) _dataBlockEndOffset);    // data block end offset
    _pageBytes[2*sizeof(uint16_t) + sizeof(uint64_t)] = (uint8_t) hasLinks;
}


bool db_page::isFull() const
{
    return double(usedBytes()) / _pageSize * 100 >= maximallyFullPercent;
}


size_t db_page::recordCount() const
{
    return _recordCount;
}


bool db_page::hasChildren() const
{
    return _hasLinks;
}


int db_page::childAt(int position) const
{
    assert( _pageBytes != nullptr );
    assert( _hasLinks );
    assert( position >= 0 && position <= _recordCount );

    return *(int32_t *)(_recordIndexRawPtr(position) + 3);
}


data_blob db_page::keyAt(int position) const
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount );

    auto recordIndex = _recordIndex(position);
    uint8_t *ptr = _pageBytes + recordIndex.keyValueOffset;
    return data_blob(ptr, recordIndex.keyLength);
}


data_blob db_page::valueAt(int position) const
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount );

    auto recordIndex = _recordIndex(position);
    uint8_t *ptr = _pageBytes + recordIndex.valueOffset();
    return data_blob(ptr, recordIndex.valueLength);
}


void db_page::insert(int position, key_value data, int linked)
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount+1 );
    assert( possibleToInsert(data) );
    assert( hasChildren() || linked == -1 );

    _dataBlockEndOffset -= data.summLength();
    std::copy(data.key.dataPtr(), data.key.dataEndPtr(), _pageBytes + _dataBlockEndOffset);
    std::copy(data.value.dataPtr(), data.value.dataEndPtr(), _pageBytes + _dataBlockEndOffset + data.key.length());

    _insertRecordIndex(position, record_index(_dataBlockEndOffset, data.key.length(), data.value.length()), linked);
    _wasChanged = true;
}


db_page::key_iterator
db_page::keysBegin() const
{
    return db_page::key_iterator(this, 0);
}


db_page::key_iterator
db_page::keysEnd() const
{
    return db_page::key_iterator(this, (int)_recordCount);
}


db_page::record_index
db_page::_recordIndex(int position) const
{
    auto rawPtr = _recordIndexRawPtr(position);
    return record_index(rawPtr[0], rawPtr[1], rawPtr[2]);
}


void db_page::_insertRecordIndex(int position, db_page::record_index const &recordIndex, int linked)
{
    assert( _auxInfoSize() + _recordIndexSize <= _dataBlockEndOffset );

    std::copy_backward((uint8_t *)_recordIndexRawPtr(position),
                       _pageBytes + _auxInfoSize(),
                       _pageBytes + _auxInfoSize() + _calcRecordIndexSize());

    auto rawPtr = _recordIndexRawPtr(position);
    rawPtr[0] = recordIndex.keyValueOffset;
    rawPtr[1] = recordIndex.keyLength;
    rawPtr[2] = recordIndex.valueLength;
    if (_hasLinks) reconnect(position, linked);

    _recordCount++;
}


void db_page::prepareForWriting()
{
    assert( _pageBytes != nullptr );

    _pageBytesUint64(0, _lastModifiedOpId);
    _pageBytesUint16(sizeof(uint64_t), (uint16_t)_recordCount);
    _pageBytesUint16(sizeof(uint16_t) + sizeof(uint64_t), (uint16_t)_dataBlockEndOffset);
}


void db_page::_load()
{
    _indexTable = _pageBytes + 2 * sizeof(uint16_t) + sizeof(uint64_t) + 1;
    _hasLinks = _pageBytes[2*sizeof(uint16_t) + sizeof(uint64_t)] != 0;
    _recordIndexSize = _calcRecordIndexSize();

    _lastModifiedOpId = _pageBytesUint64(0);
    _recordCount = _pageBytesUint16(sizeof(uint64_t));
    _dataBlockEndOffset = _pageBytesUint16(sizeof(uint16_t) + sizeof(uint64_t));
}


db_page::~db_page()
{
    _destructThis();
}


void db_page::insert(db_page::key_iterator position, key_value data, int childId)
{
// this is useful to test page llocation related bugs
/*
    for (int i = 0; i < _recordCount; ++i)
        if (_hasLinks && childAt(i) == childId) assert(0);
*/

    assert( position.associatedPage() == this );
    insert(position.position(), data, childId);
}


void db_page::reconnect(int position, int childId)
{
// this is useful to test page llocation related bugs
/*
    for (int i = 0; i < _recordCount; ++i)
        if (_hasLinks && childAt(i) == childId) assert(0);
*/

    assert( _pageBytes != nullptr );
    assert( _hasLinks );
    assert( position >= 0 && position <= _recordCount );

    *(int32_t *)(_recordIndexRawPtr(position) + 3) = childId;
    _wasChanged = true;
}


bool db_page::possibleToInsert(key_value element)
{
    return _freeBytes() >= element.summLength() + _calcRecordIndexSize();
}


void db_page::remove(int position)
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount );
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
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount );

    size_t realPageSize = _pageSize;
    _pageSize += _recordIndex(position).length();
    bool result = isMinimallyFilled();
    _pageSize = realPageSize;

    return result;
}


void db_page::replace(int position, data_blob newValue)
{
    assert( _pageBytes != nullptr );
    assert( position >= 0 && position < _recordCount );

    auto index = _recordIndex(position);
    data_blob_copy key(data_blob((uint8_t *)::malloc(index.keyLength), index.keyLength));
    std::copy(_pageBytes + index.keyValueOffset, _pageBytes + index.keyValueOffset + index.keyLength, key.dataPtr());
    int linked = -1;
    if (_hasLinks) linked = childAt(position);

    remove(position);
    insert(position, key_value(key, newValue), linked);
    key.release();
}


key_value_copy db_page::splitEquispace(db_page *rightPage)
{
    assert( _pageBytes != nullptr );
    assert( _recordCount >= 3 );    // at least 3 records for correct split

    // the way is to create proxy in-memory page and copy half of records into that page
    // then replace *this* page with proxy page's content (dirty enoughf)
    // this is done because insertion in page is much faster than removing now
    db_page *leftProxyPage = new db_page(_index, data_blob((uint8_t *)::calloc(_pageSize, 1), _pageSize));
    leftProxyPage->_initializeEmpty(_hasLinks);

    size_t allocatedSpace = (_pageSize - _dataBlockEndOffset);
    size_t neededSize = (allocatedSpace - (allocatedSpace / _recordCount)) / 2;

    size_t accumulatedSize = 0;
    int copiedRecordCount = 0;

    for (; copiedRecordCount < _recordCount-2 && (accumulatedSize < neededSize || copiedRecordCount < 1); ++copiedRecordCount) {
        accumulatedSize += _recordIndex(copiedRecordCount).length();
        leftProxyPage->insert(copiedRecordCount, recordAt(copiedRecordCount));
        if (_hasLinks) leftProxyPage->reconnect(copiedRecordCount, childAt(copiedRecordCount));
    }
    if (_hasLinks) leftProxyPage->reconnect(copiedRecordCount, childAt(copiedRecordCount));

    int medianPosition = copiedRecordCount++;

    for (int i = 0; copiedRecordCount < _recordCount; ++copiedRecordCount, ++i) {
        rightPage->insert(i, recordAt(copiedRecordCount));
        if (_hasLinks) rightPage->reconnect(i, childAt(copiedRecordCount));
    }
    if (_hasLinks) rightPage->reconnect((int) rightPage->recordCount(), lastRightChild());

    key_value_copy medianElement(this->recordAt(medianPosition));
    this->moveContentFrom(leftProxyPage);
    delete leftProxyPage;

    return medianElement;
}


bool db_page::canReplace(int position, data_blob newData) const
{
    return newData.length() - _recordIndex(position).valueLength <= _freeBytes();
}


void db_page::replace(int position, const key_value &data, int linked)
{
    assert( position >= 0 && position < _recordCount );

    remove(position);
    insert(position, data, linked);
}


void db_page::append(key_value data, int linked)
{
    insert((int)_recordCount, data, linked);
}


size_t db_page::usedBytes() const
{
    return _pageSize - _freeBytes();
}


size_t db_page::usedBytesFor(int position) const
{
    assert( position >= 0 && (position < _recordCount  || position <= _recordCount && _hasLinks) );

    if (position == _recordCount)  return _recordIndexSize;
    return _recordIndexSize + _recordIndex(position).length();
}


key_value db_page::recordAt(int position) const
{
    return key_value(this->keyAt(position), this->valueAt(position));
}


void db_page::_destructThis()
{
    if (_pageBytes) {
        free(_pageBytes);
        _pageBytes = nullptr;
    }
}


void db_page::moveContentFrom(db_page *srcPage)
{
    srcPage->prepareForWriting();
    _pageBytes = srcPage->_pageBytes;
    srcPage->_pageBytes = nullptr;     // to avoid free in srcPage's destructor

    this->_load();                     // update cached members from _pageBytes
    _wasChanged = srcPage->_wasChanged;
}


void db_page::wasSaved(uint64_t opId)
{
    _lastModifiedOpId = opId;
    _wasChanged = false;
}


uint8_t *db_page::bytes() const
{
    assert( _pageBytes != nullptr );
    return _pageBytes;
}

}