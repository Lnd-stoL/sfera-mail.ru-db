
#ifndef _DB_PAGE_INCLUDED_
#define _DB_PAGE_INCLUDED_

//----------------------------------------------------------------------------------------------------------------------

#include <type_traits>
#include <iterator>

#include "db_containers.hpp"

//----------------------------------------------------------------------------------------------------------------------

class db_page
{
public:
    class key_iterator : public std::iterator<std::random_access_iterator_tag, binary_data>
    {
    private:
        const db_page &_page;
        int  _position;

    public:
        key_iterator(const db_page &page, int position);

        bool operator != (const key_iterator &rhs);

        key_iterator operator ++ (int);
        binary_data operator * ();

        int operator - (const key_iterator &rhs);
        key_iterator operator += (int offset);
    };


private:
    struct record_index
    {
        uint16_t  keyValueOffset;
        uint16_t  keyLength;
        uint16_t  valueLength;

        record_index(off_t kvo, off_t kl, off_t vl) : keyValueOffset ((uint16_t)kvo), keyLength((uint16_t)kl),
                                                      valueLength((uint16_t)vl)  { }

        record_index(uint16_t kvo, uint16_t kl, uint16_t vl) : keyValueOffset (kvo), keyLength (kl), valueLength (vl)  { }
        inline off_t valueOffset() const  { return keyValueOffset + keyLength; }
    };


private:
    int  _index;
    size_t    _pageSize = 0;
    uint8_t  *_pageBytes = nullptr;
    bool  _wasChanged = false;

    uint8_t  *_indexTable         = nullptr;
    size_t    _recordCount        = 0;
    off_t     _dataBlockEndOffset = 0;
    bool      _hasLinks           = false;


private:
    inline uint16_t  _pageBytesUint16(off_t byteOffset) const {
        return *(uint16_t *)(_pageBytes + byteOffset);
    }

    inline void _pageBytesUint16(off_t byteOffset, uint16_t val)  {
        *((uint16_t *)(_pageBytes + byteOffset)) = val;
    }

    inline size_t _recordIndexTableSize() const {
        return (_hasLinks ? _recordCount+1 : _recordCount) * _recordIndexSize();
    }

    inline size_t _auxInfoSize() const {
        return (_indexTable - _pageBytes) + _recordIndexTableSize();
    }

    inline size_t _freeBytes() const {
        return _dataBlockEndOffset - _auxInfoSize();
    }

    inline size_t _recordIndexSize() const {
        return (_hasLinks ? 5 : 3) * sizeof(uint16_t);
    }

    inline uint16_t*_recordIndexRawPtr(int position) const {
        return (uint16_t *)(_indexTable + position * _recordIndexSize());
    }


private:
    record_index _recordIndex(int position) const;
    void _insertRecordIndex(int position, const record_index& recordIndex);

public:
    db_page(int index, binary_data pageBytes);
    void load();
    void initializeEmpty(bool hasLinks = false);
    void prepareForWriting();

    bool isFull() const;
    size_t recordCount() const;
    bool hasLinks() const;

    int link(int position) const;
    db_data_entry record(int position) const;
    binary_data key(int position) const;
    binary_data value(int position) const;

    key_iterator begin() const;
    key_iterator end() const;

    void insert(int position, db_data_entry data, int linked = 0);

    inline  size_t    size()       const  { return _pageSize; }
    inline  int       index()      const  { return _index; }
    inline  bool      wasChanged() const  { return _wasChanged; }
    inline  uint8_t*  bytes()      const  { return _pageBytes; }

    inline void wasSaved()  { _wasChanged = false; }
};

//----------------------------------------------------------------------------------------------------------------------

#endif
