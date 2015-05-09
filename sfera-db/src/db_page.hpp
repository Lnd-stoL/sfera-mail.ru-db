
#ifndef _DB_PAGE_INCLUDED_
#define _DB_PAGE_INCLUDED_

//----------------------------------------------------------------------------------------------------------------------

#include <type_traits>
#include <iterator>

#include "db_containers.hpp"

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    class db_page
    {
    public:
        class key_iterator : public std::iterator<std::random_access_iterator_tag, data_blob>
        {
        private:
            const db_page *_page;
            int            _position;

        public:
            key_iterator(const db_page *page, int position);

            bool operator!=(const key_iterator &rhs) const;
            bool operator==(const key_iterator &rhs) const;
            key_iterator& operator=(const key_iterator &rhs);

            key_iterator operator++(int);
            key_iterator operator++();
            data_blob operator*();

            int operator-(const key_iterator &rhs);
            key_iterator& operator+=(int offset);

            data_blob value() const;
            int child() const;

            inline int            position()       const  { return _position; }
            inline const db_page *associatedPage() const  { return _page; }
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
            inline off_t dataEnd() const  { return valueOffset() + valueLength; }
            inline size_t length() const  { return keyLength + valueLength; }
        };


    private:
        static const int minimallyFullPercent = 47;
        static const int maximallyFullPercent = 70;


    private:
        int  _index;
        mutable size_t  _pageSize = 0;
        uint8_t  *_pageBytes = nullptr;
        bool  _wasChanged = false;

        uint8_t  *_indexTable         = nullptr;
        size_t    _recordIndexSize    = 0;
        size_t    _recordCount        = 0;
        uint64_t  _lastModifiedOpId   = 0;
        off_t     _dataBlockEndOffset = 0;
        bool      _hasLinks           = false;


    private:
        inline uint16_t  _pageBytesUint16(off_t byteOffset) const {
            return *(uint16_t *)(_pageBytes + byteOffset);
        }

        inline uint64_t  _pageBytesUint64(off_t byteOffset) const {
            return *(uint64_t *)(_pageBytes + byteOffset);
        }

        inline void _pageBytesUint16(off_t byteOffset, uint16_t val)  {
            *((uint16_t *)(_pageBytes + byteOffset)) = val;
        }

        inline void _pageBytesUint64(off_t byteOffset, uint64_t val)  {
            *((uint64_t *)(_pageBytes + byteOffset)) = val;
        }

        inline size_t _recordIndexTableSize() const {
            return (_hasLinks ? _recordCount+1 : _recordCount) * _recordIndexSize;
        }

        inline size_t _auxInfoSize() const {
            return (_indexTable - _pageBytes) + _recordIndexTableSize();
        }

        inline size_t _freeBytes() const {
            return _dataBlockEndOffset - _auxInfoSize();
        }

        inline size_t _calcRecordIndexSize() const {
            return (_hasLinks ? 5 : 3) * sizeof(uint16_t);
        }

        inline uint16_t*_recordIndexRawPtr(int position) const {
            return (uint16_t *)(_indexTable + position * _recordIndexSize);
        }


    private:
        record_index _recordIndex(int position) const;
        void _insertRecordIndex(int position, const record_index& recordIndex, int linked);
        void _destructThis();

    private:
        db_page(int index, data_blob pageBytes);

        void _load();
        void _initializeEmpty(bool hasLinks = false);

    public:
        ~db_page();
        static db_page* load(int index, data_blob pageBytes);
        static db_page* createEmpty(int index, data_blob pageBytes, bool isLeaf);

        void moveContentFrom(db_page* srcPage);
        void prepareForWriting();

        size_t usedBytes() const;
        size_t usedBytesFor(int position) const;
        bool isFull() const;
        bool isMinimallyFilled() const;
        bool willRemainMinimallyFilledWithout(int position) const;
        size_t recordCount() const;
        bool hasChildren() const;
        bool possibleToInsert(key_value element);

        key_value recordAt(int position) const;
        int childAt(int position) const;
        data_blob keyAt(int position) const;
        data_blob valueAt(int position) const;

        key_iterator keysBegin() const;
        key_iterator keysEnd() const;

        void insert(int position, key_value data, int linked = -1);
        void append(key_value data, int linked = -1);
        void insert(key_iterator position, key_value data, int linked = -1);
        void reconnect(int position, int childId);
        void remove(int position);
        void replace(int position, data_blob newValue);
        void replace(int position, const key_value & element, int linked = -1);
        bool canReplace(int position, data_blob newValue) const;

        key_value_copy splitEquispace(db_page *rightPage);

        inline  size_t    size()       const  { return _pageSize; }
        inline  int       id()         const  { return _index; }
        inline  bool      wasChanged() const  { return _wasChanged; }
        uint8_t*  bytes() const;
        inline  int       lastRightChild()   const  { return this->childAt((int) _recordCount); }
        inline  uint64_t  lastModifiedOpId() const  { return _lastModifiedOpId; }

        void wasSaved(uint64_t opId);
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif
