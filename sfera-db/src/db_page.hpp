
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
            key_iterator operator+=(int offset);

            data_blob value() const;
            int link() const;

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
        static const int minimallyFullPercent = 45;
        static const int maximallyFullPercent = 75;


    private:
        int  _index;
        mutable size_t  _pageSize = 0;
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
        void _insertRecordIndex(int position, const record_index& recordIndex, int linked);


    private:
        db_page(int index, data_blob pageBytes);

        void _load();
        void _initializeEmpty(bool hasLinks = false);

    public:
        ~db_page();

        static db_page* load(int index, data_blob pageBytes);
        static db_page* createEmpty(int index, data_blob pageBytes, bool isLeaf);

        void prepareForWriting();

        size_t usedBytes() const;
        size_t usedBytesFor(int position) const;
        bool isFull() const;
        bool isMinimallyFilled() const;
        bool willRemainMinimallyFilledWithout(int position) const;
        size_t recordCount() const;
        bool hasLinks() const;
        bool possibleToInsert(key_value element);

        key_value record(int position) const;
        int link(int position) const;
        data_blob key(int position) const;
        data_blob value(int position) const;

        key_iterator begin() const;
        key_iterator end() const;

        void insert(int position, key_value data, int linked = -1);
        void append(key_value data, int linked = -1);
        void insert(key_iterator position, key_value data, int linked = -1);
        void relink(int position, int linked);
        void remove(int position);
        void replace(int position, data_blob newValue);
        void replace(int position, const key_value & element, int linked = -1);
        bool canReplace(int position, data_blob newValue) const;

        db_page * splitEquispace(db_page * rightPage, int& medianPosition);

        inline  size_t    size()       const  { return _pageSize; }
        inline  int       index()      const  { return _index; }
        inline  bool      wasChanged() const  { return _wasChanged; }
        inline  uint8_t*  bytes()      const  { return _pageBytes; }
        inline  int       lastLink()   const  { return link((int)_recordCount); }

        inline void wasSaved()  { _wasChanged = false; }
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif
