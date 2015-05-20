
#ifndef _SFERA_MYDBMS_DB_CONTAINERS_H_
#define _SFERA_MYDBMS_DB_CONTAINERS_H_

//----------------------------------------------------------------------------------------------------------------------

#include <cstdint>
#include <string>

using std::string;

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    class data_blob
    {
    protected:
        size_t   _length  = 0;
        uint8_t *_dataPtr = nullptr;

    public:
        data_blob() { };
        data_blob(uint8_t *dataPtr, size_t length);

        inline size_t   length()     const  { return _length;  };
        inline uint8_t *dataPtr()    const  { return _dataPtr; };
        inline uint8_t *dataEndPtr() const  { return _dataPtr + _length; }

        inline bool valid() const  { return _dataPtr != nullptr; }

        std::string toString() const;
        static data_blob fromCopyOf(const std::string &str);
    };


    class data_blob_copy : public data_blob
    {
    public:
        data_blob_copy() { }
        data_blob_copy(data_blob src);

        void release();
    };

//----------------------------------------------------------------------------------------------------------------------

    struct key_value
    {
        data_blob key;
        data_blob value;

        key_value() { }
        key_value(data_blob k, data_blob v) : key(k), value(v) { }

        inline size_t summLength() const  { return key.length() + value.length(); }
    };


    struct key_value_copy
    {
        data_blob_copy key;
        data_blob_copy value;

        key_value_copy() { }
        key_value_copy(const key_value& src);
        void release();

        inline operator key_value()  { return *(reinterpret_cast<key_value *>(this)); }
    };
}

//----------------------------------------------------------------------------------------------------------------------

#endif    //_SFERA_MYDBMS_DB_CONTAINERS_H_
