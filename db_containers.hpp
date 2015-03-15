
#ifndef _SFERA_MYDBMS_DB_CONTAINERS_H_
#define _SFERA_MYDBMS_DB_CONTAINERS_H_

//----------------------------------------------------------------------------------------------------------------------

#include <stddef.h>
#include <cstdint>
#include <string>

using std::string;

//----------------------------------------------------------------------------------------------------------------------

class binary_data
{
private:
    size_t  _length;
    void   *_dataPtr;

public:
    binary_data (size_t length);
    binary_data (void* dataPtr, size_t length);
    binary_data (string str);
    binary_data();

    ~binary_data();

    size_t length()  const;
    void * dataPtr() const;

    uint8_t *byteDataPtr() const;
    uint8_t *byteDataEnd() const;
};

//----------------------------------------------------------------------------------------------------------------------

class db_data_entry
{
private:
    binary_data  _key;
    binary_data  _value;

public:
    const binary_data&  key()   const;
    const binary_data&  value() const;

    size_t length() const;

    db_data_entry(const binary_data &key, const binary_data &value) : _key (key), _value (value)  { }
    db_data_entry()  { }
};

//----------------------------------------------------------------------------------------------------------------------

#endif //_SFERA_MYDBMS_DB_CONTAINERS_H_
