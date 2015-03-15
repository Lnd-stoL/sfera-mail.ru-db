#include <glob.h>
#include <stdint-gcc.h>
#include "db_containers.hpp"

//----------------------------------------------------------------------------------------------------------------------

binary_data::binary_data() : _length (0), _dataPtr (nullptr)
{ }


binary_data::binary_data(size_t length) : _length (length)
{
    _dataPtr = malloc(length);
}


binary_data::binary_data(void *dataPtr, size_t length) : _length (length)
{
    _dataPtr = dataPtr;
}


binary_data::binary_data(string str) : _length (str.size())
{
    _dataPtr = malloc(_length);
    std::copy(str.begin(), str.end(), (char *)_dataPtr);
}


binary_data::~binary_data()
{
}


size_t binary_data::length() const
{
    return _length;
}


void *binary_data::dataPtr() const
{
    return _dataPtr;
}


uint8_t *binary_data::byteDataPtr() const
{
    return (uint8_t *)_dataPtr;
}


uint8_t *binary_data::byteDataEnd() const
{
    return byteDataPtr() + _length;
}

//----------------------------------------------------------------------------------------------------------------------

const binary_data &db_data_entry::key() const
{
    return _key;
}


const binary_data &db_data_entry::value() const
{
    return _value;
}


size_t db_data_entry::length() const
{
    return _key.length() + _value.length();
}
