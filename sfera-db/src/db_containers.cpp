
#include "db_containers.hpp"

#include <stdlib.h>
#include <string.h>

//----------------------------------------------------------------------------------------------------------------------

sfera_db::data_blob::data_blob(uint8_t *dataPtr, size_t length) : _length (length), _dataPtr(dataPtr)
{ }


std::string sfera_db::data_blob::toString() const
{
    std::string str(_length, ' ');
    std::copy(this->dataPtr(), this->dataEndPtr(), str.begin());

    return str;
}


sfera_db::data_blob sfera_db::data_blob::fromCopyOf(const std::string &str)
{
    char *strCopy = strdup(str.c_str());
    return data_blob((uint8_t *)strCopy, str.length());
}


sfera_db::data_blob_copy::data_blob_copy(sfera_db::data_blob src)
{
    _length = src.length();
    _dataPtr = (uint8_t *)malloc(src.length());
    std::copy(src.dataPtr(), src.dataEndPtr(), _dataPtr);
}


void sfera_db::data_blob_copy::release()
{
    free(_dataPtr);
}


sfera_db::key_value_copy::key_value_copy(sfera_db::key_value const &src) : key(src.key), value(src.value)
{
}


void sfera_db::key_value_copy::release()
{
    key.release();
    value.release();
}
