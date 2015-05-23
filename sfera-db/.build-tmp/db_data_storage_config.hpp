
#ifndef SFERA_DB_DB_FILE_STORAGE_CONFIG_HPP
#define SFERA_DB_DB_FILE_STORAGE_CONFIG_HPP

//----------------------------------------------------------------------------------------------------------------------

#include <cstddef>

//----------------------------------------------------------------------------------------------------------------------

struct db_data_storage_config
{
    size_t pageSize           = 4096;
    size_t maxStorageSize     = 0;
    size_t cacheSizeInPages   = 256;
};

//----------------------------------------------------------------------------------------------------------------------

#endif //SFERA_DB_DB_FILE_STORAGE_CONFIG_HPP
