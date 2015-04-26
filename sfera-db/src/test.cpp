
#include <iostream>
#include <vector>
#include <algorithm>

using std::cout;
using std::endl;

#include "database.hpp"

using namespace sfera_db;

int main (int argc, char** argv)
{
    database_config dbConfig;
    dbConfig.pageSizeBytes = 1024;
    dbConfig.maxDBSize = 32000000;

    database *db = database::createEmpty("test_db", dbConfig);

    data_blob tk1 = data_blob::fromCopyOf("1testketestkeytestkeytestkeyytestkey");
    data_blob tk2 = data_blob::fromCopyOf("2testkeytestkeytestkeytestkeytestkjklkjklkjkljeytestkeytestkeytestkey2");
    data_blob tv = data_blob::fromCopyOf("testValue");

    db->insert(tk1, tv);
    db->insert(tk2, tv);

    std::cout << db->dump() << std::endl;
    delete db;
    return 0;
}
