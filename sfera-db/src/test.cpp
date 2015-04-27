
#include <iostream>
#include <vector>

#include "database.hpp"

using namespace sfera_db;


void fillTestSet(std::vector<std::pair<data_blob, data_blob>> &testSet, size_t count)
{
    testSet.resize(count);

    for (size_t i = 0; i < count; ++i) {

        auto skey = std::string("test key ololo ") + std::string(i%20, 't') + std::to_string(i);
        auto svalue = std::string("value test") + std::to_string(i);

        testSet[i] = std::make_pair(data_blob::fromCopyOf(skey), data_blob::fromCopyOf(svalue));
    }
}


int main (int argc, char** argv)
{
    database_config dbConfig;
    dbConfig.pageSizeBytes = 1024;
    dbConfig.maxDBSize = 32000000;

    database *db = database::createEmpty("test_db", dbConfig);

    std::vector<std::pair<data_blob, data_blob>> testSet;
    fillTestSet(testSet, 10);

    // insertion
    for (size_t i = 0; i < testSet.size(); ++i) {
        std::cout << "INSERT " << testSet[i].first.toString() << " : " << testSet[i].second.toString() << std::endl;
        db->insert(testSet[i].first, testSet[i].second);
    }

    // getting
    bool getOK = true;
    for (size_t i = 0; i < testSet.size(); ++i) {
        std::cout << "GET " << testSet[i].first.toString() << " : " << testSet[i].second.toString() << " = ";
        data_blob result = db->get(testSet[i].first);
        std::cout << result.toString() << std::endl;

        if (result.toString() != testSet[i].second.toString()) {
            getOK = false;
            break;
        }
    }
    std::cout << "GET TEST: " << getOK << std::endl;

    // removing
    for (size_t i = 0; i < testSet.size(); ++i) {
        std::cout << "REMOVE " << testSet[i].first.toString() << " ";
        db->remove(testSet[i].first);
        data_blob result = db->get(testSet[i].first);
        std::cout << (result.valid() ? "FAILED" : "OK") << std::endl;
    }

    std::cout << db->dump() << std::endl;
    delete db;
    return 0;
}
