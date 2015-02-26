
#include <iostream>

using std::cout;
using std::endl;

#include "mydb.hpp"


int main (int argc, char** argv)
{
    cout << "test" << endl;

    mydb_database db("storage.db", 128 * 1000000, mydb_internal_config (4096 * 1024));

    return 0;
}
