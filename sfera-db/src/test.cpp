
#include <iostream>
#include <vector>
#include <algorithm>

using std::cout;
using std::endl;

#include "mydb.hpp"


int main (int argc, char** argv)
{
    //mydb_database db("storage.db");
    mydb_database db("storage.db", 50000000, mydb_internal_config(2024));

    binary_data tk1("1testketestkeytestkeytestkeyytestkey");
    binary_data tk2("2testkeytestkeytestkeytestkeytestkjklkjklkjkljeytestkeytestkeytestkey2");

    db.insert(tk1, tk2);

    return 0;
}
