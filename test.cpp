
#include <iostream>

using std::cout;
using std::endl;

#include "mydb.hpp"


int main (int argc, char** argv)
{
    cout << "test" << endl;

    mydb_database db ("storage.db", 12 * 1000000, mydb_internal_config (4096));

    binary_data tk ("testkey");
    //binary_data tv ("testval");
    db.insert (tk, tk);

    free(tk.dataPtr());

    return 0;
}
