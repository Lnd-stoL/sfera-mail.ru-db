
#include <iostream>

using std::cout;
using std::endl;

#include "mydb.hpp"


int main (int argc, char** argv)
{
    cout << "test" << endl;

    //mydb_database db("storage.db");
    mydb_database db("storage.db", 1000000, mydb_internal_config(4096));

    binary_data tk("testkey");
    binary_data tk2("testkey2");
    binary_data tk3("testkey3");
    binary_data tk4("testkey4");
    binary_data tv("test value");

    db.insert(tk2, tv);
    db.insert(tk4, tv);
    db.insert(tk3, tv);
    db.insert(tk, tv);

    binary_data test = db.get(tk2);

    free(tk.dataPtr());
    free(tv.dataPtr());

    return 0;
}
