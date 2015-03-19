
#include <iostream>

using std::cout;
using std::endl;

#include "mydb.hpp"


int main (int argc, char** argv)
{
    //mydb_database db("storage.db");
    mydb_database db("storage.db", 1000000, mydb_internal_config(300));

    binary_data tk1("1testketestkeytestkeytestkeyytestkey");
    binary_data tk2("2testkeytestkeytestkeytestkeytestkjklkjklkjkljeytestkeytestkeytestkey2");
    binary_data tk3("3testketestkeytestkeyy3jksdhaaaaaaaaaaaajkjhkfjhkjhkjhdsjkhhhhhhhhhhjsdjkjdkslkjdkslfjjhkfkhjfjhk");
    binary_data tk4("4testktestkeytestkeytestkeyteyey4");
    binary_data tk5("5testkey4notfoutestkeytestkeytestkeytestkeytestkeynd5");
    binary_data tk0("0testkey4notfoutestkeytestkeytestkeytestkeytestkeynd5");
    binary_data tk6("6testkey4notfoutestkeyt");
    binary_data tk7("3zte");
    binary_data tk8("3zzzzzzzzzzzzzzz");
    binary_data tk9("0sdjhjkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkaaaaaaaa");
    binary_data tk10("01djhjkkkkkkkkkkkfffffffffffffffffkaaaaaa");
    binary_data tk11("999999999991djhjkkkkkkkkkkkfffffffffffffffffkaaaaaa");
    binary_data tk12("99991djhjkkkkkkkkkkkfffffffffffffffffkaaaaaa");
    binary_data tk13("a1djhjkkkkkkkkkkkfffffffffffffffffkaaaaaa");
    binary_data tk14("b1djhjkkkfdsgasdfsdfasdfkkkkkkkkfffffffffffffffffkaaaaaa");
    binary_data tv("test value");

    db.insert(tk1, tv);
    db.insert(tk2, tv);
    db.insert(tk3, tv);
    db.insert(tk4, tv);
    db.insert(tk5, tv);
    db.insert(tk0, tv);
    db.insert(tk6, tv);
    db.insert(tk7, tv);
    db.insert(tk8, tv);
    db.insert(tk9, tv);
    db.insert(tk10, tv);
    db.insert(tk11, tv);
    db.insert(tk12, tv);
    db.insert(tk13, tv);
    db.insert(tk14, tv);

    binary_data test = db.get(tv);
    cout << db.dump() << endl;

    db.remove(tk6);

    cout << endl << db.dump() << endl;

    free(tk1.dataPtr());
    free(tv.dataPtr());

    return 0;
}
