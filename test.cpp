
#include <iostream>
#include <vector>
#include <algorithm>

using std::cout;
using std::endl;

#include "mydb.hpp"


int main (int argc, char** argv)
{
    //mydb_database db("storage.db");
    mydb_database db("storage.db", 50000000, mydb_internal_config(512));

    std::vector<binary_data> tk;
    int K = 27000;
    for (int i = 1; i <= K; ++i) {
        tk.push_back(binary_data(std::to_string(i) + "aaaaaaaaaa"));
    }
    std::vector<binary_data> ik = tk;
    std::random_shuffle(ik.begin(), ik.end());

    binary_data tk1("1testketestkeytestkeytestkeyytestkey");
    binary_data tk2("2testkeytestkeytestkeytestkeytestkjklkjklkjkljeytestkeytestkeytestkey2");
    binary_data tk3("3testketestkeytestkeyy3jksdhaaaaaaaaaaaajkjhkfjhkjhkjhdsjkhhhhhhhhhhjsdjkjdkslkjdkslfjjhkfkhjfjhk");
    binary_data tk4("4testktestkeytestkeytestkeyteyey4");
    binary_data tk5("5testkey4notfoutestkeytestkeytestkeytestkeytestkeynd5");
    binary_data tk0("0testkey4notfoutestkeytestkeytestkeytesfgdtkeytestkeynd5");
    binary_data tk6("6testkey4notfoutestkeyt");
    binary_data tk7("3ztdfsdfasdgsdfgggggggggggggggggggggggggge");
    binary_data tk8("3zzzzzzzzzgdfgggggggggggggggggggggggggggggggggggggggggzzzzzz");
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
    //db.insert(tk13, tv);
    //db.insert(tk14, tv);
/*
    for (int i = 0; i < K; ++i) {
        db.insert(ik[i], tv);
    }

    int found = 0;
    for (int i = 0; i < K; ++i) {
        auto r = db.get(ik[i]);
        if (r.valid()) ++found;
        if (!r.valid()) {
            std::cout << "error getting " << ik[i].toString() << std::endl;
        }
    }
    std::cout << "found keys: " << found << std::endl;
    //db.insert(binary_data("--aaaaaaaa"), tv);

*/

    std::cout << db.dump() << endl;
    db.remove(tk5);
    db.remove(tk0);
    db.remove(tk1);
    db.remove(tk2);
    db.remove(tk3);
    std::cout << db.dump() << endl;
  //  std::cout << "found keys: " << found << std::endl;
   /* std::cout << ik[K].toString() << endl;
    db.insert(ik[K], tv);

    found = 0;
    for (int i = 0; i < K+1; ++i) {
        auto r = db.get(ik[i]);
        if (r.valid()) ++found;
        if (!r.valid()) {
            std::cout << "error getting " << ik[i].toString() << std::endl;
        } else {
            //std::cout << "getting " << ik[i].toString() << endl;
        }
    }
    std::cout << "found keys: " << found << std::endl;

    std::cout << db.dump() << endl;*/
    return 0;


    /*binary_data test = db.get(tk1);
    test = db.get(tk2);
    test = db.get(tk3);
    test = db.get(tk4);
    test = db.get(tk5);
    test = db.get(tk6);
    test = db.get(tk7);
    test = db.get(tk8);
    test = db.get(tk9);
    test = db.get(tk10);
    test = db.get(tk11);
    test = db.get(tk12);
    test = db.get(tk13);
    test = db.get(tk14);*/
    cout << db.dump() << endl;
    cout << "=====" << endl;
    db.insert(binary_data("50aaaaaaaa"), tv);
    cout << db.dump() << endl;

    //db.remove(tk6);

    //cout << endl << db.dump() << endl;

    free(tk1.dataPtr());
    free(tv.dataPtr());

    return 0;
}
