//1) Create an evergreen host with Amazon Linux 2 image, install mongocxx driver and build this program on that host with something like:
//g++ -std=c++17 -pthread -I /usr/local/include/mongocxx/v_noabi/ -I /usr/local/include/bsoncxx/v_noabi/ ./db_read.cpp -L/usr/local/lib64 -lmongocxx-static -lmongoc-static-1.0 -lbsoncxx-static -lbson-static-1.0 -lcrypto -lssl -lsasl2 -lresolv -lrt -ldl
//
//2) Copy binary to a dev pod and run from there to read data using a simple find() query 
//
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/client.hpp>
#include <iostream>

int main(int argc, char** argv)
{
    mongocxx::instance instance;
    mongocxx::uri uri(argv[1]);
    mongocxx::client client(uri);

    auto db = client["cDB"];
    auto collection = db["cColl"];

    auto cursor = collection.find().sort({._id =  1});

    int num = 0;
    for(auto&& doc : cursor) {
        if(num % 10000 == 0) {
            std::cout << "Received: " << num << " docs." << std::endl;
        }
        ++num;
    }
    std::cout << "Finished, Received: " << num << " docs." << std::endl;    
}
