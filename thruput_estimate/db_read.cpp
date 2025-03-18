//1) Create an evergreen host with Amazon Linux 2 image, install mongocxx driver and build this program on that host with something like:
//g++ -std=c++17 -pthread -I /usr/local/include/mongocxx/v_noabi/ -I /usr/local/include/bsoncxx/v_noabi/ ./db_read.cpp -L/usr/local/lib64 -lmongocxx-static -lmongoc-static-1.0 -lbsoncxx-static -lbson-static-1.0 -lcrypto -lssl -lsasl2 -lresolv -lrt -ldl
//
//2) Copy binary to a dev pod and run from there to read data using a simple find() query 
//
#include <bsoncxx/json.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/client.hpp>
#include <fmt/format.h>
#include <iostream>
#include <list>
#include <thread>

void logProgress(int threadId, int numDocs) {
    static std::mutex lock;
    std::lock_guard g{lock};
    std::cout << "[ThreadId: " << threadId << "] ; numDocs=" << numDocs << std::endl;
}

struct Partition {
    int threadId_;
    mongocxx::uri uri_;
    std::string min_;
    std::string max_;
    std::thread queryThr_;

    Partition(int threadId, mongocxx::uri uri, std::string minId, std::string maxId) : threadId_{threadId}, uri_{std::move(uri)}, min_{minId}, max_{maxId} {}

    void query() {
        mongocxx::client client(uri_);
        auto db = client["cDB"];
        auto collection = db["cColl"];

        std::string q;
        if(threadId_ == 0) {
            q = fmt::format("{{_id: {{$gte : ObjectId(\"{}\"), $lte: ObjectId(\"{}\")}}}}", min_, max_);
        } else {
            q = fmt::format("{{_id: {{$gt : ObjectId(\"{}\"), $lte: ObjectId(\"{}\")}}}}", min_, max_);            
        }
        auto cursor = collection.find(bsoncxx::from_json(q));
        int numDocs = 0;
        for(auto&& doc: cursor) {
            (void)doc;
            if(numDocs % 10000 == 0) {
                logProgress(threadId_, numDocs);
            }
        }
    }

    void start() {
        queryThr_ = std::thread([this](){ query();});
    }

    void stop() {
        if(queryThr_.joinable()) {
            queryThr_.join();
        }
    }
};

int main(int argc, char** argv)
{
    mongocxx::instance instance;
    mongocxx::uri uri(argv[1]);
    mongocxx::client client(uri);

    int numThreads = 1;
    if(argc > 2) {
        numThreads = atoi(argv[2]);
    }

    auto db = client["cDB"];
    auto collection = db["cColl"];

    std::string query = fmt::format("{{$bucketAuto: {{groupBy: \"$_id\", buckets: {}}}}}", numThreads);
    std::cout << query << std::endl;

    auto qObj = bsoncxx::from_json(query);
    auto pipeline = std::move(mongocxx::pipeline().bucket_auto(bsoncxx::document::view((uint8_t*)qObj.data(), qObj.length())));
    auto cursor = collection.aggregate(pipeline);

    int numBuckets = 0;
    std::list<Partition> partitions;
    for(auto&& doc : cursor) {
        std::cout << bsoncxx::to_json(doc) << std::endl;
        partitions.push_back(Partition{numBuckets, std::move(uri), doc["_id"]["min"].get_oid().value.to_string(), doc["_id"]["max"].get_oid().value.to_string()});
        ++numBuckets;
    }

    for (Partition& partition : partitions) {
        std::cout << "Starting partition: " << partition.threadId_ << std::endl;
        partition.start();
    }

    for (Partition& partition : partitions) {
        partition.stop();
    }

    std::cout << "Stopped all partitions" << std::endl;
}
