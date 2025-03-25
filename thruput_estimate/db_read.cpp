//1) Create an evergreen host with Amazon Linux 2 image, install mongocxx driver and build this program on that host:
//g++ -std=c++17 -pthread -O3 -g -I /usr/local/include/mongocxx/v_noabi/ -I /usr/local/include/bsoncxx/v_noabi/ ./db_read.cpp -L/usr/local/lib64 -lmongocxx-static -lmongoc-static-1.0 -lbsoncxx-static -lbson-static-1.0 -lfmt -lcrypto -lssl -lsasl2 -lresolv -lrt -ldl -o db_read
//
//2) Copy binary to a dev pod and run from there: prog uri numThreads
//3) Expects that a cluster has been loaded with data in cDB:cColl collection. Run insert_docs.js to do so.
//
#include <bsoncxx/json.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/client.hpp>
#include <fmt/format.h>
#include <iostream>
#include <list>
#include <thread>
#include <mutex>
#include <chrono>
#include <ctime>

void logProgress(int threadId, int numDocs) {
    static std::mutex lock;
    std::lock_guard g{lock};
    std::cout << "[ThreadId: " << threadId << "] ; numDocs=" << numDocs << std::endl;
}

struct Partition {
    int threadId_;
    const mongocxx::uri& uri_;
    int64_t min_;
    int64_t max_;
    std::thread queryThr_;

    Partition(int threadId, const mongocxx::uri& uri, int64_t minId, int64_t maxId) : threadId_{threadId}, uri_{uri}, min_{minId}, max_{maxId} {}

    void query() {
        mongocxx::client client(uri_);
        auto db = client["cDB"];
        auto collection = db["cColl"];

        std::string q;

        if(threadId_ == 0) {
            q = fmt::format("{{\"_id\": {{\"$gte\" : {}, \"$lte\": {}}}}}", min_, max_);
        } else {
            q = fmt::format("{{\"_id\": {{\"$gt\" : {}, \"$lte\": {}}}}}", min_, max_);            
        }        

        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::cout << "Starting partition " << threadId_ << " at: " << std::ctime(&now) << "; query=" << q << std::endl;
        auto cursor = collection.find(bsoncxx::from_json(q));
        int numDocs = 0;
        for(auto&& doc: cursor) {
            (void)doc;
            if(numDocs % 1000000 == 0) {
                logProgress(threadId_, numDocs);
            }
            ++numDocs;
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
    if(argc < 2) {
        std::cout << "prog uri [numThreads]" << std::endl;
        return 1;
    }
    
    mongocxx::instance instance;
    mongocxx::uri uri(argv[1]);
    mongocxx::client client(uri);

    int numThreads = 1;
    if(argc > 2) {
        numThreads = atoi(argv[2]);
    }

    auto db = client["cDB"];
    auto collection = db["cColl"];

    std::string bAutoDoc = fmt::format("{{\"groupBy\": \"$_id\", \"buckets\": {}}}", numThreads);
    std::cout << bAutoDoc << std::endl;

    auto qObj = bsoncxx::from_json(bAutoDoc);

    auto pipeline = std::move(mongocxx::pipeline().bucket_auto({qObj}));
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << "Partitioning at: " << std::ctime(&now) << std::endl;

    mongocxx::options::aggregate opts;
    opts.max_time(std::chrono::milliseconds{3600000});
    opts.allow_disk_use(true);
    opts.hint(mongocxx::hint{"_id_"});
    auto cursor = collection.aggregate(pipeline, opts);

    int numBuckets = 0;
    std::list<Partition> partitions;
    for(auto&& doc : cursor) {
        partitions.push_back(Partition{numBuckets, uri, doc["_id"]["min"].get_int64(), doc["_id"]["max"].get_int64()});
        ++numBuckets;
    }

    for (Partition& partition : partitions) {
        partition.start();
    }

    for (Partition& partition : partitions) {
        partition.stop();
    }

    now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << "[" << std::ctime(&now) << "] Stopped all partitions." << std::endl;
}
