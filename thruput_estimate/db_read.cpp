//1) Create an evergreen host with Amazon Linux 2 image, install mongocxx driver and build this program on that host:
//g++ -std=c++17 -pthread -O3 -g -I /usr/local/include/mongocxx/v_noabi/ -I /usr/local/include/bsoncxx/v_noabi/ ./db_read.cpp -L/usr/local/lib64 -lmongocxx-static -lmongoc-static-1.0 -lbsoncxx-static -lbson-static-1.0 -lfmt -lcrypto -lssl -lsasl2 -lresolv -lrt -ldl -o db_read
//
//2) Copy binary to a dev pod and run from there: prog uri numThreads
//3) Expects that a cluster has been loaded with data in cDB:cColl collection. Run insert_docs.js to do so.
//
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/client.hpp>
#include <fmt/format.h>
#include <iostream>
#include <list>
#include <stdexcept>
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
    int threadId_ = 0;
    const mongocxx::uri& uri_;
    int64_t min_ = -1;
    int64_t max_ = -1;
    bool naturalScan_ = false;
    bool recordIdScan_ = false;
    std::thread queryThr_;

    Partition(int threadId, const mongocxx::uri& uri, int64_t minId, int64_t maxId) : threadId_{threadId}, uri_{uri}, min_{minId}, max_{maxId} {}
    Partition(const mongocxx::uri& uri, bool naturalScan, bool recordIdScan) : uri_{uri}, naturalScan_{naturalScan}, recordIdScan_{recordIdScan} {
        if(naturalScan_ && recordIdScan_) {
            throw std::invalid_argument("Exactly one of naturalScan/recordIdScan expected");
        }
    }

    void query() {
        mongocxx::client client(uri_);
        auto db = client["cDB"];
        auto collection = db["cColl"];

        std::string q;

        if (naturalScan_) {
            q = R"({})";
        } else {
            if (threadId_ == 0) {
                q = fmt::format("{{\"_id\": {{\"$gte\" : {}, \"$lte\": {}}}}}", min_, max_);
            } else {
                q = fmt::format("{{\"_id\": {{\"$gt\" : {}, \"$lte\": {}}}}}", min_, max_);
            }
        }
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::cout << "Starting partition " << threadId_ << " at: " << std::ctime(&now) << "; naturalScan=" << naturalScan_ << " ; recordIdScan=" << recordIdScan_ << std::endl;

        mongocxx::cursor *cursor;
        if(recordIdScan_) {
            //R"([{$project: {_id: 1, rid: {$meta: 'recordId'}}}, {$sort: {rid: 1}}])";
            using bsoncxx::builder::basic::make_document;
            using bsoncxx::builder::basic::kvp;
            mongocxx::pipeline p;
            p.project(make_document(kvp("_id", 1), kvp("rid", make_document(kvp("$meta", "recordId")))));
            p.sort(make_document(kvp("rid", 1)));

            mongocxx::options::aggregate opts;
            opts.allow_disk_use(true);
            auto c = collection.aggregate(p, opts);
            cursor = new mongocxx::cursor{std::move(c)};
        } else {
            mongocxx::options::find opts;
            if (naturalScan_) {
                std::string hintDoc = R"({"$natural": 1})";
                opts.hint(mongocxx::hint{bsoncxx::from_json(hintDoc)});
            }
            auto c = collection.find(bsoncxx::from_json(q), opts);
            cursor = new mongocxx::cursor{std::move(c)};
        }

        int numDocs = 0;
        for(auto&& doc: *cursor) {
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
        std::cout << "prog uri [--natural]|[--recordId]|[--numThreads <>]" << std::endl;
        return 1;
    }
    
    mongocxx::instance instance;
    mongocxx::uri uri(argv[1]);
    mongocxx::client client(uri);

    bool isNaturalScan = false;
    bool isRecordIdScan = false;
    int numThreads = 1;
    if(argc > 2) {
        if(std::string{argv[2]} == "--natural") {
            isNaturalScan = true;
        } else if(std::string{argv[2]} == "--recordId") {
            isRecordIdScan = true;
        } else if(std::string{argv[2]} == "--numThreads") {
            if(argc > 3) {
                numThreads = atoi(argv[3]);
            } else {
                std::cout << "prog uri [--natural]|[--recordId]|[--numThreads <>]" << std::endl;
                return 1;
            }
        } else {
            std::cout << "prog uri [--natural]|[--recordId]|[--numThreads <>]" << std::endl;
            return 1;
        }
    }

    auto db = client["cDB"];
    auto collection = db["cColl"];

    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << "Partitioning at: " << std::ctime(&now) << std::endl;

    std::list<Partition> partitions;
    if(isNaturalScan || isRecordIdScan) {
        partitions.push_back(Partition{uri, isNaturalScan, isRecordIdScan});
    } else {
        std::string bAutoDoc =
            fmt::format("{{\"groupBy\": \"$_id\", \"buckets\": {}}}", numThreads);
        std::cout << bAutoDoc << std::endl;

        auto qObj = bsoncxx::from_json(bAutoDoc);

        auto pipeline = std::move(mongocxx::pipeline().bucket_auto({qObj}));

        mongocxx::options::aggregate opts;
        opts.max_time(std::chrono::milliseconds{3600000});
        opts.allow_disk_use(true);
        opts.hint(mongocxx::hint{"_id_"});
        auto cursor = collection.aggregate(pipeline, opts);

        int numBuckets = 0;

        bool first = true;
        for (auto&& doc : cursor) {
            if (!first) {
                auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                std::cout << "First doc at: " << std::ctime(&now) << std::endl;
                first = true;
            }
            partitions.push_back(Partition{
                numBuckets, uri, doc["_id"]["min"].get_int64(), doc["_id"]["max"].get_int64()});
            ++numBuckets;
        }
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
