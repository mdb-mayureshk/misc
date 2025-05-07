//Inserts 100GB of data, consisting of 50M docs of 2K each.
//Run from an evergreen machine to get reasonable upload times

const { MongoClient, ServerApiVersion, ExplainVerbosity } = require('mongodb');
//replace as appropriate
const uri = "mongodb+srv://mayureshtmp:mayureshtmp@cluster0.qcohq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";
const crypto = require("crypto")

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  }
});

function bytesToInt64(bytes) {
  if (bytes.length !== 8) {
    throw new Error("Byte array must be 8 bytes long for Int64 conversion.");
  }

  let result = 0n;
  for (let i = 0; i < 8; i++) {
    result <<= 8n;
    result |= BigInt(bytes[i]);
  }
  return result;
}

function getRandomInt64() {
  return bytesToInt64(crypto.randomBytes(8));
}

function generateRandomString(length) {
  const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
}

function generateShardId(idx) {
    const myShards = ["RedShard", "BlueShard", "YellowShard"]; 
    return myShards[idx % 3];
}

async function run() {
  try {
    var useObjectId = false;
    if(process.argv.length > 2) {
      if(process.argv[2] === "--objectid") {
        useObjectId = true;
      }
    }
    // Connect the client to the server	(optional starting in v4.7)
    await client.connect();
    // Send a ping to confirm a successful connection
    await client.db("admin").command({ ping: 1 });
    console.log("Pinged your deployment. You successfully connected to MongoDB!");

    const database = client.db("cDB");
    const coll = database.collection("cColl");

    var idx = 0;
    //var outerLoopCount = 100*1000;
    var outerLoopCount = 5*1000;
    var innerBatchCount = 2*5000;
    const twoKString = generateRandomString(2048) 
    for (let k = 0; k < outerLoopCount; k++) {
        var docs = [];
        for (let i = 0; i < innerBatchCount; i++) {
          if(useObjectId) {
            docs.push({idx: idx, myShard: generateShardId(idx), val: twoKString});
          } else {
            docs.push({_id: getRandomInt64(), idx: idx, val: twoKString});
          }
          idx += 1;
        }
        const options = {ordered: true};
        const result = await coll.insertMany(docs, options);
        if(k % 1000 == 0) {
          console.log(`Finished: ${k}`);
        }
    }
  } finally {
      // Ensures that the client will close when you finish/error
      await client.close();
  }
}
run().catch(console.dir);
