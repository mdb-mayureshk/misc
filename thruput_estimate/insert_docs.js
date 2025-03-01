//Inserts 100GB of data, consisting of 100k docs of 1MB each.
//Run from an evergreen machine to get reasonable upload times

const { MongoClient, ServerApiVersion } = require('mongodb');
//replace as appropriate
const uri = "mongodb+srv://mayuresh:mayuresh@cluster0.gccou.mongodb-dev.net/?retryWrites=true&w=majority&appName=Cluster0";

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  }
});

async function run() {
  try {
    // Connect the client to the server	(optional starting in v4.7)
    await client.connect();
    // Send a ping to confirm a successful connection
    await client.db("admin").command({ ping: 1 });
    console.log("Pinged your deployment. You successfully connected to MongoDB!");

    const database = client.db("cDB");
    const coll = database.collection("cColl");

    const oneMBString = 'x'.repeat(1024*1024);
    var idx = 0;
    for (let k = 0; k < 20*500; k++) {
        var docs = [];
        for (let i = 0; i < 10; i++) {
            docs.push({idx: idx, val: oneMBString});
            idx += 1;
        }
        const options = {ordered: true};
        const result = await coll.insertMany(docs, options);
        if(k % 10000 == 0) {
          console.log(`Finished: ${k}`);
        }
    }
  } finally {
      // Ensures that the client will close when you finish/error
      await client.close();
  }
}
run().catch(console.dir);
