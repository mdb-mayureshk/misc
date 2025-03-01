let pipeline = [
{
   $source: {
      connectionName: "benchmark",
      db: "cDB",
      coll: "cColl",
      //gets set at sp create. Docs will be inserted
      //before starting sp.
      config: { startAtOperationTime: ISODate() }
   }
},
{
   //filter everything out
   $match: { field: "dummy" }
},
{
   $merge: {
	into: {
	  connectionName: "benchmark",
	  db: "outDb",
	  coll: "outColl"
	}
   }
}
]

