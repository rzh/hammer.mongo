package mgowrapper

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func Update(c *mgo.Collection, docs []bson.M) (err error) {
	var results interface{}
	/*
		http://docs.mongodb.org/manual/reference/command/update/
		   {
		      update: <collection>,
		      updates:
		         [
		            { q: <query>, u: <update>, upsert: <boolean>, multi: <boolean> },
		            { q: <query>, u: <update>, upsert: <boolean>, multi: <boolean> },
		            { q: <query>, u: <update>, upsert: <boolean>, multi: <boolean> },
		            ...
		         ],
		      ordered: <boolean>,
		      writeConcern: { <write concern> }
		   }
	*/
	err = c.Database.Run(bson.D{
		{"update", c.Name},
		{"updates", docs},
	}, results)

	return
}

func Insert(c *mgo.Collection, docs []bson.M) (err error) {
	var results interface{}
	/*
		http://docs.mongodb.org/manual/reference/command/insert/
			{
			   insert: <collection>,
			   documents: [ <document>, <document>, <document>, ... ],
			   ordered: <boolean>,
			   writeConcern: { <write concern> }
			}}]
	*/

	err = c.Database.Run(
		bson.D{
			{"insert", c.Name},
			{"documents", docs}}, results)

	return
}
