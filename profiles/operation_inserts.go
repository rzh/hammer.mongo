package profiles

import (
	"log"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var payload_4M []int32 = make([]int32, 1024*1024)
var ht_insert_batch_size int = 1 // default to single insert
var ht_insert_without_id int = 0 // default to with id
var ht_query_max_id int = 0

func insert_16M_doc_no_id(c *mgo.Collection, MaxUID int, worker_id int) error {
	err := c.Insert(bson.M{
		"pld1": payload_4M,
		"pld2": payload_4M,
		"pld4": payload_4M})

	return err
}

func op_query(c *mgo.Collection, MaxUID int, worker_id int) error {
	_u := rands[worker_id].Intn(5000) // to find a random person
	var _p []interface{}

	var err error
	err = c.Find(bson.M{"group": bson.M{"$gt": _u}}).Limit(20).Select(bson.M{"group": 1, "_id": 0}).All(&_p)

	return err
}

func write_insert_doc(c *mgo.Collection, MaxUID int, worker_id int) error {
	var results interface{}
	var docs []bson.M = make([]bson.M, ht_insert_batch_size)

	for i := 0; i < ht_insert_batch_size; i++ {
		if ht_insert_without_id > 0 {
			// doc without _id
			docs[i] = bson.M{
				"f1":  "12345678",
				"f2":  "12345678",
				"f3":  "12345678",
				"f4":  "12345678",
				"f5":  "12345678",
				"f6":  "12345678",
				"f7":  "12345678",
				"f8":  "12345678",
				"f9":  "12345678",
				"f10": "12345678",
				"f11": "12345678",
				"f12": "12345678",
				"f13": "12345678",
				"f14": "12345678",
				"f15": "12345678",
				"f16": "12345678",
				"f17": "12345678",
				"f18": "12345678",
				"f19": "12345678",
				"f20": "12345678",
				"i1":  "12345678",
				"i2":  "12345678",
				"i3":  "12345678",
				"i4":  "12345678",
				"i5":  "12345678",
				"i6":  "12345678",
				"i7":  "12345678",
				"i8":  "12345678",
				"i9":  "12345678",
				"i10": "12345678",
				"i11": "12345678",
				"i12": "12345678",
				"i13": "12345678",
				"i14": "12345678",
				"i15": "12345678",
				"i16": "12345678",
				"i17": "12345678",
				"i18": "12345678",
				"i19": "12345678",
				"i20": "12345678",
				"t1":  "12345678",
				"t2":  "12345678",
				"t3":  "12345678",
				"t4":  "12345678",
				"t5":  "12345678",
				"t6":  "12345678",
				"t7":  "12345678",
				"t8":  "12345678",
				"t9":  "12345678",
				"t10": "12345678",
				"t11": "12345678",
				"t12": "12345678",
				"t13": "12345678",
				"t14": "12345678",
				"t15": "12345678",
				"t16": "12345678",
				"t17": "12345678",
				"t18": "12345678",
				"t19": "12345678",
				"t20": "12345678"}
		} else {
			// doc with _id
			docs[i] = bson.M{
				"f1":  "12345678",
				"f2":  "12345678",
				"f3":  "12345678",
				"f4":  "12345678",
				"f5":  "12345678",
				"f6":  "12345678",
				"f7":  "12345678",
				"f8":  "12345678",
				"f9":  "12345678",
				"f10": "12345678",
				"f11": "12345678",
				"f12": "12345678",
				"f13": "12345678",
				"f14": "12345678",
				"f15": "12345678",
				"f16": "12345678",
				"f17": "12345678",
				"f18": "12345678",
				"f19": "12345678",
				"f20": "12345678",
				"i1":  "12345678",
				"i2":  "12345678",
				"i3":  "12345678",
				"i4":  "12345678",
				"i5":  "12345678",
				"i6":  "12345678",
				"i7":  "12345678",
				"i8":  "12345678",
				"i9":  "12345678",
				"i10": "12345678",
				"i11": "12345678",
				"i12": "12345678",
				"i13": "12345678",
				"i14": "12345678",
				"i15": "12345678",
				"i16": "12345678",
				"i17": "12345678",
				"i18": "12345678",
				"i19": "12345678",
				"i20": "12345678",
				"t1":  "12345678",
				"t2":  "12345678",
				"t3":  "12345678",
				"t4":  "12345678",
				"t5":  "12345678",
				"t6":  "12345678",
				"t7":  "12345678",
				"t8":  "12345678",
				"t9":  "12345678",
				"t10": "12345678",
				"t11": "12345678",
				"t12": "12345678",
				"t13": "12345678",
				"t14": "12345678",
				"t15": "12345678",
				"t16": "12345678",
				"t17": "12345678",
				"t18": "12345678",
				"t19": "12345678",
				"t20": "12345678",
				"_id": bson.NewObjectId()}
		}
	}

	err := c.Database.Run(bson.M{
		"insert":    c.Name,
		"documents": docs}, results)

	if err != nil {
		log.Println(err)
	}
	return err
}

func init() {
	ht_insert_batch_size = getOSEnvFlag("HT_INSERT_BATCH_SIZE", 1, 1)
	ht_insert_without_id = getOSEnvFlag("HT_INSERT_WITHOUT_ID", 0, 0)
	ht_query_max_id = getOSEnvFlag("HT_QUERY_MAX_ID", 1, 1)
}
