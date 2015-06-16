package profiles

import (
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert into capped collection only profile, insert a doc of 400 bytes
*/

type cappedCollInsertReadProfile struct {
	UID int64

	indexTTL time.Duration

	initProfile sync.Once

	session *mgo.Session
}

var _cappedCollInsertReadProfile cappedCollInsertReadProfile

// func Int2ObjId(i int64) string {
// 	// return string represenation of UID
// }

func (i cappedCollInsertReadProfile) SendNext(s *mgo.Session, worker_id int) error {
	var err error
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))

	// r := rands[worker_id].Int63n(3)

	if worker_id == 1 || worker_id == 2 {

		num := (_cappedCollInsertReadProfile.UID - 10000)

		c.Find(bson.M{"name": &num}).Limit(1)
	} else {
		_u := atomic.AddInt64(&_cappedCollInsertReadProfile.UID, 1) // to make this unique

		doc := bson.M{
			// "_id":      _u,
			"name":     _u,
			"group":    rands[worker_id].Int(),
			"payload":  &Payload,
			"payload1": &Payload1,
			"payload2": &Payload2,
			"payload3": &Payload3,
			"payload4": &Payload4,
			"payload5": &Payload5,
			"payload6": &Payload6}

		if _profile_use_legacy_write {
			err = c.Insert(doc)
		} else {
			var results interface{}

			err = c.Database.Run(bson.D{{"insert", c.Name},
				{"documents", []bson.M{doc}}}, results)
		}
	}

	panicOnError(err)
	return err
}

func initCappedInsertReadTest(session *mgo.Session, _initdb bool) {
	log.Println("Initialize simple DB. with initdb flag ", _initdb)

	ci := new(mgo.CollectionInfo)
	ci.Capped = true
	ci.MaxBytes = 1000000000

	log.Println(". Init DB, drop collections")

	for i := 1; i <= _multi_db; i++ {
		for j := 1; j <= _multi_col; j++ {
			collection := session.DB(default_db_name_prefix + strconv.Itoa(i)).C(default_col_name_prefix + strconv.Itoa(j))
			collection.DropCollection()
			collection.Create(ci)
			ensureIndexKeysHelper(collection)
		}
	}

}

func ensureIndexKeysHelper(collection *mgo.Collection) {
	indexGroup := mgo.Index{
		Key:         []string{"group"},
		ExpireAfter: _cappedCollInsertProfile.indexTTL,
	}

	err := collection.EnsureIndexKey("name")
	if err != nil {
		panic(err)
	}

	err = collection.EnsureIndex(indexGroup)
	if err != nil {
		panic(err)
	}
}

func (i cappedCollInsertReadProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s
	f := func() {
		initCappedInsertReadTest(s, _initdb)
	}

	_cappedCollInsertReadProfile.initProfile.Do(f)
	return nil
}

func (i cappedCollInsertReadProfile) CsvString(total_time float64) string {
	return ""
}

func (i cappedCollInsertReadProfile) CsvHeader() string {
	return ""
}

func init() {
	atomic.StoreInt64(&_cappedCollInsertReadProfile.UID, -1) // UID starts with 1

	registerProfile("CAPPED_COLL_INSERT_READ", func() Profile {
		return Profile(_cappedCollInsertReadProfile) // use the same instance
	})

	_cappedCollInsertProfile.indexTTL = time.Duration(getOSEnvFlagInt("HT_INDEX_TTL", 0)) * time.Second
}
