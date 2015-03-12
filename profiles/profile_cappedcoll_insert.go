package profiles

import (
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert into capped collection only profile, insert a doc of 400 bytes
*/

type cappedCollInsertProfile struct {
	UID int64

	indexGroup bool

	initProfile sync.Once

	session *mgo.Session
}

var _cappedCollInsertProfile cappedCollInsertProfile

// func Int2ObjId(i int64) string {
// 	// return string represenation of UID
// }

func (i cappedCollInsertProfile) SendNext(s *mgo.Session, worker_id int) error {
	var err error
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))

	_u := atomic.AddInt64(&_cappedCollInsertProfile.UID, 1) // to make this unique

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

	panicOnError(err)
	return err
}

func initCappedTest(session *mgo.Session, _initdb bool) {
	log.Println("Initialize simple DB. with initdb flag ", _initdb)

	ci := new(mgo.CollectionInfo)
	ci.Capped = true
	ci.MaxBytes = 1000000000

	_initdb = true

	log.Println(". Init DB, drop collections")
	session.DB(default_db_name_prefix).C(default_col_name_prefix).DropCollection()
	session.DB(default_db_name_prefix).C(default_col_name_prefix).Create(ci)

	for i := 1; i < _multi_db; i++ {
		for j := 1; j < _multi_col; j++ {
			collection := session.DB(default_db_name_prefix + strconv.Itoa(i)).C(default_col_name_prefix + strconv.Itoa(j))
			collection.Create(ci)
			err := collection.EnsureIndexKey("name")
			if err != nil {
				panic(err)
			}

			err = collection.EnsureIndexKey("uid")
			if err != nil {
				panic(err)
			}
		}
	}

}

func (i cappedCollInsertProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		initCappedTest(s, _initdb)
	}

	_cappedCollInsertProfile.initProfile.Do(f)
	return nil
}

func (i cappedCollInsertProfile) CsvString(total_time float64) string {
	return ""
}

func (i cappedCollInsertProfile) CsvHeader() string {
	return ""
}

func init() {
	atomic.StoreInt64(&_cappedCollInsertProfile.UID, -1) // UID starts with 1

	registerProfile("CAPPED_COLL_INSERT", func() Profile {
		return Profile(_cappedCollInsertProfile) // use the same instance
	})

	s := os.Getenv("HT_INDEX_GROUP")
	if s == "" {
		_cappedCollInsertProfile.indexGroup = false
	} else {
		_cappedCollInsertProfile.indexGroup = true
	}
}
