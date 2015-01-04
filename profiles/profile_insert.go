package profiles

import (
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

type Person struct {
	Name     int64
	UID      bson.ObjectId `bson:"_id,omitempty"`
	Group    int
	Payload  [40]int
	Payload1 [120]byte
	Payload2 [120]byte
	Payload3 [120]byte
	Payload4 [120]byte
	Payload5 [120]byte
	Payload6 [120]byte
}

var Payload [40]int
var Payload1 [120]byte
var Payload2 [120]byte
var Payload3 [120]byte
var Payload4 [120]byte
var Payload5 [120]byte
var Payload6 [120]byte

type insertProfile struct {
	UID int64

	indexGroup bool

	initProfile sync.Once

	session *mgo.Session
}

var _insertProfile insertProfile

// func Int2ObjId(i int64) string {
// 	// return string represenation of UID
// }

func (i insertProfile) SendNext(s *mgo.Session, worker_id int) error {
	var err error
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))

	_u := atomic.AddInt64(&_insertProfile.UID, 1) // to make this unique

	doc := bson.M{
		// "_id":      _u,
		"name":     _u,
		"group":    rand.Int(),
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

func InitSimpleTest(session *mgo.Session, _initdb bool) {
	log.Println("Initialize simple DB. with initdb flag ", _initdb)

	_initdb = true

	if !_initdb {
		panic("flag is false")
	}

	// drop the colelction here  TODO?  FIXME:
	if _initdb {
		log.Println(". Init DB, drop collections")
		session.DB(_db_name).C("people").DropCollection()

		// may drop DB here as well TODO:
	} // this will be moved to each profile. FIXME:

	// follow should be moved into SetupTest
	collection := session.DB(_db_name).C("people")

	err := collection.EnsureIndexKey("name")
	if err != nil {
		panic(err)
	}

	// err = collection.EnsureIndexKey("group")
	err = collection.EnsureIndexKey("uid")
	if err != nil {
		panic(err)
	}
}

func (i insertProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		InitSimpleTest(s, _initdb)
	}

	_insertProfile.initProfile.Do(f)
	return nil
}

func (i insertProfile) CsvString(total_time float64) string {
	return ""
}

func (i insertProfile) CsvHeader() string {
	return ""
}

func init() {
	// fmt.Println("Init INSERT profile")

	atomic.StoreInt64(&_insertProfile.UID, -1) // UID starts with 1

	registerProfile("INSERT", func() Profile {
		return Profile(_insertProfile) // use the same instance
	})

	s := os.Getenv("HT_INDEX_GROUP")
	if s == "" {
		_insertProfile.indexGroup = false
	} else {
		_insertProfile.indexGroup = true
	}

	// fmt.Println("Done Init INSERT profile")
}
