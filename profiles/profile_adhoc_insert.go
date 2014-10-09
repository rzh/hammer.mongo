package profiles

import (
	// "fmt"
	"math/rand"
	"sync/atomic"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

type AdHocPerson struct {
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

type adhocInsertProfile struct {
	UID int64

	session *mgo.Session
}

var _adhocInsertProfile adhocInsertProfile

// func Int2ObjId(i int64) string {
// 	// return string represenation of UID
// }

var simpleArray [20]int

func randomArrayInt(n int) []int {
	a := make([]int, n)

	for i := 0; i < n; i++ {
		a[i] = rand.Int()
	}

	return a
}

func (i adhocInsertProfile) SendNext(s *mgo.Session, worker_id int) error {
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))

	_u := atomic.AddInt64(&_adhocInsertProfile.UID, 1) // to make this unique

	// err := c.Insert(&AdHocPerson{Name: 100, UID: bson.ObjectIdHex(fmt.Sprintf("%#x", _u)), Group: 100}) // insert a new record
	err := c.Insert(bson.M{
		"_id":   _u,
		"name":  randomString(20),
		"group": rand.Int(),
		//"simpleArray": randomArrayInt(20),
		//"payload":     &Payload,
		//"payload1":    &Payload1,
		//"payload2":    &Payload2,
		//"payload3":    &Payload3,
		//"payload4":    &Payload4,
		//"payload5": &Payload5,
		"payload6": &Payload6})

	return err
}

func (i adhocInsertProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	// InitSimpleTest(s, _initdb)

	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))

	err := c.EnsureIndexKey("simpleArray")
	panicOnError(err)
	err = c.EnsureIndexKey("group")
	panicOnError(err)

	return err
}

func (i adhocInsertProfile) CsvString(total_time float64) string {
	return ""
}

func (i adhocInsertProfile) CsvHeader() string {
	return ""
}

func init() {
	// fmt.Println("Init INSERT profile")

	atomic.StoreInt64(&_adhocInsertProfile.UID, -1) // UID starts with 1

	registerProfile("ADHOCINSERT", func() Profile {
		return Profile(_adhocInsertProfile) // use the same instance
	})

	// fmt.Println("Done Init INSERT profile")
}
