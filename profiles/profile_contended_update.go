package profiles

import (
	"sync"
	"sync/atomic"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	wrapper "github.com/rzh/hammer.mongo/mgowrapper"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

type contendedUpdate struct {
	UID int64

	indexGroup bool

	initProfile sync.Once

	session *mgo.Session
}

var _contendedUpdate contendedUpdate

// func Int2ObjId(i int64) string {
// 	// return string represenation of UID
// }

func (i contendedUpdate) SendNext(s *mgo.Session, worker_id int) error {

	err := wrapper.Update(s.DB("test").C("test"),
		[]bson.M{bson.M{
			"q": bson.M{"_id": 1},
			"u": bson.M{"$inc": bson.M{"uid": 1}}}})
	return err
}

func (i contendedUpdate) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		// insert one doc
		var results interface{}

		doc := bson.M{
			"_id":   1,
			"name":  "test",
			"uid":   0,
			"price": 100,
		}

		s.DB("test").C("test").RemoveId(1)
		s.DB("test").Run(bson.D{{"insert", "test"},
			{"documents", []bson.M{doc}}}, results)
	}

	_contendedUpdate.initProfile.Do(f)
	return nil
}

func (i contendedUpdate) CsvString(total_time float64) string {
	return ""
}

func (i contendedUpdate) CsvHeader() string {
	return ""
}

func init() {
	// fmt.Println("Init INSERT profile")

	atomic.StoreInt64(&_contendedUpdate.UID, -1) // UID starts with 1

	registerProfile("CONTENDED_UPDATE", func() Profile {
		return Profile(_contendedUpdate) // use the same instance
	})
}
