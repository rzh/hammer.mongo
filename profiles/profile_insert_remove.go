package profiles

import (
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

type insertDeleteProfile struct {
	MaxUID int64

	session *mgo.Session
}

var _insertDeleteProfile insertDeleteProfile

func (i insertDeleteProfile) SendNext(s *mgo.Session, worker_id int) error {

	// c := s.DB("test1").C("people")
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))

	if _insertDeleteProfile.MaxUID == 0 {
		// to find out how many records we have
		n, err := c.Count()

		if err != nil {
			panic("cannot count collection")
		}
		_insertDeleteProfile.MaxUID = int64(n)
		fmt.Println("Total doc", n)
	}

	_u := rands[worker_id].Int63n(_insertDeleteProfile.MaxUID) // to find a random person

	doc := bson.M{
		"_id":     _u,
		"name":    _u,
		"group":   rands[worker_id].Int(),
		"payload": &Payload}

	if _profile_use_legacy_write {
		c.Remove(bson.M{"_id": _u})
		c.Insert(doc)
	} else {
		var results interface{}

		c.Database.Run(bson.D{{"delete", c.Name},
			{"query", []bson.M{bson.M{"_id": _u}}}}, results)
		c.Database.Run(bson.D{{"insert", c.Name},
			{"documents", []bson.M{doc}}}, results)
	}
	return nil
}

func (i insertDeleteProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	return nil
}

func (i insertDeleteProfile) CsvString(total_time float64) string {
	return ""
}

func (i insertDeleteProfile) CsvHeader() string {
	return ""
}

func init() {
	_insertDeleteProfile.MaxUID = 0

	// fmt.Println("Init Inplace Update  profile")

	registerProfile("InsertDelete", func() Profile {
		return Profile(_insertDeleteProfile) // use the same instance
	})

	// fmt.Println("Done Init InPlaceUpdte profile")
}
