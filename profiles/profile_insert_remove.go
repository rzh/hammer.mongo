package profiles

import (
	"fmt"
	"math/rand"
	"time"

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

	c := s.DB("test1").C("people")

	if _insertDeleteProfile.MaxUID == 0 {
		// to find out how many records we have
		n, err := c.Count()

		if err != nil {
			panic("cannot count collection")
		}
		_insertDeleteProfile.MaxUID = int64(n)
		fmt.Println("Total doc", n)
	}

	_u := rand.Int63n(_insertDeleteProfile.MaxUID) // to find a random person

	c.Remove(bson.M{"_id": _u})
	c.Insert(bson.M{
		"_id":      _u,
		"name":     _u,
		"group":    rand.Int(),
		"payload":  &Payload,
		"payload1": &Payload1,
		"payload2": &Payload2,
		"payload3": &Payload3,
		"payload4": &Payload4,
		"payload5": &Payload5,
		"payload6": &Payload6})

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
	rand.Seed(time.Now().UnixNano())

	// fmt.Println("Init Inplace Update  profile")

	registerProfile("InsertDelete", func() Profile {
		return Profile(_insertDeleteProfile) // use the same instance
	})

	// fmt.Println("Done Init InPlaceUpdte profile")
}
