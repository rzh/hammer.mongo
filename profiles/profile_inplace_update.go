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

type inPlaceUpdateProfile struct {
	MaxUID int64

	session *mgo.Session
}

var _inPlaceUpdateProfile inPlaceUpdateProfile

func (i inPlaceUpdateProfile) SendNext(s *mgo.Session, worker_id int) error {

	c := s.DB(_db_name).C("people")

	if _inPlaceUpdateProfile.MaxUID == 0 {
		// to find out how many records we have
		n, err := c.Count()

		if err != nil {
			panic("cannot count collection")
		}
		_inPlaceUpdateProfile.MaxUID = int64(n)
		fmt.Println("Total doc", n)
	}

	_u := rand.Int63n(_inPlaceUpdateProfile.MaxUID) // to find a random person

	err := c.Update(bson.M{"_id": _u}, bson.M{"$set": bson.M{"group": rand.Int()}}) // insert a new record
	// err := db.C("posts").UpdateId(id, bson.M{"$set": bson.M{"field1": "v1"}})

	if err != nil {
		fmt.Println(err, "uid is ", _u)
	}
	return err
}

func (i inPlaceUpdateProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	return nil
}

func (i inPlaceUpdateProfile) CsvString(total_time float64) string {
	return ""
}

func (i inPlaceUpdateProfile) CsvHeader() string {
	return ""
}

func init() {
	_inPlaceUpdateProfile.MaxUID = 0
	rand.Seed(time.Now().UnixNano())

	// fmt.Println("Init Inplace Update  profile")

	registerProfile("InPlaceUpdate", func() Profile {
		return Profile(_inPlaceUpdateProfile) // use the same instance
	})

	// fmt.Println("Done Init InPlaceUpdte profile")
}
