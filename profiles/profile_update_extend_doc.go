package profiles

import (
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

type extendUpdateProfile struct {
	MaxUID int64

	session *mgo.Session
}

var _extendUpdateProfile extendUpdateProfile

func (i extendUpdateProfile) SendNext(s *mgo.Session, worker_id int) error {
	c := s.DB(_db_name).C("people")

	if _extendUpdateProfile.MaxUID == 0 {
		// to find out how many records we have
		n, err := c.Count()

		if err != nil {
			panic("cannot count collection")
		}
		_extendUpdateProfile.MaxUID = int64(n)
		fmt.Println("Total doc", n)
	}

	_u := rands[worker_id].Int63n(_extendUpdateProfile.MaxUID) // to find a random person
	// var _p [100]byte

	err := c.Update(bson.M{"_id": _u}, bson.M{"$set": bson.M{"group": rands[worker_id].Int(), "newField": &Payload}}) // insert a new record
	// err := db.C("posts").UpdateId(id, bson.M{"$set": bson.M{"field1": "v1"}})

	if err != nil {
		fmt.Println(err, "uid is ", _u)
	}
	return err
}

func (i extendUpdateProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	return nil
}

func (i extendUpdateProfile) CsvString(total_time float64) string {
	return ""
}

func (i extendUpdateProfile) CsvHeader() string {
	return ""
}

func init() {
	_extendUpdateProfile.MaxUID = 0

	_profile_name := "extendedUpdate"
	// fmt.Println("Init ", _profile_name, "  profile")

	registerProfile(_profile_name, func() Profile {
		return Profile(_extendUpdateProfile) // use the same instance
	})

	// fmt.Println("Done ", _profile_name, " profile")
}
