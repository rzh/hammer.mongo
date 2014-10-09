package profiles

import (
	"math/rand"
	"time"

	"gopkg.in/mgo.v2"
)

/*
  basic insert only profile, insert a doc of 400 bytes
*/

type mixed_Profile struct {
	session *mgo.Session
}

var _mixed_Profile mixed_Profile

func (i mixed_Profile) SendNext(s *mgo.Session, worker_id int) error {

	var err error

	r := rand.Int63n(3)
	if r == 0 {
		err = _query_single_doc_Profile.SendNext(s, worker_id)
	} else if r == 1 {
		err = _inPlaceUpdateProfile.SendNext(s, worker_id)
	} else if r == 2 {
		err = _extendUpdateProfile.SendNext(s, worker_id)
	} else {
		err = _insertProfile.SendNext(s, worker_id) // not run this
	}

	return err
}

func (i mixed_Profile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	_query_single_doc_Profile.SetupTest(s, _initdb)
	_inPlaceUpdateProfile.SetupTest(s, _initdb)
	_extendUpdateProfile.SetupTest(s, _initdb)
	// _insertProfile.SetupTest(s, _initdb)

	return nil
}

func (i mixed_Profile) CsvString(total_time float64) string {
	return ""
}

func (i mixed_Profile) CsvHeader() string {
	return ""
}

func init() {
	rand.Seed(time.Now().UnixNano())

	_profile_name := "mixed"
	// fmt.Println("Init ", _profile_name, "  profile")

	registerProfile(_profile_name, func() Profile {
		return Profile(_mixed_Profile) // use the same instance
	})
}
