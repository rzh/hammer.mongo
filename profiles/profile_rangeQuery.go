package profiles

import (
	"fmt"
	"os"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

type queryRangeProfile struct {
	MaxUID    int64
	USE_NO_Id bool

	session *mgo.Session

	explainStats ProfileStats
}

var _queryRangeProfile queryRangeProfile

func (i queryRangeProfile) SendNext(s *mgo.Session, worker_id int) error {
	c := s.DB("test1").C("people")

	if _queryRangeProfile.MaxUID == 0 {
		// to find out how many records we have
		n, err := c.Count()

		if err != nil {
			panic("cannot count collection")
		}
		_queryRangeProfile.MaxUID = int64(n)
		fmt.Println("Total doc", n)
	}

	_u := rands[worker_id].Intn(1000) // to find a random person
	_p := bson.M{}

	var err error
	err = c.Find(bson.M{"group": bson.M{"$gt": _u}}).Limit(20).Explain(&_p)

	return err
}

func (i queryRangeProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	return nil
}

func (i queryRangeProfile) CsvString(total_time float64) string {
	return ""
}

func (i queryRangeProfile) CsvHeader() string {
	return ""
}

func init() {
	_queryRangeProfile.MaxUID = 0

	_profile_name := "rangeQuery"
	// fmt.Println("Init ", _profile_name, "  profile")

	registerProfile(_profile_name, func() Profile {
		return Profile(_queryRangeProfile) // use the same instance
	})

	// fmt.Println("Done ", _profile_name, " profile")

	// this is a hack! to be removed later  FIXME
	if os.Getenv("HT_NO_ID") != "" {
		_queryRangeProfile.USE_NO_Id = true
	} else {
		_queryRangeProfile.USE_NO_Id = false
	}
}
