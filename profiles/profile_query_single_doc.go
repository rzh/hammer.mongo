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

type query_single_doc_Profile struct {
	MaxUID    int64
	USE_NO_Id bool

	session *mgo.Session
}

var _query_single_doc_Profile query_single_doc_Profile

func (i query_single_doc_Profile) SendNext(s *mgo.Session, worker_id int) error {
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))

	if _query_single_doc_Profile.MaxUID == 0 {
		// to find out how many records we have
		n, err := c.Count()

		if err != nil {
			panic("cannot count collection")
		}
		_query_single_doc_Profile.MaxUID = int64(n)
		fmt.Println("Total doc", n)
	}

	_u := rands[worker_id].Int63n(_query_single_doc_Profile.MaxUID) // to find a random person
	_p := SmallDoc{}

	var err error
	if i.USE_NO_Id {
		// query to not use _ID
		err = c.Find(bson.M{"name": _u}).Limit(1).Explain(&_p)
	} else {
		err = c.Find(bson.M{"_id": _u}).Limit(1).Explain(&_p)
	}

	if err != nil {
		fmt.Println(err, "uid is ", _u)
	}
	return err
}

func (i query_single_doc_Profile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	return nil
}

func (i query_single_doc_Profile) CsvString(total_time float64) string {
	return ""
}

func (i query_single_doc_Profile) CsvHeader() string {
	return ""
}

func init() {
	_query_single_doc_Profile.MaxUID = 0

	_profile_name := "singleQuery"
	// fmt.Println("Init ", _profile_name, "  profile")

	registerProfile(_profile_name, func() Profile {
		return Profile(_query_single_doc_Profile) // use the same instance
	})

	// fmt.Println("Done ", _profile_name, " profile")

	// this is a hack! to be removed later  FIXME
	if os.Getenv("HT_NO_ID") != "" {
		_query_single_doc_Profile.USE_NO_Id = true
	} else {
		_query_single_doc_Profile.USE_NO_Id = false
	}
}
