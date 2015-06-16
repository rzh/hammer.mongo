package profiles

import (
	"fmt"
	"log"
	"os"
	"sync"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

type query_single_doc_Profile struct {
	MaxUID      int64
	USE_ID      bool
	initProfile sync.Once

	session *mgo.Session
}

var _query_single_doc_Profile query_single_doc_Profile

func (i query_single_doc_Profile) SendNext(s *mgo.Session, worker_id int) error {
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))

	_u := rands[worker_id].Int63n(_query_single_doc_Profile.MaxUID) // to find a random person
	//_p := SmallDoc{}
	var result interface{}

	var err error
	if i.USE_ID {
		err = c.Find(bson.M{"_id": _u}).One(&result) //.Explain(&_p)
	} else {
		// query to not use _ID
		err = c.Find(bson.M{"name": _u}).One(&result) //.Explain(&_p)
	}

	if err != nil {
		fmt.Println("Error: ", err, "user [", _u, "] in collection", c.FullName)
	}
	return err
}

func initSingleQuery(s *mgo.Session) {
	_query_single_doc_Profile.MaxUID = -1
	// to find out how many records we have
	var dbName, colName string

	for i := 1; i <= _multi_db; i++ {
		dbName = fmt.Sprint(default_db_name_prefix, i)

		for j := 1; j <= _multi_col; j++ {
			colName = fmt.Sprint(default_col_name_prefix, j)

			c := s.DB(dbName).C(colName)

			n, err := c.Count()

			if err != nil {
				panic("cannot count collection")
			}

			if _query_single_doc_Profile.MaxUID > int64(n) || _query_single_doc_Profile.MaxUID < 0 {
				// found out the smalles collection
				// here we assume most collection will be similar or equal size
				// which shall be true based on test design
				// TODO: enhance this
				_query_single_doc_Profile.MaxUID = int64(n)
			}
		}
	}

	if _query_single_doc_Profile.MaxUID == 0 {
		log.Fatalln("Error: total document to be queries is 0!")
		os.Exit(1)
	}
	log.Println("Total document to be queried is", _query_single_doc_Profile.MaxUID)
}

func (i query_single_doc_Profile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		initSingleQuery(s)
	}

	_query_single_doc_Profile.initProfile.Do(f)
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
	if os.Getenv("HT_USE_ID") != "" {
		_query_single_doc_Profile.USE_ID = true
	} else {
		_query_single_doc_Profile.USE_ID = false
	}
}
