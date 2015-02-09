package profiles

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

type insertSmallProfile struct {
	UID    int64
	MaxUID int64

	session     *mgo.Session
	initProfile sync.Once
}

var _insertSmallProfile insertSmallProfile
var qa373ArrayPayload [25]int
var insert_with_id bool = true
var __index_field_group bool = false

type SmallDoc struct {
	Name    int64
	UID     bson.ObjectId `bson:"_id,omitempty"`
	Group   int
	Payload [25]int
}

func (i insertSmallProfile) SendNext(s *mgo.Session, worker_id int) error {
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))
	var err error
	var doc bson.M

	if insert_with_id {
		_u := atomic.AddInt64(&_insertSmallProfile.UID, 1) // to make this unique
		doc = bson.M{"_id": _u, "group": rands[worker_id].Int63()}
	} else {
		doc = bson.M{"group": rands[worker_id].Int63()}
	}

	if _profile_use_legacy_write {
		err = c.Insert(doc)
	} else {
		var results interface{}
		err = c.Database.Run(bson.D{{"insert", c.Name},
			{"documents", []bson.M{doc}}}, results)
	}

	return err
}

func (i insertSmallProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		_initdb = false

		if _initdb {
			_insertSmallProfile.MaxUID = 0
			for i := 1; i <= _multi_db; i++ {
				for j := 1; j <= _multi_col; j++ {
					c := s.DB(default_db_name_prefix + strconv.Itoa(i)).C(default_col_name_prefix + strconv.Itoa(j))
					if __index_field_group {
						c.EnsureIndexKey("group")
					}
				}
			}

		} else if _insertSmallProfile.MaxUID == -1 {
			c := s.DB(default_db_name_prefix + "1").C(default_col_name_prefix + "1")

			// to find out how many records we have
			n, err := c.Count()

			if err != nil {
				panic("cannot count collection")
			}

			for i := 1; i <= _multi_db; i++ {
				for j := 1; j <= _multi_col; j++ {
					if __index_field_group {
						c.EnsureIndexKey("group")
					}
				}
			}

			_insertSmallProfile.MaxUID = int64(n)
			fmt.Println("Total doc", n)
		}
	}

	_insertSmallProfile.initProfile.Do(f)

	return nil
}

func (i insertSmallProfile) CsvString(total_time float64) string {
	return ""
}

func (i insertSmallProfile) CsvHeader() string {
	return ""
}

func init() {
	atomic.StoreInt64(&_insertSmallProfile.UID, -1) // UID starts with 1
	atomic.StoreInt64(&_insertSmallProfile.MaxUID, -1)

	registerProfile("INSERTSMALL", func() Profile {
		return Profile(_insertSmallProfile) // use the same instance
	})

	for i := 0; i < len(qa373ArrayPayload); i++ {
		qa373ArrayPayload[i] = i
	}

	s := os.Getenv("HT_INDEX_FIELD_GROUP")
	if s == "" {
		__index_field_group = false
	} else {
		__index_field_group = true
	}

	s = os.Getenv("HT_INSERT_WITH_ID")
	if s != "" {
		log.Println("HT: send without _id")
		insert_with_id = false
	} else {
		log.Println("HT: send with _id")
		insert_with_id = true
	}
}
