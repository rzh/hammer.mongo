package profiles

import (
	"fmt"

	"strconv"
	"sync"
	"sync/atomic"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

var BulkPayload [2048]byte
var payload_400k []int32 = make([]int32, 100*1024)

type bulkInsertProfile struct {
	UID    int64
	MaxUID int64

	indexGroup bool

	session     *mgo.Session
	initProfile sync.Once
}

var _bulkInsertProfile bulkInsertProfile

func (i bulkInsertProfile) SendNext(s *mgo.Session, worker_id int) error {
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))
	var err error
	var results interface{}

	_u := atomic.AddInt64(&_bulkInsertProfile.UID, 1) // to make this unique

	var docs []bson.M = make([]bson.M, ht_insert_batch_size)

	for i := 0; i < ht_insert_batch_size; i++ {
		docs[i] = bson.M{
			"_id": _u,
			// "payload": payload_400k,
			"payload": rands[worker_id].Int63(),
		}
	}

	if _profile_use_legacy_write {
		err = c.Insert(docs)
	} else {
		err = c.Database.Run(bson.D{{"insert", c.Name},
			{"documents", docs}}, results)
	}

	return err
}

func (i bulkInsertProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		_initdb = false // disable initdb
		if _initdb {
			_bulkInsertProfile.MaxUID = 0
			for i := 1; i <= _multi_db; i++ {
				for j := 1; j <= _multi_col; j++ {
					_ = s.DB(default_db_name_prefix + strconv.Itoa(i)).C(default_col_name_prefix + strconv.Itoa(j))
					if _bulkInsertProfile.indexGroup {
						// c.EnsureIndexKey("group")
					}

				}
			}

		} else if _bulkInsertProfile.MaxUID == -1 {
			// c := s.DB(_db_name).C("people")
			c := s.DB(default_db_name_prefix + "1").C(default_col_name_prefix + "1")

			// to find out how many records we have
			n, err := c.Count()

			if err != nil {
				panic("cannot count collection")
			}

			for i := 1; i <= _multi_db; i++ {
				for j := 1; j <= _multi_col; j++ {
					c := s.DB(default_db_name_prefix + strconv.Itoa(i)).C(default_col_name_prefix + strconv.Itoa(j))
					if _bulkInsertProfile.indexGroup {
						c.EnsureIndexKey("group")
					}
				}
			}

			_bulkInsertProfile.MaxUID = int64(n)
			fmt.Println("Total doc", n)
		}
	}

	_bulkInsertProfile.initProfile.Do(f)

	return nil
}

func (i bulkInsertProfile) CsvString(total_time float64) string {
	return ""
}

func (i bulkInsertProfile) CsvHeader() string {
	return ""
}

func init() {
	atomic.StoreInt64(&_bulkInsertProfile.UID, -1) // UID starts with 1
	atomic.StoreInt64(&_bulkInsertProfile.MaxUID, -1)

	registerProfile("BULKINSERT", func() Profile {
		return Profile(_bulkInsertProfile) // use the same instance
	})
}
