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

type Person struct {
	Name    int64
	UID     bson.ObjectId `bson:"_id,omitempty"`
	Group   int
	Payload string
}

var Payload [40]int
var Payload1 [120]byte
var Payload2 [120]byte
var Payload3 [120]byte
var Payload4 [120]byte
var Payload5 [120]byte
var Payload6 [120]byte

var stageInsert bool

type insertProfile struct {
	UID int64

	indexGroup bool

	initProfile sync.Once

	session *mgo.Session
}

var _insertProfile insertProfile
var _payload_string_lens int

// func Int2ObjId(i int64) string {
// 	// return string represenation of UID
// }

func (i insertProfile) SendNext(s *mgo.Session, worker_id int) error {
	var err error
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))

	_u := atomic.AddInt64(&_insertProfile.UID, 1) // to make this unique

	doc := bson.M{
		"_id":     _u,
		"name":    _u,
		"group":   rands[worker_id].Int(),
		"payload": randomString(_payload_string_lens),
	}

	if stageInsert {
		var dbName, colName string

		for i := 1; i <= _multi_db; i++ { // start from 1
			if _multi_db == 1 {
				dbName = default_db_name_prefix
			} else {
				dbName = fmt.Sprint(default_db_name_prefix, i)
			}

			for j := 1; j <= _multi_col; j++ {
				if _multi_col == 1 {
					colName = default_col_name_prefix
				} else {
					colName = fmt.Sprint(default_col_name_prefix, j)
				}

				c = s.DB(dbName).C(colName)

				if _profile_use_legacy_write {
					err = c.Insert(doc)
				} else {
					var results interface{}

					err = c.Database.Run(bson.D{{"insert", c.Name},
						{"documents", []bson.M{doc}}}, results)
				}

				panicOnError(err)
			}
		}

	} else {
		if _profile_use_legacy_write {
			err = c.Insert(doc)
		} else {
			var results interface{}

			err = c.Database.Run(bson.D{{"insert", c.Name},
				{"documents", []bson.M{doc}}}, results)
		}

		panicOnError(err)
	}
	return nil // never come here when there is error
}

func InitSimpleTest(session *mgo.Session, _initdb bool) {
	log.Println("Initialize simple DB. with initdb flag ", _initdb)

	_initdb = true

	if !_initdb {
		panic("flag is false")
	}

	// drop the colelction here  TODO?  FIXME:
	if _initdb {
		log.Println(". Init DB, drop collections")
		session.DB(_db_name).C("people").DropCollection()
		// may drop DB here as well TODO:
	} // this will be moved to each profile. FIXME:

	var dbName, colName string

	for i := 1; i <= _multi_db; i++ {
		dbName = default_db_name_prefix

		if _multi_db != 1 {
			dbName = fmt.Sprint(default_db_name_prefix, i)
		}
		for j := 1; j <= _multi_col; j++ {
			colName = default_col_name_prefix

			if _multi_col != 1 {
				colName = fmt.Sprint(default_col_name_prefix, j)
			}
			fmt.Println("Create index for ", dbName+"."+colName)
			collection := session.DB(dbName).C(colName)
			err := collection.EnsureIndexKey("name")
			if err != nil {
				panic(err)
			}

			// err = collection.EnsureIndexKey("group")
			err = collection.EnsureIndexKey("uid")
			if err != nil {
				panic(err)
			}
		}
	}
}

func (i insertProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		InitSimpleTest(s, _initdb)
	}

	_insertProfile.initProfile.Do(f)
	return nil
}

func (i insertProfile) CsvString(total_time float64) string {
	return ""
}

func (i insertProfile) CsvHeader() string {
	return ""
}

func init() {
	// fmt.Println("Init INSERT profile")

	atomic.StoreInt64(&_insertProfile.UID, -1) // UID starts with 1

	registerProfile("INSERT", func() Profile {
		return Profile(_insertProfile) // use the same instance
	})

	s := os.Getenv("HT_INDEX_GROUP")
	if s == "" {
		_insertProfile.indexGroup = false
	} else {
		_insertProfile.indexGroup = true
	}

	s = os.Getenv("HT_STAGE_INSERT")
	if s == "" {
		stageInsert = false
	} else {
		stageInsert = true
	}

	s = os.Getenv("HT_INSERT_PAYLOAD_STRING_LENGTH")
	if s == "" {
		_payload_string_lens = 256
	} else {
		l, err := strconv.ParseInt(s, 10, 64)

		if err != nil {
			log.Fatalln("Cannot parse environment variable HT_INSERT_PAYLOAD_STRING_LENGTH, got ", s, " expecting an integer")
		}
		_payload_string_lens = int(l)
	}
}
