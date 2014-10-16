package profiles

import (
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	// "math/rand"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

// var Payload [40]int
// var Payload1 [120]byte
// var Payload2 [120]byte
// var Payload3 [120]byte
// var Payload4 [120]byte
// var Payload5 [120]byte
// var Payload6 [120]byte

type insertSmallProfile struct {
	UID    int64
	MaxUID int64

	indexGroup bool

	session     *mgo.Session
	initProfile sync.Once
}

var _insertSmallProfile insertSmallProfile
var qa373ArrayPayload [25]int
var has_id bool = true

// func Int2ObjId(i int64) string {
// 	// return string represenation of UID
// }

type SmallDoc struct {
	Name    int64
	UID     bson.ObjectId `bson:"_id,omitempty"`
	Group   int
	Payload [25]int
}

func (i insertSmallProfile) SendNext(s *mgo.Session, worker_id int) error {
	c := s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))
	var err error
	var results interface{}

	_u := atomic.AddInt64(&_insertSmallProfile.UID, 1) // to make this unique
	// var _u int64 = 0

	// return c.Insert(bson.M{"_id": _u})
        err = c.Database.Run(bson.D{{"insert",    c.Name},
                		    {"documents", []bson.M{bson.M{"_id": _u} }}}, results)

	return err

	// err = c.Insert(&Person{Name: 100, UID: bson.ObjectIdHex(fmt.Sprintf("%#x", _u)), Group: 100}) // insert a new record
	err = c.Insert(bson.M{"name": 100, "uid": "string", "group": 100}) // insert a new record
	/*
		if has_id {
			err = c.Insert(bson.M{
				//"_id":     _u + _insertSmallProfile.MaxUID,
				"f1":  "12345678",
				"f2":  "12345678",
				"f3":  "12345678",
				"f4":  "12345678",
				"f5":  "12345678",
				"f6":  "12345678",
				"f7":  "12345678",
				"f8":  "12345678",
				"f9":  "12345678",
				"f10": "12345678",
				"f11": "12345678",
				"f12": "12345678",
				"f13": "12345678",
				"f14": "12345678",
				"f15": "12345678",
				"f16": "12345678",
				"f17": "12345678",
				"f18": "12345678",
				"f19": "12345678",
				"f20": "12345678",
				"i1":  "12345678",
				"i2":  "12345678",
				"i3":  "12345678",
				"i4":  "12345678",
				"i5":  "12345678",
				"i6":  "12345678",
				"i7":  "12345678",
				"i8":  "12345678",
				"i9":  "12345678",
				"i10": "12345678",
				"i11": "12345678",
				"i12": "12345678",
				"i13": "12345678",
				"i14": "12345678",
				"i15": "12345678",
				"i16": "12345678",
				"i17": "12345678",
				"i18": "12345678",
				"i19": "12345678",
				"i20": "12345678",
				"t1":  "12345678",
				"t2":  "12345678",
				"t3":  "12345678",
				"t4":  "12345678",
				"t5":  "12345678",
				"t6":  "12345678",
				"t7":  "12345678",
				"t8":  "12345678",
				"t9":  "12345678",
				"t10": "12345678",
				"t11": "12345678",
				"t12": "12345678",
				"t13": "12345678",
				"t14": "12345678",
				"t15": "12345678",
				"t16": "12345678",
				"t17": "12345678",
				"t18": "12345678",
				"t19": "12345678",
				"t20": "12345678",
				//"payload": &qa373ArrayPayload})
				"_id":  bson.NewObjectId(),
				"name": _u + _insertSmallProfile.MaxUID})
			//"group":   rand.Intn(1000),
			//"payload": &qa373ArrayPayload})
		} else {
			err = c.Insert(bson.M{
				"xid":  bson.NewObjectId(),
				"f1":   "12345678",
				"f2":   "12345678",
				"f3":   "12345678",
				"f4":   "12345678",
				"f5":   "12345678",
				"f6":   "12345678",
				"f7":   "12345678",
				"f8":   "12345678",
				"f9":   "12345678",
				"f10":  "12345678",
				"f11":  "12345678",
				"f12":  "12345678",
				"f13":  "12345678",
				"f14":  "12345678",
				"f15":  "12345678",
				"f16":  "12345678",
				"f17":  "12345678",
				"f18":  "12345678",
				"f19":  "12345678",
				"f20":  "12345678",
				"i1":   "12345678",
				"i2":   "12345678",
				"i3":   "12345678",
				"i4":   "12345678",
				"i5":   "12345678",
				"i6":   "12345678",
				"i7":   "12345678",
				"i8":   "12345678",
				"i9":   "12345678",
				"i10":  "12345678",
				"i11":  "12345678",
				"i12":  "12345678",
				"i13":  "12345678",
				"i14":  "12345678",
				"i15":  "12345678",
				"i16":  "12345678",
				"i17":  "12345678",
				"i18":  "12345678",
				"i19":  "12345678",
				"i20":  "12345678",
				"t1":   "12345678",
				"t2":   "12345678",
				"t3":   "12345678",
				"t4":   "12345678",
				"t5":   "12345678",
				"t6":   "12345678",
				"t7":   "12345678",
				"t8":   "12345678",
				"t9":   "12345678",
				"t10":  "12345678",
				"t11":  "12345678",
				"t12":  "12345678",
				"t13":  "12345678",
				"t14":  "12345678",
				"t15":  "12345678",
				"t16":  "12345678",
				"t17":  "12345678",
				"t18":  "12345678",
				"t19":  "12345678",
				"t20":  "12345678",
				"name": _u + _insertSmallProfile.MaxUID})
			//"group":   rand.Intn(1000),
			//"payload": &qa373ArrayPayload})
		}
		// "payload1": &Payload1,
		// "payload2": &Payload2,
		// "payload3": &Payload3,
		// "payload4": &Payload4,
		// "payload5": &Payload5,
		// "payload6": &Payload6
	*/
	return err
}

func (i insertSmallProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	// // follow should be moved into SetupTest
	// collection := i.session.DB("test1").C("people")

	// err := collection.EnsureIndexKey("name")
	// if err != nil {
	// 	panic(err)
	// }

	// err = collection.EnsureIndexKey("uid")
	// if err != nil {
	// 	panic(err)
	// }

	f := func() {
		_initdb = false
		// InitSimpleTest(s, _initdb)

		if _initdb {
			_insertSmallProfile.MaxUID = 0
			for i := 1; i < _multi_db; i++ {
				for j := 1; j < _multi_col; j++ {
					_ = s.DB(default_db_name_prefix + strconv.Itoa(i)).C(default_col_name_prefix + strconv.Itoa(j))
					if _insertSmallProfile.indexGroup {
						// c.EnsureIndexKey("group")
					}

				}
			}

		} else if _insertSmallProfile.MaxUID == -1 {
			// c := s.DB(_db_name).C("people")
			c := s.DB(default_db_name_prefix + "1").C(default_col_name_prefix + "1")

			// to find out how many records we have
			n, err := c.Count()

			if err != nil {
				panic("cannot count collection")
			}

			for i := 1; i < _multi_db; i++ {
				for j := 1; j < _multi_col; j++ {
					if _insertSmallProfile.indexGroup {
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
	// fmt.Println("Init INSERT Small profile")

	atomic.StoreInt64(&_insertSmallProfile.UID, -1) // UID starts with 1
	atomic.StoreInt64(&_insertSmallProfile.MaxUID, -1)

	registerProfile("INSERTSMALL", func() Profile {
		return Profile(_insertSmallProfile) // use the same instance
	})

	for i := 0; i < len(qa373ArrayPayload); i++ {
		qa373ArrayPayload[i] = i
	}

	s := os.Getenv("HT_INDEX_GROUP")
	if s == "" {
		_insertSmallProfile.indexGroup = false
	} else {
		_insertSmallProfile.indexGroup = true
	}

	s = os.Getenv("HT_INSERT_NO_ID")
	if s != "" {
		log.Println("HT: send without _id")
		has_id = false
	} else {
		log.Println("HT: send with _id")
		has_id = true
	}
}
