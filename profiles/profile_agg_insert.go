package profiles

import (
	"fmt"
	"sync"
	"sync/atomic"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

type insertAggProfile struct {
	UID    int64
	MaxUID int64

	session     *mgo.Session
	initProfile sync.Once
}

var _insertAggProfile insertAggProfile

// var qa373ArrayPayload [100]int

// func Int2ObjId(i int64) string {
// 	// return string represenation of UID
// }

type AggSmallDoc struct {
	Name          int64
	UID           bson.ObjectId `bson:"_id,omitempty"`
	RandUnindexed int
	Rem100        int
	RandIndexed   int
	Array         [10]int
}

func (i insertAggProfile) SendNext(s *mgo.Session, worker_id int) error {
	c := s.DB(_db_name).C("people")

	_u := atomic.AddInt64(&_insertAggProfile.UID, 1) // to make this unique

	// err := c.Insert(&Person{Name: 100, UID: bson.ObjectIdHex(fmt.Sprintf("%#x", _u)), Group: 100}) // insert a new record
	err := c.Insert(bson.M{
		"_id":            _u,
		"name":           _u,
		"rand_unindexed": rands[worker_id].Intn(100000),
		"rand_indexed":   rands[worker_id].Intn(100000),
		"rem100":         rands[worker_id].Intn(100),
		"array": [10]int{rands[worker_id].Intn(10000),
			rands[worker_id].Intn(10000),
			rands[worker_id].Intn(10000),
			rands[worker_id].Intn(10000),
			rands[worker_id].Intn(10000),
			rands[worker_id].Intn(10000),
			rands[worker_id].Intn(10000),
			rands[worker_id].Intn(10000),
			rands[worker_id].Intn(10000),
			rands[worker_id].Intn(10000)}})

	return err
}

func (i insertAggProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		InitSimpleTest(s, _initdb)

		if _insertAggProfile.MaxUID == -1 {
			c := s.DB(_db_name).C("people")

			// to find out how many records we have
			n, err := c.Count()

			if err != nil {
				panic("cannot count collection")
			}
			_insertAggProfile.MaxUID = int64(n)
			fmt.Println("Total doc", n)
		}
	}

	_insertAggProfile.initProfile.Do(f)

	return nil
}

func (i insertAggProfile) CsvString(total_time float64) string {
	return ""
}

func (i insertAggProfile) CsvHeader() string {
	return ""
}

func init() {
	// fmt.Println("Init INSERT Small profile")

	atomic.StoreInt64(&_insertAggProfile.UID, -1) // UID starts with 1
	atomic.StoreInt64(&_insertAggProfile.MaxUID, -1)

	registerProfile("AGG_INSERT", func() Profile {
		return Profile(_insertAggProfile) // use the same instance
	})

	for i := 0; i < len(qa373ArrayPayload); i++ {
		qa373ArrayPayload[i] = i
	}

	// fmt.Println("Done Init INSERT Small profile")
}
