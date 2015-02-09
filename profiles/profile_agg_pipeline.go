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

type addPipelineProfile struct {
	UID    int64
	MaxUID int64

	session     *mgo.Session
	initProfile sync.Once
}

var _addPipelineProfile addPipelineProfile

func (i addPipelineProfile) SendNext(s *mgo.Session, worker_id int) error {
	c := s.DB(_db_name).C("people")
	result := bson.M{}

	pipe := c.Pipe([]bson.M{
		{"$match": bson.M{"rand_indexed": bson.M{"$gt": rands[worker_id].Int63n(_addPipelineProfile.MaxUID) / 2}}},
		{"$limit": 100},
		{"$group": bson.M{"_id": "$rem100", "array": bson.M{"$first": "$array"}}},
		{"$unwind": "$array"},
		{"$group": bson.M{"_id": 0, "array_all": bson.M{"$addToSet": "$array"}}},
	})
	pipe.Iter().Next(&result)

	return nil
}

func (i addPipelineProfile) SendNextPipeline(s *mgo.Session, op *bson.M) error {
	// c := s.DB(_db_name).C("people")

	// _u := atomic.AddInt64(&_addPipelineProfile.UID, 1) // to make this unique

	return nil
}

func (i addPipelineProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		// InitSimpleTest(s, _initdb)

		if _addPipelineProfile.MaxUID == -1 {
			c := s.DB(_db_name).C("people")

			// to find out how many records we have
			n, err := c.Count()

			if err != nil {
				panic("cannot count collection")
			}
			_addPipelineProfile.MaxUID = int64(n)
			fmt.Println("Total doc", n)
		}
	}

	_addPipelineProfile.initProfile.Do(f)

	return nil
}

func (i addPipelineProfile) CsvString(total_time float64) string {
	return ""
}

func (i addPipelineProfile) CsvHeader() string {
	return ""
}

func init() {
	// fmt.Println("Init INSERT Small profile")

	atomic.StoreInt64(&_addPipelineProfile.UID, -1) // UID starts with 1
	atomic.StoreInt64(&_addPipelineProfile.MaxUID, -1)

	registerProfile("AGG_PIPELINE", func() Profile {
		return Profile(_addPipelineProfile) // use the same instance
	})

	// fmt.Println("Done Init INSERT Small profile")
}
