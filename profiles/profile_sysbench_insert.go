package profiles

import (
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"

	wrapper "github.com/rzh/hammer.mongo/mgowrapper"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type sbInsertProfile struct {
	UID int64

	session     *mgo.Session
	initProfile sync.Once
}

var _sbInsertProfile sbInsertProfile

const _sb_batch_insert_size = 1000
const _sb_array_size = 120

type sbDoc struct {
	id  int // 1 … 10000000
	k   int // random integer between 1 and 10000000
	c   int // 10 segments of 11 random digits plus hyphen (119 characters total)
	pad int // 5 segments of 11 random digits plus hyphen  (59 characters total)
}

// to create random payload
const numeric_chars = "0123456789"

var sb_array_buffer [_sb_array_size]int

func createRandPad(worker_id int) string {
	// format "###########-###########-###########-###########-###########"

	x := make([]byte, 59)

	for i := 0; i < 59; i++ {
		x[i] = byte(numeric_chars[rands[worker_id].Intn(10)])
	}

	x[11] = '-'
	x[23] = '-'
	x[34] = '-'
	x[45] = '-'

	return string(x)
}

func createRandC(worker_id int) string {
	// format "###########-###########-###########-###########-###########-###########-###########-###########-###########-###########"

	x := make([]byte, 119)

	for i := 0; i < 119; i++ {
		x[i] = byte(numeric_chars[rands[worker_id].Intn(10)])
	}

	x[11] = '-'
	x[23] = '-'
	x[35] = '-'
	x[46] = '-'
	x[58] = '-'
	x[70] = '-'
	x[82] = '-'
	x[94] = '-'
	x[106] = '-'

	return string(x)
}

func sb_insert_doc(c *mgo.Collection, docs *[]bson.M) {
	err := c.Insert(docs)
	panicOnError(err)
}

func (i sbInsertProfile) SendNext(s *mgo.Session, worker_id int) error {
	var _u int64

	// docs := make([]bson.M, _sb_batch_insert_size)
	docs := make([]bson.M, _sb_batch_insert_size)

	_u = atomic.AddInt64(&_sbInsertProfile.UID, _sb_batch_insert_size) // to make this unique, and grab 1000 ids
	for i := 0; i < _sb_batch_insert_size; i++ {
		docs[i] = bson.M{
			"_id":   (_u - _sb_batch_insert_size + int64(i)),
			"k":     rands[worker_id].Intn(10000000),
			"c":     createRandC(worker_id),
			"pad":   createRandPad(worker_id),
			"array": sb_array_buffer}
	}

	// we shall insert for all the DB & collections
	for i := 1; i <= _multi_db; i++ {
		for j := 1; j <= _multi_col; j++ {
			//err := s.DB(default_db_name_prefix + strconv.Itoa(i)).C(default_col_name_prefix + strconv.Itoa(j)).Insert(docs...)
			err := wrapper.Insert(s.DB(default_db_name_prefix+strconv.Itoa(i)).C(default_col_name_prefix+strconv.Itoa(j)), docs)
			panicOnError(err)
		}
	}

	return nil
}

func (i sbInsertProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		for i := 1; i <= _multi_db; i++ {
			s.DB(default_db_name_prefix + strconv.Itoa(i)).DropDatabase()
			log.Println("Drop DB ", default_db_name_prefix, i)
		}

		// ensure index
		for i := 1; i <= _multi_db; i++ {
			for j := 1; j <= _multi_col; j++ {
				c := s.DB(default_db_name_prefix + strconv.Itoa(i)).C(default_col_name_prefix + strconv.Itoa(j))
				c.EnsureIndexKey("k")
			}
		}
	}

	_sbInsertProfile.initProfile.Do(f)

	return nil
}

func (i sbInsertProfile) CsvString(total_time float64) string {
	return ""
}

func (i sbInsertProfile) CsvHeader() string {
	return ""
}

func init() {
	_sbInsertProfile.UID = 0

	// init array buffer
	for i := 0; i < _sb_array_size; i++ {
		sb_array_buffer[i] = rand.Int() // init, ok to use common rand
	}

	registerProfile("SB_INSERT", func() Profile {
		return Profile(_sbInsertProfile) // use the same instance
	})
}
