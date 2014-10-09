package profiles

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
)

const (
	OP_POINT_SELECTS       = iota
	OP_POINT_SELECTS_AGG   = iota
	OP_SIMPLE_RANGES       = iota
	OP_SIMPLE_RANGES_AGG   = iota
	OP_SUM_RANGES          = iota
	OP_ORDER_RANGES        = iota
	OP_ORDER_RANGES_AGG    = iota
	OP_DISTINCT_RANGES     = iota
	OP_DISTINCT_RANGES_AGG = iota
	OP_INDEX_UPDATES       = iota
	OP_NON_INDEX_UPDATES   = iota
	OP_REMOVE_AND_INSERT   = iota
	OP_UPDATE_PUSH_FIRST   = iota
	OP_UPDATE_PUSH_LAST    = iota

	OP_INSERT_16M_DOC    = iota
	OP_WRITE_BULK_INSERT = iota
	OP_RANGE_QUERY       = iota

	OP_MAX_OPS = iota
)

var opsADHocNameLookup = map[int]string{
	OP_POINT_SELECTS:       "OP_POINT_SELECTS",
	OP_POINT_SELECTS_AGG:   "OP_POINT_SELECTS_AGG",
	OP_SIMPLE_RANGES:       "OP_SIMPLE_RANGES",
	OP_SIMPLE_RANGES_AGG:   "OP_SIMPLE_RANGES_AGG",
	OP_SUM_RANGES:          "OP_SUM_RANGES",
	OP_ORDER_RANGES:        "OP_ORDER_RANGES",
	OP_ORDER_RANGES_AGG:    "OP_ORDER_RANGES_AGG",
	OP_DISTINCT_RANGES:     "OP_DISTINCT_RANGES",
	OP_DISTINCT_RANGES_AGG: "OP_DISTINCT_RANGES_AGG",
	OP_INDEX_UPDATES:       "OP_INDEX_UPDATES",
	OP_NON_INDEX_UPDATES:   "OP_NON_INDEX_UPDATES",
	OP_REMOVE_AND_INSERT:   "OP_REMOVE_AND_INSERT",
	OP_UPDATE_PUSH_FIRST:   "OP_UPDATE_PUSH_FIRST",
	OP_UPDATE_PUSH_LAST:    "OP_UPDATE_PUSH_LAST",
	OP_INSERT_16M_DOC:      "OP_INSERT_16M_DOC",
	OP_WRITE_BULK_INSERT:   "OP_WRITE_BULK_INSERT",
	OP_RANGE_QUERY:         "OP_RANGE_QUERY",

	OP_MAX_OPS: "OP_MAX_OPS",
}

var OP_Ops [OP_MAX_OPS]operation

type adhocProfile struct {
	UID    int64
	MaxUID int

	session     *mgo.Session
	initProfile sync.Once

	HT_OP_RUN_MORE_TEST int
}

var _adhocProfile adhocProfile

// some OS env
var HT_BATCH_SIZE int
var HT_INSERT_WITH_ID int

//************************
// ops

func insert_doc(c *mgo.Collection, maxid int) {

}

func insert_doc_write_ops(c *mgo.Collection, maxid int) {

}

// end of ops definition
//************************

func (sb adhocProfile) SendNext(s *mgo.Session, worker_id int) error {
	var c *mgo.Collection

	// TODO: this need change!
	c = s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))

	var t1 time.Time
	var err error
	for i := 0; i < OP_MAX_OPS; i++ {
		t1 = time.Now()
		err = nil
		for j := 0; j < OP_Ops[i].NumOps; j++ {
			if err == nil {
				err = OP_Ops[i].f(c, _adhocProfile.MaxUID)
			} else {
				OP_Ops[i].f(c, _adhocProfile.MaxUID)
			}
		}
		if err == nil && int64(OP_Ops[i].NumOps) > 0 {
			OP_Ops[i].stat.RecordResponse(time.Since(t1).Nanoseconds() / int64(OP_Ops[i].NumOps))
		}
	}

	return nil
}

func (i adhocProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		// s.DB(getDBName("sbtest")).DropDatabase()  // never drop DB
		if _adhocProfile.MaxUID == -1 {
			var c *mgo.Collection

			c = s.DB(default_db_name_prefix + "1").C(default_col_name_prefix + "1")

			// to find out how many records we have
			n, err := c.Count()

			if err != nil {
				panic("cannot count collection")
			}
			_adhocProfile.MaxUID = int(n - 5) // adjust, should have logic to find minimal count
			log.Println("Total doc", n)
		}
	}

	_adhocProfile.initProfile.Do(f)

	return nil
}

func (i adhocProfile) CsvString(c float64) string {
	var r string

	for i := 0; i < OP_MAX_OPS; i++ {
		r = r + fmt.Sprintf("%f,%f,", OP_Ops[i].stat.GetThroughput(c), OP_Ops[i].stat.GetAvg(c))
	}

	return r
}

func (i adhocProfile) CsvHeader() string {
	var r string

	for i := 0; i < OP_MAX_OPS; i++ {
		r = r + fmt.Sprintf("%s TPS,%s AVG,", OP_Ops[i].stat.Name, OP_Ops[i].stat.Name)
	}

	return r
}

func init() {
	_adhocProfile.UID = 0
	_adhocProfile.MaxUID = -1
	Run_TOKU_Test = false

	rand.Seed(time.Now().UnixNano())

	OP_Ops[OP_POINT_SELECTS].f = findOneBy_id_project
	OP_Ops[OP_POINT_SELECTS_AGG].f = findOneBy_id_project_with_agg
	OP_Ops[OP_SUM_RANGES].f = aggregation
	OP_Ops[OP_SIMPLE_RANGES].f = rangeQuery_id_project
	OP_Ops[OP_SIMPLE_RANGES_AGG].f = rangeQuery_id_project_agg
	OP_Ops[OP_ORDER_RANGES].f = rangQuery_id_project_sort_c_1
	OP_Ops[OP_ORDER_RANGES_AGG].f = rangQuery_id_project_sort_c_1_agg
	OP_Ops[OP_DISTINCT_RANGES].f = rangeQuery_id_distinct
	OP_Ops[OP_DISTINCT_RANGES_AGG].f = rangeQuery_id_distinct_agg
	OP_Ops[OP_INDEX_UPDATES].f = singleQuery_inc_k
	OP_Ops[OP_NON_INDEX_UPDATES].f = setField_c
	OP_Ops[OP_REMOVE_AND_INSERT].f = removeThenAdd
	OP_Ops[OP_UPDATE_PUSH_FIRST].f = pushFirst
	OP_Ops[OP_UPDATE_PUSH_LAST].f = pushLast

	OP_Ops[OP_INSERT_16M_DOC].f = insert_16M_doc_no_id
	OP_Ops[OP_WRITE_BULK_INSERT].f = write_insert_doc
	OP_Ops[OP_RANGE_QUERY].f = op_query

	for i := 0; i < OP_MAX_OPS; i++ {
		if OP_Ops[i].f == nil {
			log.Panicln("Sysbench Ops ", i, "is not definted")
		} else {
			OP_Ops[i].NumOps = getOSEnvFlagInt(opsADHocNameLookup[i], 0)
			OP_Ops[i].stat.Name = strings.TrimPrefix(runtime.FuncForPC(reflect.ValueOf(OP_Ops[i].f).Pointer()).Name(), "hammer/profiles.")
		}
	}

	registerProfile("ALLOps", func() Profile {
		return Profile(_adhocProfile) // use the same instance
	})
}
