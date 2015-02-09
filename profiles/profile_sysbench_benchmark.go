package profiles

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/rzh/hammer.mongo/stats"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	SYSBENCH_POINT_SELECTS       = iota
	SYSBENCH_POINT_SELECTS_AGG   = iota
	SYSBENCH_SIMPLE_RANGES       = iota
	SYSBENCH_SIMPLE_RANGES_AGG   = iota
	SYSBENCH_SUM_RANGES          = iota
	SYSBENCH_ORDER_RANGES        = iota
	SYSBENCH_ORDER_RANGES_AGG    = iota
	SYSBENCH_DISTINCT_RANGES     = iota
	SYSBENCH_DISTINCT_RANGES_AGG = iota
	SYSBENCH_INDEX_UPDATES       = iota
	SYSBENCH_NON_INDEX_UPDATES   = iota
	SYSBENCH_REMOVE_AND_INSERT   = iota
	SYSBENCH_UPDATE_PUSH_FIRST   = iota
	SYSBENCH_UPDATE_PUSH_LAST    = iota

	SYSBENCH_MAX_OPS = iota
)

var opsNameLookip = map[int]string{
	SYSBENCH_POINT_SELECTS:       "SYSBENCH_POINT_SELECTS",
	SYSBENCH_POINT_SELECTS_AGG:   "SYSBENCH_POINT_SELECTS_AGG",
	SYSBENCH_SIMPLE_RANGES:       "SYSBENCH_SIMPLE_RANGES",
	SYSBENCH_SIMPLE_RANGES_AGG:   "SYSBENCH_SIMPLE_RANGES_AGG",
	SYSBENCH_SUM_RANGES:          "SYSBENCH_SUM_RANGES",
	SYSBENCH_ORDER_RANGES:        "SYSBENCH_ORDER_RANGES",
	SYSBENCH_ORDER_RANGES_AGG:    "SYSBENCH_ORDER_RANGES_AGG",
	SYSBENCH_DISTINCT_RANGES:     "SYSBENCH_DISTINCT_RANGES",
	SYSBENCH_DISTINCT_RANGES_AGG: "SYSBENCH_DISTINCT_RANGES_AGG",
	SYSBENCH_INDEX_UPDATES:       "SYSBENCH_INDEX_UPDATES",
	SYSBENCH_NON_INDEX_UPDATES:   "SYSBENCH_NON_INDEX_UPDATES",
	SYSBENCH_REMOVE_AND_INSERT:   "SYSBENCH_REMOVE_AND_INSERT",
	SYSBENCH_UPDATE_PUSH_FIRST:   "SYSBENCH_UPDATE_PUSH_FIRST",
	SYSBENCH_UPDATE_PUSH_LAST:    "SYSBENCH_UPDATE_PUSH_LAST",

	SYSBENCH_MAX_OPS: "SYSBENCH_MAX_OPS",
}

type operation struct {
	OSEnv  string
	NumOps int
	f      operationFunc
	stat   stats.ProfileStats
}

var SB_Ops [SYSBENCH_MAX_OPS]operation
var Run_TOKU_Test bool

type sbBenchProfile struct {
	UID    int64
	MaxUID int

	session     *mgo.Session
	initProfile sync.Once

	HT_SYSBENCH_RUN_MORE_TEST int
}

var _sbBenchProfile sbBenchProfile

func findOneBy_id_project(c *mgo.Collection, MaxUID int, worker_id int) error {
	var result interface{}

	c.FindId(rands[worker_id].Intn(MaxUID)).Select(bson.M{"c": 1, "_id": 0}).One(&result)

	return nil
}

func findOneBy_id_project_with_agg(c *mgo.Collection, MaxUID int, worker_id int) error {
	var result interface{}

	c.Pipe([]bson.M{
		bson.M{"$match": bson.M{"_id": rands[worker_id].Intn(MaxUID)}},
		bson.M{"$project": bson.M{"c": 1, "_id": 0}},
	}).One(&result)

	return nil
}

func rangeQuery_id_project(c *mgo.Collection, MaxUID int, worker_id int) error {
	var result []interface{}

	start := rands[worker_id].Intn(MaxUID - 200)

	c.Find(bson.M{"_id": bson.M{"$gte": start, "$lte": start + 100}}).Select(bson.M{"c": 1, "_id": 0}).All(&result)

	return nil
}

func rangeQuery_id_project_agg(c *mgo.Collection, MaxUID int, worker_id int) error {
	var result []interface{}

	start := rands[worker_id].Intn(MaxUID - 200)

	c.Pipe([]bson.M{
		bson.M{"$match": bson.M{"_id": bson.M{"$gte": start, "$lte": start + 100}}},
		bson.M{"$project": bson.M{"k": 1, "_id": 0}},
	}).Iter().Next(&result)

	return nil
}

func aggregation(c *mgo.Collection, MaxUID int, worker_id int) error {
	var result interface{}

	// find 100 doc
	start := rands[worker_id].Intn(MaxUID - 200)

	c.Pipe([]bson.M{
		{"$match": bson.M{"_id": bson.M{"$gte": start, "$lte": start + 100}}},
		{"$project": bson.M{"k": 1}},
		{"$group": bson.M{"_id": nil, "average": bson.M{"$sum": "$k"}}},
	}).Iter().Next(&result)

	return nil
}

func rangQuery_id_project_sort_c_1(c *mgo.Collection, MaxUID int, worker_id int) error {
	var result []interface{}

	start := rands[worker_id].Intn(MaxUID - 200)

	q := c.Find(bson.M{"_id": bson.M{"$gte": start, "$lte": start + 100}})
	q.Select(bson.M{"c": 1, "_id": 0})
	q.Sort("c")
	q.All(&result)

	return nil
}

func rangQuery_id_project_sort_c_1_agg(c *mgo.Collection, MaxUID int, worker_id int) error {
	var result []interface{}

	start := rands[worker_id].Intn(MaxUID - 200)

	c.Pipe([]bson.M{
		{"$match": bson.M{"_id": bson.M{"$gte": start, "$lte": start + 100}}},
		{"$project": bson.M{"k": 1, "_id": 0}},
		{"$sort": bson.M{"c": 1}},
	}).Iter().Next(&result)

	return nil
}

func rangeQuery_id_distinct(c *mgo.Collection, MaxUID int, worker_id int) error {
	var result interface{}

	start := rands[worker_id].Intn(MaxUID - 200)

	q := c.Find(bson.M{"_id": bson.M{"$gte": start, "$lte": start + 100}})
	q.Select(bson.M{"c": 1, "_id": 0})
	q.Distinct("c", &result)

	return nil
}

func rangeQuery_id_distinct_agg(c *mgo.Collection, MaxUID int, worker_id int) error {
	var result interface{}

	start := rands[worker_id].Intn(MaxUID - 200)

	c.Pipe([]bson.M{
		{"$match": bson.M{"_id": bson.M{"$gte": start, "$lte": start + 100}}},
		{"$project": bson.M{"k": 1, "_id": 0}},
	}).Iter().Next(&result)

	return nil
}

func singleQuery_inc_k(c *mgo.Collection, MaxUID int, worker_id int) error {
	c.Update(bson.M{"_id": rands[worker_id].Intn(MaxUID)}, bson.M{"$inc": bson.M{"k": 1}})

	return nil
}

func setField_c(c *mgo.Collection, MaxUID int, worker_id int) error {
	// c.Update(bson.M{"_id": rands[worker_id].Intn(MaxUID)}, bson.M{"$set": bson.M{"c": createRandC()}})
	c.Update(bson.M{"_id": 1}, bson.M{"$set": bson.M{"c": createRandC(worker_id)}})

	return nil
}

func removeThenAdd(c *mgo.Collection, MaxUID int, worker_id int) error {
	_u := rands[worker_id].Intn(MaxUID)
	c.Remove(bson.M{"_id": _u})
	c.Insert(bson.M{
		"_id": _u,
		"k":   rands[worker_id].Intn(10000000),
		"c":   createRandC(worker_id),
		"pad": createRandPad(worker_id)})

	return nil
}

func pushFirst(c *mgo.Collection, MaxUID int, worker_id int) error {
	_u := rands[worker_id].Intn(MaxUID)

	// err := c.Update(bson.M{"_id": _u}, bson.M{"$push": bson.M{"array": rands[worker_id].Intn(5000)}})
	err := c.Update(bson.M{"_id": _u}, bson.M{"$push": bson.M{"array": bson.M{"$each": []int{rands[worker_id].Intn(5000), rands[worker_id].Intn(5000)}, "$position": 0}}})
	// panicOnError(err)
	return err
}

func pushLast(c *mgo.Collection, MaxUID int, worker_id int) error {
	_u := rands[worker_id].Intn(MaxUID)

	err := c.Update(bson.M{"_id": _u}, bson.M{"$push": bson.M{"array": rands[worker_id].Intn(5000)}})
	return err
}

// additional traffic
func additional_traffic(c *mgo.Collection, MaxUID int, worker_id int) error {
	_u := rands[worker_id].Intn(MaxUID)
	// c.Update(bson.M{"_id": rands[worker_id].Intn(s.MaxUID)}, bson.M{"$set": bson.M{"c": createRandC()}})

	// mode_Update
	err := c.Update(bson.M{"_id": _u}, bson.M{"$set": bson.M{"group": rands[worker_id].Int()}})
	_ = err

	// mode_AddToSet, cannot do this now
	return nil
}

func (sb sbBenchProfile) SendNext(s *mgo.Session, worker_id int) error {
	var c *mgo.Collection

	// TODO: this need change!
	if Run_TOKU_Test {
		c = s.DB("sbtest").C(getCollectionName("sbtest"))
	} else {
		c = s.DB(getDBName(default_db_name_prefix)).C(getCollectionName(default_col_name_prefix))
	}

	var t1 time.Time
	var err error
	for i := 0; i < SYSBENCH_MAX_OPS; i++ {
		if SB_Ops[i].NumOps > 0 {
			t1 = time.Now()
			err = nil
			for j := 0; j < SB_Ops[i].NumOps; j++ {
				if err == nil {
					err = SB_Ops[i].f(c, _sbBenchProfile.MaxUID, worker_id)
				} else {
					SB_Ops[i].f(c, _sbBenchProfile.MaxUID, worker_id)
				}
			}

			if err == nil && int64(SB_Ops[i].NumOps) > 0 {
				SB_Ops[i].stat.RecordResponse(time.Since(t1).Nanoseconds() / int64(SB_Ops[i].NumOps))
			}

		}
	}

	return nil
}

func (i sbBenchProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	f := func() {
		// s.DB(getDBName("sbtest")).DropDatabase()  // never drop DB
		if _sbBenchProfile.MaxUID == -1 {
			var c *mgo.Collection

			if Run_TOKU_Test {
				c = s.DB("sbtest").C("sbtest1")
			} else {
				c = s.DB(default_db_name_prefix + "1").C(default_col_name_prefix + "1")
			}

			// to find out how many records we have
			n, err := c.Count()

			if err != nil {
				panic("cannot count collection")
			}
			_sbBenchProfile.MaxUID = int(n - 5) // adjust, should have logic to find minimal count
			log.Println("Total doc", n)
		}
	}

	_sbBenchProfile.initProfile.Do(f)

	return nil
}

func (i sbBenchProfile) CsvString(c float64) string {
	var r string

	for i := 0; i < SYSBENCH_MAX_OPS; i++ {
		r = r + fmt.Sprintf("%f,%f,", SB_Ops[i].stat.GetThroughput(c), SB_Ops[i].stat.GetAvg(c))
	}

	return r
}

func (i sbBenchProfile) CsvHeader() string {
	var r string

	for i := 0; i < SYSBENCH_MAX_OPS; i++ {
		r = r + fmt.Sprintf("%s TPS,%s AVG,", SB_Ops[i].stat.Name, SB_Ops[i].stat.Name)
	}

	return r
}

func init() {
	_sbBenchProfile.UID = 0
	_sbBenchProfile.MaxUID = -1
	Run_TOKU_Test = false

	SB_Ops[SYSBENCH_POINT_SELECTS].f = findOneBy_id_project
	SB_Ops[SYSBENCH_POINT_SELECTS_AGG].f = findOneBy_id_project_with_agg
	SB_Ops[SYSBENCH_SUM_RANGES].f = aggregation
	SB_Ops[SYSBENCH_SIMPLE_RANGES].f = rangeQuery_id_project
	SB_Ops[SYSBENCH_SIMPLE_RANGES_AGG].f = rangeQuery_id_project_agg
	SB_Ops[SYSBENCH_ORDER_RANGES].f = rangQuery_id_project_sort_c_1
	SB_Ops[SYSBENCH_ORDER_RANGES_AGG].f = rangQuery_id_project_sort_c_1_agg
	SB_Ops[SYSBENCH_DISTINCT_RANGES].f = rangeQuery_id_distinct
	SB_Ops[SYSBENCH_DISTINCT_RANGES_AGG].f = rangeQuery_id_distinct_agg
	SB_Ops[SYSBENCH_INDEX_UPDATES].f = singleQuery_inc_k
	SB_Ops[SYSBENCH_NON_INDEX_UPDATES].f = setField_c
	SB_Ops[SYSBENCH_REMOVE_AND_INSERT].f = removeThenAdd
	SB_Ops[SYSBENCH_UPDATE_PUSH_FIRST].f = pushFirst
	SB_Ops[SYSBENCH_UPDATE_PUSH_LAST].f = pushLast

	for i := 0; i < SYSBENCH_MAX_OPS; i++ {
		if SB_Ops[i].f == nil {
			log.Panicln("Sysbench Ops ", i, "is not definted")
		} else {
			SB_Ops[i].NumOps = getOSEnvFlagInt(opsNameLookip[i], 0)
			SB_Ops[i].stat.Name = strings.TrimPrefix(runtime.FuncForPC(reflect.ValueOf(SB_Ops[i].f).Pointer()).Name(), "hammer/profiles.")
		}
	}
	// _sbBenchProfile.SYSBENCH_REMOVE_AND_INSERT = getOSEnvFlagInt("HT_SYSBENCH_RUN_MORE_TEST", 0)

	s := os.Getenv("HT_RUN_TOKU_TEST")
	if s != "" {
		Run_TOKU_Test = true
	}

	registerProfile("SB_BENCHRUN", func() Profile {
		return Profile(_sbBenchProfile) // use the same instance
	})
}
