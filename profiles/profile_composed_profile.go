/*
	ticket: https://jira.mongodb.org/browse/QA-408

	this profile will provide a way to compose different Ops based on relative weight.

	You can configure the weight via a pre-defined configure file if it presents.
	If the config file is not there, it will use the default percentage.

	Ops provided
	  - insert
	  - insert_duplicated_record #will fail
	  - update
	  - update_non_exist_doc  # do nothing
	  - query_single_doc_id
	  - query_single_doc_index_field     # not _id
	  - query_non_exist_doc_id
	  - query_non_exist_doc_index_field  # not _id

	To be provide in later phase
	  - range_query
	  - regex_query
	  - deletion # this need more consideration
	  - update_add_field   # trigger fragmentation
	  ...
*/

package profiles

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rzh/hammer.mongo/stats"

	"code.google.com/p/gcfg"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type composedProfile struct {
	UID int64

	session   *mgo.Session
	numWorker int32

	syncWG      sync.WaitGroup // to sync the init start
	initProfile sync.Once
	// allWorkerDone sync.WaitGroup

	cfg       QA408Config
	threshold QA408Distribution

	staging    int
	queryLimit int
	BatchSize  int

	// insertObj Event // hold object to be inserted

	insertStats           ProfileStats
	findByTagStats        ProfileStats
	findByStreamNameStats ProfileStats
	findByDateRangeStats  ProfileStats
	findByPayloadStats    ProfileStats
	updateStats           ProfileStats
	iteratorStats         ProfileStats
}

type runOps func(c *mgo.Collection) error

var _functions map[string]runOps
var _weights map[string]int
var _configFile string // this is config file, hardcoded for not FIXME

var _composedProfile composedProfile
var _dataBacktrack int64 = 43200 // how long to distribute the doc

/*
{
  tags: [ <tagValues>] // between 1 and 20 <tabValues> per document
  stream: String
  position: Integer
  payload: String // 0 - 16kb, peak 500 bytes
  timestamp: Date
}

<tagValues> := String 1..100 characters in length, 10k possible unique enumerations

ensureIndex( { tags: 1 } )
ensureIndex( {stream:1, position: -1} )
ensureIndex( {timestamp: -1} )
*/

// some bufferred object for better performance
var payloadBuffer [10240]byte // max to 10k
var payloadBuffer_len = 10240 // FIXME: no hard code

var streamNames [1000]string // to hold 100 stream names
var tagNames [10240]string

// some helper function

type Event struct {
	ID        bson.ObjectId `bson:"_id,omitempty"`
	Tags      []string      // between 1 and 20 <tagValues> per document
	Stream    string
	Position  int
	Payload   string // 0 - 16kb, peak 500 bytes
	Timestamp time.Time
}

type ProfileStats struct {
	totalReq int64
	lastReq  int64

	totalResponseTime int64
}

func (p *ProfileStats) GetThroughput(total_time float64) float64 {
	return float64(p.totalReq) / total_time
}

func (p *ProfileStats) GetLastIntervalReq() int64 {
	lastSend := p.totalReq - p.lastReq
	if p.totalReq < p.lastReq {
		log.Fatalln("Error, profile QA409 total send is less than latest send requst number")
	}

	p.lastReq = p.totalReq

	return lastSend
}

func (p *ProfileStats) GetAvg(total_time float64) float64 {
	// log.Println("avg -> ", p.totalResponseTime, p.totalReq)
	return float64(p.totalResponseTime) / (float64(p.totalReq) * 1.0e6) //in ms
}

func (p *ProfileStats) RecordResponse(t int64) {
	// log.Println("record ", t)
	atomic.AddInt64(&p.totalResponseTime, t)
	atomic.AddInt64(&p.totalReq, 1)
}

type QA408Config struct {
	/*
		Find all documents matching a stream name (40%)
		Find all documents with a given tag (40%)
		Find all documents in a date range (10%)
		Add and remove tags from documents (3%)
		Update/replace the payload and date fields (6%)
		Add new documents (1%)
		Find all documents that match a given payload (0.01%) - full collection scan
	*/

	Profile struct {
		Staging          int // number of record to be insert at the beginning of the run
		QueryLimit       int
		FindByStreamName float64
		FindByTag        float64
		FindByDateRange  float64
		UpdateTag        float64
		UpdatePayload    float64
		AddDoc           float64
		FindByPayload    float64
		BatchSize        int
	}
}

type QA408Distribution struct {
	th_FindByStreamName float64
	th_FindByTag        float64
	th_FindByDateRange  float64
	th_UpdateTag        float64
	th_UpdatePayload    float64
	th_AddDoc           float64
	th_FindByPayload    float64
}

/*
	TODO:
	- come optimization can use done, especially use more cache objects
	- avoid return large chunk of data, such as for Payload
*/

func (c *composedProfile) getTags(n int, worker_id int) []string {
	// need to add return of multiple tag

	buffer := make([]string, n)

	for i := 0; i < n; i++ {
		buffer[i] = tagNames[rands[worker_id].Intn(len(tagNames))]
	}

	return buffer

	// return []string{tagNames[rands[worker_id].Intn(len(tagNames))]}
}

func (c *composedProfile) getStream(worker_id int) string {
	return streamNames[rands[worker_id].Intn(len(streamNames))]
}

func (c *composedProfile) getPosition(worker_id int) int {
	return rands[worker_id].Intn(1000) // max position to 1000 since there is no requirement for this
}

func (c *composedProfile) getPayloadSize(worker_id int) int {
	r := int(rands[worker_id].NormFloat64()*200 + 500)

	if r < 0 {
		// log.Println("length is less than 0:", r)
		r = 250 + rands[worker_id].Intn(500)
	} else if r > len(payloadBuffer) {
		r = 250 + rands[worker_id].Intn(500) // FIXME:
	}

	return r // no more than 10k
}

func (c *composedProfile) getTimeStamp(worker_id int) time.Time {
	return time.Now().Add((-1) * time.Duration(rands[worker_id].Int63n(_dataBacktrack)) * time.Minute) // distribute to the last 30 days
}

func randomPayloadBuffer() {
	for i := 0; i < len(payloadBuffer); i++ {
		payloadBuffer[i] = byte(all_chars[rand.Intn(len(all_chars))]) // init payloadBuffer with random chars, ok to use rand
	}
}

func initStreamNames(_worker_id int) {
	for i := 0; i < len(streamNames); i++ {
		streamNames[i] = randomString(20, _worker_id) // FIXME: random lens to be used
	}
}

func initTagNames(_worker_id int) {
	for i := 0; i < len(tagNames); i++ {
		tagNames[i] = randomString(100, _worker_id) // FIXME: random lens to be used
	}
}

// runOps function

/*
Find all documents matching a stream name (40%)
Find all documents with a given tag (40%)
Find all documents in a date range (10%)
Add and remove tags from documents (3%)
Update/replace the payload and date fields (6%)
Add new documents (1%)
Find all documents that match a given payload (0.01%) - full collection scan
*/

func (c *composedProfile) iterateAllDoc(q *mgo.Query, size int, ops string) error {
	// do something here

	result := bson.M{}
	err := q.Explain(result)

	// fmt.Printf("%+v", result)

	return err
}

func (c *composedProfile) addNewDoc(collection *mgo.Collection, _log bool, worker_id int) error {
	// _log : whether this is the "real" run,
	//        reuse this func for staging, will set _log to false
	t := time.Now()

	_in_warmup := stats.IN_WARMUP

	err := collection.Insert(&Event{
		Tags:      c.getTags(rands[worker_id].Intn(15), worker_id),
		Stream:    c.getStream(worker_id),
		Position:  c.getPosition(worker_id), // integ
		Payload:   string(payloadBuffer[0:c.getPayloadSize(worker_id)]),
		Timestamp: c.getTimeStamp(worker_id),
	}) // whether to use a cache object to speed up, not sure which works better. TODO:

	if _log && !_in_warmup {
		d := time.Since(t).Nanoseconds()
		// we will log when this is not in test staging stage
		stats.HammerStats.RecordSend(worker_id)
		stats.HammerStats.RecordRes(uint64(d), "AddDoc", worker_id)
		_composedProfile.insertStats.RecordResponse(d)
		go func() {
			logData("insert one new doc", "insertNewDoc", 1, 1, d)
		}()
	}

	return err
}

func (c *composedProfile) findByStreamName(collection *mgo.Collection, worker_id int) error {
	t := time.Now()
	q := queryMongo(collection, bson.M{"stream": c.getStream(worker_id)}, c.queryLimit, c.BatchSize)
	// q.Iter().Next(&result)
	err := c.iterateAllDoc(q, 0, "findByStreamName")
	d := time.Since(t).Nanoseconds()

	if !stats.IN_WARMUP {
		stats.HammerStats.RecordSend(worker_id)
		stats.HammerStats.RecordRes(uint64(d), "findByStreamName", worker_id)
		_composedProfile.findByStreamNameStats.RecordResponse(d) // collect stats
	}

	return err
}

func (c *composedProfile) findByTag(collection *mgo.Collection, worker_id int) error {
	// result := Event{}
	t := time.Now()
	q := queryMongo(collection, bson.M{"tags": bson.M{"$in": c.getTags(1, worker_id)}}, c.queryLimit, c.BatchSize)
	err := c.iterateAllDoc(q, 0, "findByTag")
	d := time.Since(t).Nanoseconds()

	if !stats.IN_WARMUP {
		stats.HammerStats.RecordSend(worker_id)
		stats.HammerStats.RecordRes(uint64(d), "findByTag", worker_id)
		_composedProfile.findByTagStats.RecordResponse(d)
	}

	return err
}

func (c *composedProfile) findByDateRange(collection *mgo.Collection, worker_id int) error {

	// query start time is at least two days ago, to give enough range to query
	_start_time := time.Now().Add((-1) * time.Duration(rands[worker_id].Int63n(_dataBacktrack-2880)) * time.Minute)

	// query end time is at least 100 min, up to two days
	_end_time := _start_time.Add(time.Duration(rands[worker_id].Int63n(2780)+100) * time.Minute)

	t := time.Now()
	q := queryMongo(collection, bson.M{"timestamp": bson.M{"$gte": _start_time, "$lte": _end_time}}, c.queryLimit, c.BatchSize)
	err := c.iterateAllDoc(q, 0, "findByDateRange")
	d := time.Since(t).Nanoseconds()

	if !stats.IN_WARMUP {
		stats.HammerStats.RecordSend(worker_id)
		stats.HammerStats.RecordRes(uint64(d), "findByDateRange", worker_id)
		_composedProfile.findByDateRangeStats.RecordResponse(d)
	}

	return err
}

func (c *composedProfile) addRemoveTags_with_find_modify(collection *mgo.Collection, worker_id int) error {
	// this will use findAndModify
	doc := Event{}

	tag_have := c.getTags(1, worker_id)
	tag_not_have := c.getTags(1, worker_id)

	change := mgo.Change{
		Update:    c.getTags(rands[worker_id].Intn(15), worker_id),
		ReturnNew: true,
	}

	_, err := collection.Find(bson.M{"tags": bson.M{"$in": tag_have, "$nin": tag_not_have}}).Apply(change, &doc)

	// log.Println("Found tags with ", count)
	return err
}

func (c *composedProfile) addRemoveTags(collection *mgo.Collection, worker_id int) error {
	tag_have := c.getTags(1, worker_id)
	tag_not_have := c.getTags(1, worker_id)

	// t := time.Now()
	q := queryMongo(collection, bson.M{"tags": bson.M{"$in": tag_have, "$nin": tag_not_have}}, c.queryLimit, c.BatchSize)

	result := Event{}
	_in_warmup := stats.IN_WARMUP

	_iter := q.Iter()

	t1 := time.Now().UnixNano()
	var d int64

	for _iter.Next(&result) {
		collection.UpdateId(result.ID,
			bson.M{"$set": bson.M{
				"tags": c.getTags(rands[worker_id].Intn(15), worker_id)}})

		d = time.Now().UnixNano()

		if !_in_warmup {
			_composedProfile.updateStats.RecordResponse(d - t1)
		}
		t1 = d // adjust for the delta used
	}

	return nil
}

func (c *composedProfile) updatePayloadDate(collection *mgo.Collection, worker_id int) error {
	// doc := Event{}

	tag_have := c.getTags(1, worker_id)

	result := Event{}
	_in_warmup := stats.IN_WARMUP

	q := queryMongo(collection, bson.M{"tags": bson.M{"$in": tag_have}}, c.queryLimit, c.BatchSize)

	_iter := q.Iter()

	t1 := time.Now().UnixNano()
	var d int64

	for _iter.Next(&result) {
		collection.UpdateId(result.ID, bson.M{"$set": bson.M{"payload": string(payloadBuffer[0:c.getPayloadSize(worker_id)]), "timestamp": c.getTimeStamp(worker_id)}})
		d = time.Now().UnixNano()
		if !_in_warmup {
			c.updateStats.RecordResponse(d - t1)
		}
		t1 = d // adjust for the delta used
	}

	return nil
}

func (c *composedProfile) findByPayload(collection *mgo.Collection, worker_id int) error {
	t := time.Now()
	q := queryMongo(collection, bson.M{"payload": string(payloadBuffer[0:c.getPayloadSize(worker_id)])}, c.queryLimit, c.BatchSize)
	c.iterateAllDoc(q, 0, "findByPayload")
	d := time.Since(t).Nanoseconds()

	if !stats.IN_WARMUP {
		_composedProfile.findByPayloadStats.RecordResponse(d)
	}

	return nil
}

// end of runOps function

func (pp composedProfile) SendNext(s *mgo.Session, worker_id int) error {
	collection := s.DB("test1").C("QA408")

	c := _composedProfile
	// _u := atomic.AddInt64(&_composedProfile.UID, 1) // to make this unique

	var err error

	r := float64(rands[worker_id].Intn(10000)) / 100.0

	switch {
	case r < c.threshold.th_FindByStreamName:
		err = c.findByStreamName(collection, worker_id)
		// log.Println("Woot 1")
	case r < c.threshold.th_FindByTag:
		err = c.findByTag(collection, worker_id)
		// log.Println("Woot 2")
	case r < c.threshold.th_FindByDateRange:
		err = c.findByDateRange(collection, worker_id)
		// log.Println("Woot 3")
	case r < c.threshold.th_UpdateTag:
		err = c.addRemoveTags(collection, worker_id)
		// log.Println("Woot 4")
	case r < c.threshold.th_UpdatePayload:
		err = c.updatePayloadDate(collection, worker_id)
		// log.Println("Woot 5")
	case r < c.threshold.th_AddDoc:
		err = c.addNewDoc(collection, true, worker_id)
		// log.Println("Woot 6")
	case r < c.threshold.th_FindByPayload:
		err = c.findByPayload(collection, worker_id)
	default:
		log.Println("hmm.. got ", r)
		// log.Println("Woot 7")
	}

	/*
		464405	39.93%
		464960	39.98%
		116440	10.01%
		35134	3.02%
		70180	6.03%
		11777	1.01%
		99	    0.01%

		Total : 1162995
	*/

	return err
}

func (c composedProfile) SetupTest(s *mgo.Session, _initdb bool) error {
	n := atomic.AddInt32(&_composedProfile.numWorker, 1) // global
	collection := s.DB("test1").C("QA408")

	_composedProfile.initProfile.Do(_composedProfile.readConfigFile)
	// c.allWorkerDone.Add(1)

	// log.Println("n is ", c.numWorker)
	if n == 1 {
		// I am the first one, do some init work here, and the let all other worker go
		_composedProfile.session = s //saved session is useless... FIXME: since this is singletoen
		c.session = s

		// doing something to the DB here

		if _initdb {
			fmt.Println("Init DB, drop collections for QA408")
			collection.DropCollection()
		}

		err := collection.EnsureIndexKey("tags")
		if err != nil {
			panic(err)
		}

		err = collection.EnsureIndexKey("stream", "-position")
		if err != nil {
			panic(err)
		}

		err = collection.EnsureIndexKey("-timestamp")
		if err != nil {
			panic(err)
		}

		// s.SetBatch(c.BatchSize) // set batchsize

		_u := atomic.AddInt64(&_composedProfile.UID, 1)

		// now I am done as the lucky first workers, wake all
		_composedProfile.syncWG.Done()

		// log.Println("Staging ", c.staging, " doc")
		// staging some data
		for int(_u) < c.staging {
			c.addNewDoc(collection, false, 1) // use a temp worker id, since we do not care about this
			// if math.Mod(float64(_u), 100) < 1 {
			// 	fmt.Print(".")
			// } // some visual feedback
			_u = atomic.AddInt64(&_composedProfile.UID, 1) // to make this unique

		}

	} else {
		// have to wait
		_composedProfile.syncWG.Wait() // just wait for the first worker done

		_u := atomic.AddInt64(&_composedProfile.UID, 1)
		// staging some data
		for int(_u) < c.staging {
			c.addNewDoc(collection, false, 1)              // use a temp worker id, since we do not care about this
			_u = atomic.AddInt64(&_composedProfile.UID, 1) // to make this unique
		}
	}

	// fmt.Println("c.staging is ", c.staging)
	// c.allWorkerDone.Done()
	// c.allWorkerDone.Wait()
	return nil
}

func (i composedProfile) CsvString(total_time float64) string {
	return fmt.Sprintf("%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f",
		_composedProfile.insertStats.GetThroughput(total_time),
		_composedProfile.insertStats.GetAvg(total_time),

		_composedProfile.findByTagStats.GetThroughput(total_time),
		_composedProfile.findByTagStats.GetAvg(total_time),

		_composedProfile.findByStreamNameStats.GetThroughput(total_time),
		_composedProfile.findByStreamNameStats.GetAvg(total_time),

		_composedProfile.findByDateRangeStats.GetThroughput(total_time),
		_composedProfile.findByDateRangeStats.GetAvg(total_time),

		_composedProfile.findByPayloadStats.GetThroughput(total_time),
		_composedProfile.findByPayloadStats.GetAvg(total_time),

		_composedProfile.updateStats.GetThroughput(total_time),
		_composedProfile.updateStats.GetAvg(total_time),

		_composedProfile.iteratorStats.GetThroughput(total_time),
		_composedProfile.iteratorStats.GetAvg(total_time))

}

func (c composedProfile) CsvHeader() string {
	/*
		Find all documents matching a stream name (40%)
		Find all documents with a given tag (40%)
		Find all documents in a date range (10%)
		Add and remove tags from documents (3%)
		Update/replace the payload and date fields (6%)
		Add new documents (1%)
		Find all documents that match a given payload (0.01%) - full collection scan

		insertStats           ProfileStats
		findByTagStats        ProfileStats
		findByStreamNameStats ProfileStats
		findByDateRange       ProfileStats
		findByPayload         ProfileStats
		updateStats           ProfileStats
		iteratorStats         ProfileStats

	*/

	return "" +
		"QA408 Add Doc Throughput,Add Doc reponse time," +
		"findByTag Query Throughput,findByTag avg time," +
		"findByStreamName Query Throughput,findByStreamName avg time," +
		"findByDateRange Query Throughput,findByDateRange avg time," +
		"findByPayload Query Throughput,findByPayload avg time," +
		"update doc Throughput,update doc avg time," +
		"iterator throughput,iterator avg time"
}

func (c *composedProfile) readConfigFile() {

	err := gcfg.ReadFileInto(&c.cfg, "qa408.conf")
	if err != nil {
		fmt.Printf("\nFailed to read qa408.conf: %s\nPlease make sure you have qa408.conf at current directory, you can copy from qa408.conf.template\n\n", err)
		os.Exit(1)
	}
	initStreamNames(0)
	initTagNames(0)

	// fmt.Printf("%f\n %f\n", c.cfg.Profile.AddDoc, c.cfg.Profile.AddDoc)

	// now apply this to
	c.updateProfile()
}

func (c *composedProfile) updateProfile() {

	// update threshold based on cfg
	c.threshold.th_FindByStreamName = c.cfg.Profile.FindByStreamName
	c.threshold.th_FindByTag = c.cfg.Profile.FindByTag + c.threshold.th_FindByStreamName
	c.threshold.th_FindByDateRange = c.cfg.Profile.FindByDateRange + c.threshold.th_FindByTag
	c.threshold.th_UpdateTag = c.cfg.Profile.UpdateTag + c.threshold.th_FindByDateRange
	c.threshold.th_UpdatePayload = c.cfg.Profile.UpdatePayload + c.threshold.th_UpdateTag
	c.threshold.th_AddDoc = c.cfg.Profile.AddDoc + c.threshold.th_UpdatePayload
	c.threshold.th_FindByPayload = c.cfg.Profile.FindByPayload + c.threshold.th_AddDoc

	c.staging = c.cfg.Profile.Staging
	c.queryLimit = c.cfg.Profile.QueryLimit
	c.BatchSize = c.cfg.Profile.BatchSize

	if !(math.Abs(100.0-c.threshold.th_FindByPayload) < 0.005) {
		log.Fatalln("QA408 Total Distribution is not 100%, received  ", c.threshold.th_FindByPayload)
	}
}

func init() {
	// fmt.Println("Init QA408 profile")

	var err error
	_logFile, err = os.Create("testlogfile")
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	_logFile.WriteString(fmt.Sprintf("%s, %s, %s, %s\n", "ops", "query.Count", "counted by iteration", "time use (ms)"))

	// defer _logFile.Close()

	atomic.StoreInt64(&_composedProfile.UID, -1) // UID starts with 1
	atomic.StoreInt32(&_composedProfile.numWorker, 0)

	randomPayloadBuffer()

	_composedProfile.syncWG.Add(1) // to make this for the first worker

	// register a second name for easier access
	registerProfile("QA408", func() Profile {
		return _composedProfile // use the same instance
	})

	// fmt.Println("Done Init Composed profile")
}
