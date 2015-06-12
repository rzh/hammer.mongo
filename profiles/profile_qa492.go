/*
	ticket: https://jira.mongodb.org/browse/QA-492
*/

package profiles

import (
	"encoding/binary"
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

const (
	uniformDistribution int = 1
	normalDistribution  int = 2
)

type qa492Profile struct {
	UID      int64 // unique user ID
	docCount int64 // to count how many doc in the DB, update every 5 min

	session   *mgo.Session
	numWorker int32

	syncWG      sync.WaitGroup // to sync the init start
	initProfile sync.Once

	cfg       QA492Config
	threshold QA492Distribution

	staging    int
	queryLimit int
	BatchSize  int

	// insertObj QA492_Event // hold object to be inserted

	insertStats           stats.ProfileStats
	findByTagStats        stats.ProfileStats
	findByStreamNameStats stats.ProfileStats
	findByDateRangeStats  stats.ProfileStats
	findByPayloadStats    stats.ProfileStats
	updateStats           stats.ProfileStats
	iteratorStats         stats.ProfileStats

	// qa492 new
	findBy_stream_and_user_stats     stats.ProfileStats
	findBy_tag_and_user_stats        stats.ProfileStats
	findBy_date_range_and_user_stats stats.ProfileStats

	// some config
	dateBacktrack int64

	// cached staging data
	payloadBuffer [16384]byte
	streamNames   []string
	tagNames      []string

	streamNames_len   int64
	tagNames_len      int64
	payloadBuffer_len int

	distribution             int
	normalDistributionStdDev float64
}

var _qa492Profile qa492Profile

/*
{
  user: integra, random from within a range
  tags: [ <tagValues>] // between 1 and 20 <tabValues> per document
  stream: String
  position: Integer
  payload: String // 0 - 16kb, peak 500 bytes
  timestamp: Date
}

<tagValues> := String 1..100 characters in length, 10k possible unique enumerations

ensureIndex( {user:1, stream:1}, {unique: true} )
ensureIndex( {user:1} )
ensureIndex( {tags: 1} )
ensureIndex( {stream:1, position: -1} )
ensureIndex( {timestamp: -1} )
*/

type QA492_Event struct {
	ID        bson.ObjectId `bson:"_id,omitempty"`
	User      int64         `bson:"user"` // user name, could be duplicate
	Tags      []string      `bson:"tags"` // between 1 and 20 <tagValues> per document
	Stream    string        `bson:"stream"`
	Position  int           `bson:"position"`
	Payload   string        `bson:"payload"` // 0 - 16kb, peak 500 bytes
	Timestamp time.Time     `bson:"timestamp"`
	// UID       int64         `bson:"uid"`  // unique ID

}

type QA492Config struct {
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
		BatchSize        int
		UIDRange         int
		DateRange        int
		FindByStreamName float64
		FindByTag        float64
		FindByDateRange  float64
		UpdateTag        float64
		UpdatePayload    float64
		AddDoc           float64
		FindByPayload    float64

		// qa492 new
		FindByStreamAndUser    float64
		FindByTagAndUser       float64
		FindByDateEangeAndUser float64

		FindByStreamAndUserHint    bool
		FindByTagAndUserHint       bool
		FindByDateEangeAndUserHint bool

		NumOfStreams int64
		NumOfTags    int64

		Distribution      string
		NormalStdDevRatio float64
	}
}

type QA492Distribution struct {
	th_FindByStreamName float64
	th_FindByTag        float64
	th_FindByDateRange  float64
	th_UpdateTag        float64
	th_UpdatePayload    float64
	th_AddDoc           float64
	th_FindByPayload    float64

	// qa492 new
	th_find_by_stream_and_user     float64
	th_find_by_tag_and_user        float64
	th_find_by_date_range_and_user float64
}

/*
	TODO:
	- come optimization can use done, especially use more cache objects
	- avoid return large chunk of data, such as for Payload
*/

func (c *qa492Profile) normalInRange(drange int64, worker_id int) int64 {
	i := int64(-1)

	for i < 0 || i >= drange {
		i = int64((rands[worker_id].NormFloat64()*_qa492Profile.normalDistributionStdDev + 0.5) * float64(drange))
	}
	return i
}

func (c *qa492Profile) getTags(n int, worker_id int) []string {
	buffer := make([]string, n)

	for i := 0; i < n; i++ {
		buffer[i] = _qa492Profile.tagNames[rands[worker_id].Intn(len(_qa492Profile.tagNames))]
	}

	return buffer
}

func (c *qa492Profile) getStream(worker_id int) string {
	if _qa492Profile.distribution == uniformDistribution {
		return _qa492Profile.streamNames[rands[worker_id].Intn(len(c.streamNames))]
	} else if _qa492Profile.distribution == normalDistribution {
		return _qa492Profile.streamNames[c.normalInRange(int64(_qa492Profile.streamNames_len), worker_id)]
	} else {
		log.Panicln("unknown random distribution ", c.distribution)
	}
	return "" // shall never be here
}

func (c *qa492Profile) getPosition(worker_id int) int {
	return rands[worker_id].Intn(1000) // max position to 1000 since there is no requirement for this
}

func (c *qa492Profile) getPayloadSize(worker_id int) int {
	r := int(rands[worker_id].NormFloat64()*200 + 500)

	if r < 0 {
		r = 250 + rands[worker_id].Intn(500)
	} else if r > len(c.payloadBuffer) {
		r = 250 + rands[worker_id].Intn(500) // FIXME:
	}

	return r // no more than 10k
}

func (c *qa492Profile) getTimeStamp(worker_id int) time.Time {
	return time.Now().Add((-1) * time.Duration(rands[worker_id].Int63n(_qa492Profile.dateBacktrack)) * time.Minute) // distribute to the last 30 days
}

func (c *qa492Profile) getUser(worker_id int) int64 {

	if _qa492Profile.distribution == uniformDistribution {
		return rands[worker_id].Int63n(int64(c.cfg.Profile.UIDRange))
	} else if _qa492Profile.distribution == normalDistribution {
		return c.normalInRange(int64(c.cfg.Profile.UIDRange), worker_id)
	} else {
		log.Panicln("unknown random distribution ", c.distribution)
	}
	return -1 // shall never be here
}

func (c *qa492Profile) getUID() bson.ObjectId {
	var b [12]byte

	binary.BigEndian.PutUint64(b[:], uint64(atomic.AddInt64(&_qa492Profile.UID, 1)))
	return bson.ObjectId(b[:])
}

func (c *qa492Profile) randomPayloadBuffer() {
	for i := 0; i < len(c.payloadBuffer); i++ {
		_qa492Profile.payloadBuffer[i] = byte(all_chars[rand.Intn(len(all_chars))]) // Ok to use command rand
	}
}

func (c *qa492Profile) initStreamNames() {
	fmt.Println("init stream name to size ", len(c.streamNames))
	for i := 0; i < len(c.streamNames); i++ {
		_qa492Profile.streamNames[i] = randomWord(20) // no requiremend, do we need change this?
	}
}

func (c *qa492Profile) initTagNames() {
	for i := 0; i < len(c.tagNames); i++ {
		_qa492Profile.tagNames[i] = randomString(1+rand.Intn(99), 0) // from 1 to 100, Ok to use command rand
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
func queryMongoIter(collection *mgo.Collection, query interface{}, queryLimit int, batchSize int) (*mgo.Query, *mgo.Iter) {
	var q *mgo.Query

	if queryLimit > 0 && batchSize > 0 {
		q = collection.Find(query).Batch(batchSize).Limit(queryLimit)
	} else if queryLimit > 0 {
		q = collection.Find(query).Limit(queryLimit)
	} else {
		q = collection.Find(query) // unlimited
	}

	return q, q.Iter()
}

func (c *qa492Profile) iterateAllDoc(q *mgo.Query, size int, ops string) error {
	// do something here

	result := bson.M{}
	err := q.Explain(result)

	// fmt.Printf("%+v", result)

	return err
}

func (pp *qa492Profile) addNewDoc(collection *mgo.Collection, _log bool, worker_id int) error {
	// _log : whether this is the "real" run,
	//        reuse this func for staging, will set _log to false
	t := time.Now()

	c := &_qa492Profile

	_in_warmup := stats.IN_WARMUP

	payload_size := _qa492Profile.getPayloadSize(worker_id)
	payload_start := rands[worker_id].Intn(_qa492Profile.payloadBuffer_len - payload_size - 1)

	err := collection.Insert(&QA492_Event{
		// ID:        c.getUID(),
		User:      c.getUser(worker_id),
		Tags:      c.getTags(rands[worker_id].Intn(15), worker_id),
		Stream:    c.getStream(worker_id),
		Position:  c.getPosition(worker_id), // integ
		Payload:   string(c.payloadBuffer[payload_start : payload_start+payload_size]),
		Timestamp: c.getTimeStamp(worker_id),
	}) // whether to use a cache object to speed up, not sure which works better. TODO:

	if _log && !_in_warmup {
		d := time.Since(t).Nanoseconds()
		// we will log when this is not in test staging stage
		stats.HammerStats.RecordSend(worker_id)
		stats.HammerStats.RecordRes(uint64(d), "AddDoc", worker_id)
		_qa492Profile.insertStats.RecordResponse(d)
		go func() {
			logData("insert one new doc", "insertNewDoc", 1, 1, d)
		}()
	}

	return err
}

// util func

func (pp *qa492Profile) findByStreamName(collection *mgo.Collection, worker_id int) error {
	c := &_qa492Profile

	t := time.Now()
	q := queryMongo(collection, bson.M{"stream": c.getStream(worker_id)}, c.queryLimit, c.BatchSize)
	err := c.iterateAllDoc(q, 0, "findByStreamName")
	d := time.Since(t).Nanoseconds()

	if !stats.IN_WARMUP {
		stats.HammerStats.RecordSend(worker_id)
		stats.HammerStats.RecordRes(uint64(d), "findByStreamName", worker_id)
		_qa492Profile.findByStreamNameStats.RecordResponse(d) // collect stats
	}

	q.Count()

	return err
}

// qa-492 new
func (pp *qa492Profile) findBy_stream_and_user(collection *mgo.Collection, worker_id int) error {
	var q *mgo.Query
	c := &_qa492Profile

	result := bson.M{}
	s1 := c.getStream(worker_id)
	u1 := c.getUser(worker_id)

	t := time.Now()
	if c.cfg.Profile.FindByStreamAndUserHint {
		// with hint
		q = queryMongo(collection, bson.M{"stream": s1, "user": u1}, c.queryLimit, c.BatchSize).Hint("user", "stream")
	} else {
		// no hint
		q = queryMongo(collection, bson.M{"stream": s1, "user": u1}, c.queryLimit, c.BatchSize)
	}
	err := q.Explain(result)
	q.Count()

	d := time.Since(t).Nanoseconds()

	if !stats.IN_WARMUP {
		stats.HammerStats.RecordSend(worker_id)
		stats.HammerStats.RecordRes(uint64(d), "findBy_stream_and_user", worker_id)
		_qa492Profile.findBy_stream_and_user_stats.RecordResponse(d) // collect stats
	}

	// if count == 0 {
	// 	// fmt.Println("not found with stream ", s1, " and user ", u1)

	// 	// return nil // no need to iterator
	// }

	// c.iterateAllDoc(it, count, "findBy_stream_and_user")

	return err
}

func (pp *qa492Profile) findByTag(collection *mgo.Collection, worker_id int) error {
	c := &_qa492Profile

	t := time.Now()
	q := queryMongo(collection, bson.M{"tags": bson.M{"$in": c.getTags(1, worker_id)}}, c.queryLimit, c.BatchSize)
	err := c.iterateAllDoc(q, 0, "findByTag")
	d := time.Since(t).Nanoseconds()

	if !stats.IN_WARMUP {
		stats.HammerStats.RecordSend(worker_id)
		stats.HammerStats.RecordRes(uint64(d), "findByTag", worker_id)
		c.findByTagStats.RecordResponse(d)
	}

	return err
}

func (pp *qa492Profile) findBy_tag_and_user(collection *mgo.Collection, worker_id int) error {
	var q *mgo.Query
	c := &_qa492Profile

	result := bson.M{}

	t := time.Now()
	if c.cfg.Profile.FindByTagAndUserHint {
		q = queryMongo(collection, bson.M{"tags": bson.M{"$in": c.getTags(1, worker_id)}, "user": c.getUser(worker_id)}, c.queryLimit, c.BatchSize).Hint("tags")
	} else {
		q = queryMongo(collection, bson.M{"tags": bson.M{"$in": c.getTags(1, worker_id)}, "user": c.getUser(worker_id)}, c.queryLimit, c.BatchSize)
	}
	err := q.Explain(result)
	q.Count()

	d := time.Since(t).Nanoseconds()

	if !stats.IN_WARMUP {
		stats.HammerStats.RecordSend(worker_id)
		stats.HammerStats.RecordRes(uint64(d), "findBy_tag_and_user", worker_id)
		_qa492Profile.findBy_tag_and_user_stats.RecordResponse(d)
	}

	// c.iterateAllDoc(it, count, "findBy_tag_and_user")

	return err
}

func (pp *qa492Profile) findByDateRange(collection *mgo.Collection, worker_id int) error {
	c := &_qa492Profile

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
		c.findByDateRangeStats.RecordResponse(d)
	}

	return err
}

// qa-492 new
func (pp *qa492Profile) findBy_date_range_and_user(collection *mgo.Collection, worker_id int) error {
	c := &_qa492Profile

	var q *mgo.Query

	result := bson.M{}

	// query start time is at least two days ago, to give enough range to query
	_start_time := time.Now().Add((-1) * time.Duration(rands[worker_id].Int63n(c.dateBacktrack-2880)) * time.Minute)

	// query end time is at least 100 min, up to two days
	_end_time := _start_time.Add(time.Duration(rands[worker_id].Int63n(2780)+100) * time.Minute)

	t := time.Now()

	if c.cfg.Profile.FindByDateEangeAndUserHint {
		q = queryMongo(collection,
			bson.M{"timestamp": bson.M{
				"$gte": _start_time,
				"$lte": _end_time},
				"user": c.getUser(worker_id)},
			c.queryLimit, c.BatchSize).Hint("timestamp")
	} else {
		q = queryMongo(collection,
			bson.M{"timestamp": bson.M{
				"$gte": _start_time,
				"$lte": _end_time},
				"user": c.getUser(worker_id)},
			c.queryLimit, c.BatchSize)
	}

	err := q.Explain(result)
	_, _ = q.Count()

	d := time.Since(t).Nanoseconds()

	if !stats.IN_WARMUP {
		stats.HammerStats.RecordSend(worker_id)
		stats.HammerStats.RecordRes(uint64(d), "findBy_date_range_and_user", worker_id)
		_qa492Profile.findBy_date_range_and_user_stats.RecordResponse(d)
	}

	// c.iterateAllDoc(it, count, "findBy_date_range_and_user")

	return err
}

func (pp *qa492Profile) addRemoveTags_with_find_modify(collection *mgo.Collection, worker_id int) error {
	c := &_qa492Profile

	// this will use findAndModify
	doc := QA492_Event{}

	tag_have := c.getTags(1, worker_id)
	tag_not_have := c.getTags(1, worker_id)

	change := mgo.Change{
		Update:    c.getTags(rands[worker_id].Intn(15), worker_id),
		ReturnNew: true,
	}

	_, err := collection.Find(bson.M{
		"tags": bson.M{
			"$in":  tag_have,
			"$nin": tag_not_have}}).Apply(change, &doc)

	return err
}

func (pp *qa492Profile) addRemoveTags(collection *mgo.Collection, worker_id int) error {
	c := &_qa492Profile

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
			c.updateStats.RecordResponse(d - t1)
		}
		t1 = d // adjust for the delta used
	}

	return nil
}

func (pp *qa492Profile) updatePayloadDate(collection *mgo.Collection, worker_id int) error {
	c := &_qa492Profile

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

func (pp *qa492Profile) findByPayload(collection *mgo.Collection, worker_id int) error {
	c := &_qa492Profile

	t := time.Now()
	q := queryMongo(collection, bson.M{"payload": string(payloadBuffer[0:c.getPayloadSize(worker_id)])}, c.queryLimit, c.BatchSize)
	c.iterateAllDoc(q, 0, "findByPayload")
	d := time.Since(t).Nanoseconds()

	if !stats.IN_WARMUP {
		c.findByPayloadStats.RecordResponse(d)
	}

	return nil
}

// end of runOps function

func (pp qa492Profile) SendNext(s *mgo.Session, worker_id int) error {

	collection := s.DB("test1").C("QA492")

	var err error

	r := float64(rands[worker_id].Intn(10000)) / 100.0

	// new qa492 call
	// - findBy_stream_and_user
	// - findBy_tag_and_user
	// - findBy_data_range_and_user

	switch {
	case r < _qa492Profile.threshold.th_find_by_stream_and_user:
		err = _qa492Profile.findBy_stream_and_user(collection, worker_id)
	case r < _qa492Profile.threshold.th_find_by_tag_and_user:
		err = _qa492Profile.findBy_tag_and_user(collection, worker_id)
	case r < _qa492Profile.threshold.th_find_by_date_range_and_user:
		err = _qa492Profile.findBy_date_range_and_user(collection, worker_id)
	case r < _qa492Profile.threshold.th_FindByStreamName:
		err = _qa492Profile.findByStreamName(collection, worker_id)
	case r < _qa492Profile.threshold.th_FindByTag:
		err = _qa492Profile.findByTag(collection, worker_id)
	case r < _qa492Profile.threshold.th_FindByDateRange:
		err = _qa492Profile.findByDateRange(collection, worker_id)
	case r < _qa492Profile.threshold.th_UpdateTag:
		err = _qa492Profile.addRemoveTags(collection, worker_id)
	case r < _qa492Profile.threshold.th_UpdatePayload:
		err = _qa492Profile.updatePayloadDate(collection, worker_id)
	case r < _qa492Profile.threshold.th_AddDoc:
		err = _qa492Profile.addNewDoc(collection, true, worker_id)
	case r < _qa492Profile.threshold.th_FindByPayload:
		err = _qa492Profile.findByPayload(collection, worker_id)

	default:
		log.Println("hmm.. got ", r)
		log.Panicf("c.threshold.th_FindByPayload is %+v", _qa492Profile.threshold)
	}

	// if err != nil {
	// 	log.Println(err)
	// }
	return err
}

func (pp qa492Profile) SetupTest(s *mgo.Session, _initdb bool) error {
	c := &_qa492Profile

	n := atomic.AddInt32(&_qa492Profile.numWorker, 1) // global
	collection := s.DB("test1").C("QA492")

	_qa492Profile.initProfile.Do(_qa492Profile.readConfigFile)

	if n == 1 {
		// I am the first one, do some init work here, and the let all other worker go

		_qa492Profile.session = s //saved session is useless... FIXME: since this is singleton

		// doing something to the DB here

		if _initdb {
			fmt.Println("Init DB, drop collections for QA492")
			collection.DropCollection()
		}

		/* indexing requirement for QA-492
		   ensureIndex( {user:1, stream:1}, {unique: true} )
		   ensureIndex( {user:1} )
		   ensureIndex( {tags: 1} )
		   ensureIndex( {stream:1, position: -1} )
		   ensureIndex( {timestamp: -1}
		*/
		index := mgo.Index{
			Key:    []string{"user", "stream"},
			Unique: true,
			// DropDups: true,
			// Background: true, // See notes.
			// Sparse: true,
		}
		err := collection.EnsureIndex(index)
		panicOnError(err)

		err = collection.EnsureIndexKey("user")
		panicOnError(err)

		err = collection.EnsureIndexKey("tags")
		panicOnError(err)

		err = collection.EnsureIndexKey("stream", "-position")
		panicOnError(err)

		err = collection.EnsureIndexKey("-timestamp")
		panicOnError(err)

		// err = collection.EnsureIndexKey("UID")
		// panicOnError(err)

		_u := int64(0)
		_qa492Profile.UID = -1 // UID start with 0

		// log.Println("Staging ", c.staging, " doc")
		// staging some data
		for int(_u) < _qa492Profile.staging {
			_qa492Profile.addNewDoc(collection, false, 1)

			_u = atomic.AddInt64(&_qa492Profile.UID, 1) // to make this unique
		}

		// now I am done as the lucky first workers, wake all
		_qa492Profile.syncWG.Done()
	} else {
		// have to wait
		_u := int64(0)
		// staging some data
		for int(_u) < _qa492Profile.staging {
			c.addNewDoc(collection, false, 1)
			_u = atomic.AddInt64(&_qa492Profile.UID, 1) // to make this unique

		}
		_qa492Profile.syncWG.Wait() // just wait for the first worker done
	}

	return nil
}

func (i qa492Profile) CsvString(total_time float64) string {
	return fmt.Sprintf("%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f",
		_qa492Profile.findBy_stream_and_user_stats.GetThroughput(total_time),
		_qa492Profile.findBy_stream_and_user_stats.GetAvg(total_time),

		_qa492Profile.findBy_tag_and_user_stats.GetThroughput(total_time),
		_qa492Profile.findBy_tag_and_user_stats.GetAvg(total_time),

		_qa492Profile.findBy_date_range_and_user_stats.GetThroughput(total_time),
		_qa492Profile.findBy_date_range_and_user_stats.GetAvg(total_time),

		_qa492Profile.insertStats.GetThroughput(total_time),
		_qa492Profile.insertStats.GetAvg(total_time),

		_qa492Profile.findByTagStats.GetThroughput(total_time),
		_qa492Profile.findByTagStats.GetAvg(total_time),

		_qa492Profile.findByStreamNameStats.GetThroughput(total_time),
		_qa492Profile.findByStreamNameStats.GetAvg(total_time),

		_qa492Profile.findByDateRangeStats.GetThroughput(total_time),
		_qa492Profile.findByDateRangeStats.GetAvg(total_time),

		_qa492Profile.findByPayloadStats.GetThroughput(total_time),
		_qa492Profile.findByPayloadStats.GetAvg(total_time),

		_qa492Profile.updateStats.GetThroughput(total_time),
		_qa492Profile.updateStats.GetAvg(total_time),

		_qa492Profile.iteratorStats.GetThroughput(total_time),
		_qa492Profile.iteratorStats.GetAvg(total_time))

}

func (pp qa492Profile) CsvHeader() string {
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
		"QA492 findByStreamAndUser throughput, findByStreamAndUser avg time," +
		"findByTagAndUser throughput, findByTagAndUser avg time," +
		"findByDateRangeAndUser throughput, findByDateRangeAndUser avg time," +
		"Add Doc Throughput,Add Doc avg time," +
		"findByTag Query Throughput,findByTag avg time," +
		"findByStreamName Query Throughput,findByStreamName avg time," +
		"findByDateRange Query Throughput,findByDateRange avg time," +
		"findByPayload Query Throughput,findByPayload avg time," +
		"update doc Throughput,update doc avg time," +
		"iterator throughput,iterator avg time"
}

func (pp *qa492Profile) readConfigFile() {
	var err error
	c := &_qa492Profile

	// add environment variable for control which profile to be used
	s := os.Getenv("HT_PROFILE_CONFIG")

	if s == "" {
		err = gcfg.ReadFileInto(&c.cfg, "qa492.conf")
	} else {
		err = gcfg.ReadFileInto(&c.cfg, s)
	}

	if err != nil {
		log.Fatalf("Failed to parse gcfg data: %s", err)
	}

	// now apply this to
	_qa492Profile.updateProfile()

	fmt.Println("Init QA492 profile done")
}

// add a weight to the percentage and return the new one.
func add_new_method(weight float64, percentage float64) float64 {
	return weight + percentage
}

func (pp *qa492Profile) updateProfile() {
	c := &_qa492Profile

	var percentage float64

	percentage = 0.0

	// update threshold based on cfg
	// add new QA492 methods
	// 		find_by_stream_and_user     float64
	//  	find_by_tag_and_user        float64
	//  	find_by_date_range_and_user float64
	// new config
	//  FindByStreamAndUser    float64
	//  FindByTagAndUser       float64
	//  FindByDateEangeAndUser float64

	fmt.Println("find_by_stream_and_user---> ", c.cfg.Profile.FindByStreamAndUser)
	percentage = add_new_method(c.cfg.Profile.FindByStreamAndUser, percentage)
	c.threshold.th_find_by_stream_and_user = percentage

	percentage = add_new_method(c.cfg.Profile.FindByTagAndUser, percentage)
	c.threshold.th_find_by_tag_and_user = percentage

	percentage = add_new_method(c.cfg.Profile.FindByDateEangeAndUser, percentage)
	c.threshold.th_find_by_date_range_and_user = percentage

	// old
	percentage = add_new_method(c.cfg.Profile.FindByStreamName, percentage)
	c.threshold.th_FindByStreamName = percentage

	percentage = add_new_method(c.cfg.Profile.FindByTag, percentage)
	c.threshold.th_FindByTag = percentage

	percentage = add_new_method(c.cfg.Profile.FindByDateRange, percentage)
	c.threshold.th_FindByDateRange = percentage

	percentage = add_new_method(c.cfg.Profile.UpdateTag, percentage)
	c.threshold.th_UpdateTag = percentage

	percentage = add_new_method(c.cfg.Profile.UpdatePayload, percentage)
	c.threshold.th_UpdatePayload = percentage

	percentage = add_new_method(c.cfg.Profile.AddDoc, percentage)
	c.threshold.th_AddDoc = percentage

	percentage = add_new_method(c.cfg.Profile.FindByPayload, percentage)
	c.threshold.th_FindByPayload = percentage

	c.staging = c.cfg.Profile.Staging
	c.queryLimit = c.cfg.Profile.QueryLimit
	c.streamNames_len = c.cfg.Profile.NumOfStreams
	c.tagNames_len = c.cfg.Profile.NumOfTags
	c.payloadBuffer_len = len(c.payloadBuffer)

	if c.streamNames_len == 0 {
		c.streamNames_len = 1000 // set default
	}

	if c.tagNames_len == 0 {
		c.tagNames_len = 10240 // set default
	}

	c.streamNames = make([]string, c.streamNames_len)
	c.tagNames = make([]string, c.tagNames_len)

	if !(math.Abs(100.0-percentage) < 0.0005) {
		log.Fatalln("QA492 Total Distribution is not 100%, received total as ", percentage)
	}

	if c.cfg.Profile.Distribution == "uniform" {
		c.distribution = uniformDistribution
	} else if c.cfg.Profile.Distribution == "normal" {
		c.distribution = normalDistribution
		c.normalDistributionStdDev = c.cfg.Profile.NormalStdDevRatio
	}

	//process dateRange
	if c.cfg.Profile.DateRange != 0 {
		_qa492Profile.dateBacktrack = int64(c.cfg.Profile.DateRange * 24 * 60) // in minuted
	}

	// init staging data
	_qa492Profile.randomPayloadBuffer()
	_qa492Profile.initStreamNames()
	_qa492Profile.initTagNames()
}

func init() {
	var err error
	_logFile, err = os.Create("testlogfile")
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	_logFile.WriteString(fmt.Sprintf("%s, %s, %s, %s\n", "ops", "query.Count", "counted by iteration", "time use (ms)"))

	_qa492Profile.dateBacktrack = 43200       // how long to distribute the doc
	atomic.StoreInt64(&_qa492Profile.UID, -1) // UID starts with 1
	atomic.StoreInt32(&_qa492Profile.numWorker, 0)

	_qa492Profile.syncWG.Add(1) // I the first one, so I will block everyone else until they are let go

	// register a second name for easier access
	registerProfile("QA492", func() Profile {
		return _qa492Profile // use the same instance
	})
}
