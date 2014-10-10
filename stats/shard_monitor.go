package stats

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var _env_monitor_shard bool
var _env_stop_chunk_number int64 = 40
var _env_shards []string
var _env_mongos string
var _shard_connections []Shard
var _shard_num int64 = 3        //FIXME need findout
var _shard_chunk_number []int64 // count how many chunk per shard
//var _shard_session *mgo.Session
var _shard_mongos_session *mgo.Session

var _shard_monitor_channel *time.Ticker

// struct for Shard
type Shard struct {
	Name    string `bson:"name"`
	Url     string `bson:"url"`
	Session *mgo.Session
}

type ShardChunk struct {
	Id    string `bson:"_id"`
	Count int64  `bson:"count"`
}

// helper
func getShardName(i int64) string {
	return fmt.Sprintf("shard%04d", i)
}

func getShardNumber(name string) int64 {
	i, err := strconv.ParseInt(string(name[5:]), 10, 64)
	if err != nil {
		log.Fatalln("Failed to parse shard name ", name)
	}
	return i
}

// goal here is to monitor shard cluster
func monitorShardCluster() {
	_shard_monitor_channel = time.NewTicker(time.Second * time.Duration(10)) // monitor every 10 seconds

	// count chunk for every shard
	// this is the aggregation pipeline:
	//     db.chunks.aggregate([{$project: {shard: 1, _id: 0}}, {$group: {_id: "$shard", count: {$sum: 1}}}])}}})
	//         { "_id" : "shard0002", "count" : 2 }
	//         { "_id" : "shard0001", "count" : 1 }
	//         { "_id" : "shard0000", "count" : 1 }

	var _t_total_chunk int64
	_chunks := []ShardChunk{}

	for {
		_t_total_chunk = 0
		_shard_mongos_session.DB("config").C("chunks").Pipe(
			[]bson.M{
				{"$project": bson.M{"shard": 1, "_id": 0}},
				{"$group": bson.M{"_id": "$shard", "count": bson.M{"$sum": 1}}},
			}).All(&_chunks)

		for i := 0; i < len(_chunks); i++ {
			_t_total_chunk += ShardChunk(_chunks[i]).Count
			_shard_chunk_number[getShardNumber(_chunks[i].Id)] = _chunks[i].Count
		}
		log.Printf("%v\t%d\n", _shard_chunk_number, _t_total_chunk)
		<-_shard_monitor_channel.C
	}
}

// to initilize connection to the shard and some initial variables
func initShardCluster() {
	var err error

	// create connection pool
	_shard_connections = make([]Shard, len(_env_shards), len(_env_shards))
	for i, url := range _env_shards {
		_shard_connections[i].Name = ""
		_shard_connections[i].Url = url
		_shard_connections[i].Session, err = mgo.Dial(url)

		if err != nil {
			log.Fatalln("Cannot open mongo connection to ", url)
		}
	}

	// create connection to mongos
	_shard_mongos_session, err = mgo.Dial(_env_mongos)

	if err != nil {
		log.Fatalln("Cannot open mongos connection to ", _env_mongos)
	}
}

func init() {
	// hack for test
	os.Setenv("HT_MONITOR_SHARD", "true")
	os.Setenv("HT_MONGOS_URL", "54.68.41.49:27017")

	s := os.Getenv("HT_MONITOR_SHARD")
	if s != "" {
		_env_monitor_shard = true
	}

	if !_env_monitor_shard {
		// stop init if not monitoring
		return
	}

	s = os.Getenv("HT_MONGOS_URL")
	if s != "" {
		_env_mongos = s
	}

	s = os.Getenv("HT_SHARDS")
	if s != "" {
		_env_shards = strings.Fields(s)
		_env_monitor_shard = true

		if len(_env_shards) == 0 {
			log.Fatalln("HT_SHARDS, if set, must have at least one mongod URL")
		}
	}

	// a few special cases for test shard/auto-split
	_shard_chunk_number = make([]int64, 3, 3)

	initShardCluster()
	go monitorShardCluster()
}
