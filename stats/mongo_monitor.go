package stats

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var HammerMongoStats MongoStats
var _db_name string

// to monitor Mongo Server Status.

// ****
// this module not complete, still investigate how to use mgo for adminCommand  TODO!!!
// ****

type MongoStats struct {
	server string

	session *mgo.Session
	// collection *mgo.Collection

	// stats track mongod stats
	prev_m  ServerStatus
	_inited bool
	// curr_m ServerStatus
}

// struct to hold serverStatus, this may not be ideal way to do this, TODO
type ServerStatus struct {
	// "uptimeMillis" : NumberLong(701103909),
	Uptime       int64
	UptimeMillis int64 `bson:"uptimeMillis"`

	/*
		"connections" : {
			"current" : 4,
			"available" : 2044,
			"totalCreated" : NumberLong(6128)
		},
	*/
	Connections map[string]int
	/*
		"cursors" : {
			"totalOpen" : 0,
			"clientCursors_size" : 0,
			"timedOut" : 39
		},
	*/
	Cursors map[string]int

	/*
		"opcounters" : {
			"insert" : 137499268,
			"query" : 2664989,
			"update" : 31827171,
			"delete" : 0,
			"getmore" : 596,
			"command" : 107359317
		},
	*/
	Opcounters map[string]int

	/*
		"mem" : {
			"bits" : 64,
			"resident" : 4278,
			"virtual" : 36037,
			"supported" : true,
			"mapped" : 16729,
			"mappedWithJournal" : 33458
		},
	*/
	Mem map[string]int

	/*
		"extra_info" : {
			"note" : "fields vary by platform",
			"page_faults" : 4034466
		},
	*/
	Extra_info map[string]int

	/*
		"globalLock" : {
			"totalTime" : NumberLong("692055293000"),
			"lockTime" : NumberLong(1523335985),
			"currentQueue" : {
				"total" : 0,
				"readers" : 0,
				"writers" : 0
			},
			"activeClients" : {
				"total" : 0,
				"readers" : 0,
				"writers" : 0
			}
		},
	*/
	GlobalLock map[string]map[string]int `bson:"globalLock"`

	/*
		"backgroundFlushing" : {
			"flushes" : 7825,
			"total_ms" : 1295781,
			"average_ms" : 165.59501597444088,
			"last_ms" : 12017,
			"last_finished" : ISODate("2014-01-23T19:43:59.507Z")
		},
	*/
	BackgroundFlushing map[string]int `bson:"backgroundFlushing"`

	/*
		"indexCounters" : {
			"accesses" : 543179283,
			"hits" : 543179119,
			"misses" : 0,
			"resets" : 0,
			"missRatio" : 0
		},
	*/
	IndexCounters map[string]int

	/*
		"locks" : {
			"." : {    <-- global lock
				"timeLockedMicros" : {
					"R" : NumberLong(58871490),
					"W" : NumberLong(1344716751)
				},
				"timeAcquiringMicros" : {
					"R" : NumberLong(142669463),
					"W" : NumberLong(3293543)
				}
			},
			....
			"test1" : {    <-- per DB lock
				"timeLockedMicros" : {
					"r" : NumberLong(239821),
					"w" : NumberLong(73567)
				},
				"timeAcquiringMicros" : {
					"r" : NumberLong(81857),
					"w" : NumberLong(7573)
				}
			},
	*/
	Locks map[string]map[string]map[string]int64

	/*struct {
		TimeLockedMicros    map[string]int64
		TimeAcquiringMicros struct {
			R int64
			W int64
		}
	}*/

	Host string
}

/*
	> db.runCommand({dbStats:1})
	{
		"db" : "test",
		"collections" : 6,
		"objects" : 58604887,
		"avgObjSize" : 359.6749222637354,
		"dataSize" : 21078708176,
		"storageSize" : 23281577872,
		"numExtents" : 69,
		"indexes" : 6,
		"indexSize" : 1991583664,
		"fileSize" : 29984030720,
		"nsSizeMB" : 16,
		"dataFileVersion" : {
			"major" : 4,
			"minor" : 5
		},
		"ok" : 1
	}
*/

type DbStats struct {
	Db      string `bson:"db"`
	Objects string `bson:"objects"`

	AvgObjSize string `bson:"avgObjSize"`

	IndexSize string `bson:"indexSize"`

	IndexFileSize string `bson:"fileSize"` // index file size
}

func (m *MongoStats) InitMongo_Monitor(_server string) {
	s := os.Getenv("HT_DB_NAME")

	if s == "" {
		_db_name = "htest"
	} else {
		_db_name = s
	}

	m.server = _server
	m.prev_m = ServerStatus{}
	m._inited = false

	info := mgo.DialInfo{
		FailFast: true,
		Addrs:    strings.Split(_server, ","),
	}

	session, err := mgo.DialWithInfo(&info)

	if err != nil {
		panic(err)
	}

	session.SetSafe(&mgo.Safe{})
	m.session = session
}

func (m *MongoStats) MonitorMongo() (string, string) {
	result := &ServerStatus{}

	/*
		mongo command to get lock-less serverStatus
		db.runCommand( { "serverStatus": 1, recordStats:0, metrics:0, locks:0, connections:1, dur:0, opcountersRepl:0, opcounters:0,globalLock:0,backgroundFlushing:0,indexCounters:0,asserts:0 } )
	*/

	err := m.session.Run(bson.D{
		{"serverStatus", 1},
		{"recordStats", 0}}, result) // no lock
	// err := m.session.Run("serverStatus", result)

	if err != nil {
		// ignore error for serverStatus
	}

	lockRatio := 0.0

	if m._inited {
		lockRatio = float64(result.Locks["."]["timeLockedMicros"]["W"]-m.prev_m.Locks["."]["timeLockedMicros"]["W"]+
			result.Locks[_db_name]["timeLockedMicros"]["w"]-m.prev_m.Locks[_db_name]["timeLockedMicros"]["w"]) /
			(1000.0 * float64(result.UptimeMillis-m.prev_m.UptimeMillis)) // hardcoded for now FIXME
	}

	faults := result.Extra_info["page_faults"] - m.prev_m.Extra_info["page_faults"]
	m.prev_m = *result
	m._inited = true

	// check DB stats on
	return fmt.Sprint(" | ", "Mongo conn: ", result.Connections["current"], " ",
			" Mem res(M): ", result.Mem["resident"],
			" mapped(M): ", result.Mem["mapped"],
			" PageFault: ", faults,
			" DB(", _db_name, ") locked: ", fmt.Sprintf("%2.2f%s", lockRatio*100, "%"),
			" qr: ", result.GlobalLock["currentQueue"]["readers"],
			" qw: ", result.GlobalLock["currentQueue"]["writers"],
			" ar: ", result.GlobalLock["activeClients"]["readers"],
			" aw: ", result.GlobalLock["activeClients"]["writers"],
			" avg flush(ms): ", result.BackgroundFlushing["average_ms"],
			" last flush(ms): ", result.BackgroundFlushing["last_ms"]),

		fmt.Sprint(result.Connections["current"], //Mongo conn:
			",", result.Mem["resident"], //  Mem res(M)
			",", result.Mem["mapped"], // mapped(M)
			",", faults, // PageFault
			",", fmt.Sprintf("%2.2f", lockRatio*100),
			",", result.GlobalLock["currentQueue"]["readers"],
			",", result.GlobalLock["currentQueue"]["writers"],
			",", result.GlobalLock["activeClients"]["readers"],
			",", result.GlobalLock["activeClients"]["writers"],
			",", result.BackgroundFlushing["average_ms"],
			",", result.BackgroundFlushing["last_ms"])
}

func (m *MongoStats) CsvHeader() string {
	if _env_no_serverStatus {
		return "NA"
	}
	return "conn,Mem res(M),Mem mapped(M),PageFault,locked,qr,qw,ar,aw,avg flush(ms),last flush(ms)"
}
