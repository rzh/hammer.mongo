package profiles

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rzh/hammer.mongo/stats"
	"gopkg.in/mgo.v2"
)

// Profile is the interface to define Profiles.
type Profile interface {
	// Next()                      // return next call information  ??Need this??
	SendNext(c *mgo.Session, worker_id int) error // prepare and send the next call
	SetupTest(s *mgo.Session, _initdb bool) error
	CsvString(total_time float64) string
	CsvHeader() string
}

var rands []*rand.Rand
var randomSrc []RandomDataMaker

func InitProfile(_num_of_workers int) {
	log.Println("Init profiles")
	rands = make([]*rand.Rand, _num_of_workers, _num_of_workers)
	randomSrc = make([]RandomDataMaker, _num_of_workers, _num_of_workers)

	for i := 0; i < _num_of_workers; i++ {
		rands[i] = rand.New(rand.NewSource(int64(time.Now().Nanosecond() + i)))
		randomSrc[i] = RandomDataMaker{rand.NewSource(int64(time.Now().Nanosecond() + i))}
	}
}

type ProfileFinalFunc interface {
	Final(c *mgo.Session) // will be run before program exit, used for correctness check
}

var finalFunc *ProfileFinalFunc = nil

func CallFinalFunc(c *mgo.Session) {
	if finalFunc != nil {
		(*finalFunc).Final(c)
	}
}

// // var _hammer_stats *stats.Stats

// func InitStats(_stat *stats.Stats) {
// 	_hammer_stats = _stat
// }

type getNextFunc func() Profile // to return a profile
type ProfilePointer *Profile

type operationFunc func(c *mgo.Collection, max_id int, worker_id int) error

var _profiles map[string]getNextFunc
var _logFile *os.File
var _mutex sync.Mutex // mutext to prevent logfile corruption
var _multi_db int
var _multi_col int
var _db_name string
var _profile_use_legacy_write bool

const alpha_numeric_chars_with_space = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 abcdefghijklmnopqrstuvwxyz"
const alpha_numeric_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyz"
const all_chars = alpha_numeric_chars + "~!@#$%^&*()-_+={}[]\\|<,>.?/\"';:`"
const alpha_numeric_chars_with_space_len = len(alpha_numeric_chars_with_space)
const alpha_numeric_chars_len = len(alpha_numeric_chars)
const all_chars_len = len(all_chars)

const default_db_name_prefix = "sbtest"
const default_col_name_prefix = "sbtest"

var _currentProfile Profile // to hold what is the current profile

func GetCurrentProfileCSV(total_time float64) string {
	return _currentProfile.CsvString(total_time)
}

func GetProfileCSVHeader() string {
	return _currentProfile.CsvHeader()
}

var _get_profile_mutex sync.Mutex

func GetProfile(s string) Profile {
	// sync _get_profile
	_get_profile_mutex.Lock()

	if _, ok := _profiles[s]; ok {
		_currentProfile = _profiles[s]()
		_get_profile_mutex.Unlock()
		return _profiles[s]()
	} else {
		fmt.Println("\n\nError: Profile= " + s + " Please specify a valid profile name, valid profiles are:")
		for key, _ := range _profiles {
			fmt.Println("    ", key)
		}
		os.Exit(0)
		return nil
	}
}

func registerProfile(_name string, _func getNextFunc) {

	if _func == nil {
		panic("must provide a valid getNextFunc")
	}

	_profiles[strings.ToUpper(_name)] = _func
}

func init() {
	_profiles = make(map[string]getNextFunc)

	var err error
	s := os.Getenv("HT_MULTI_COLLECTION")

	if s == "" {
		_multi_col = 1
	} else {
		_multi_col, err = strconv.Atoi(s)
		panicOnError(err)
		log.Println("multi collection with ", _multi_col)

		if _multi_col <= 0 {
			log.Panicln("Got HT_MUTLI_DB $le 0, valure read ==>", _multi_col)
		}
	}

	s = os.Getenv("HT_MULTI_DB")

	if s == "" {
		_multi_db = 1
	} else {
		_multi_db, err = strconv.Atoi(s)
		panicOnError(err)
		log.Println("multi DB with ", s, "| and multi collection ", _multi_col)

		if _multi_db <= 0 {
			log.Panicln("Got HT_MUTLI_DB $le 0, valure read ==>", _multi_db)
		}
	}

	s = os.Getenv("HT_DB_NAME")

	if s == "" {
		_db_name = "htest"
	} else {
		_db_name = s
	}

	s = os.Getenv("HT_USE_LEGACY_WRITE")

	if s == "" || s == "0" {
		_profile_use_legacy_write = false
	} else if s == "1" {
		_profile_use_legacy_write = true
		log.Println("HT: use legacy write op")
	} else {
		log.Println("HT_USE_LEGACY_WRITE shall be either 0 or 1, got ", s)
		os.Exit(1)
	}
}

// utility functions

func getDBName(prefix string) string {
	if _multi_db == 1 {
		return fmt.Sprint(prefix, 1)
	} else {
		return fmt.Sprint(prefix, rand.Intn(_multi_db)+1)
	}
}

func getCollectionName(prefix string) string {
	if _multi_col == 1 {
		return fmt.Sprint(prefix, 1)
	} else {
		return fmt.Sprint(prefix, rand.Intn(_multi_col)+1)
	}
}

type randomDataMaker struct {
	src rand.Source
}

func (r *randomDataMaker) Read(p []byte) {
	todo := len(p)
	offset := 0
	for {
		val := int64(r.src.Int63())
		for i := 0; i < 8; i++ {
			p[offset] = byte(val & 0xff)
			todo--
			if todo == 0 {
				return
			}
			offset++
			val >>= 8
		}
	}

	panic("unreachable")
}

type RandomDataMaker struct {
	src rand.Source
}

func (r *RandomDataMaker) Read(p []byte) (n int, err error) {
	todo := len(p)
	offset := 0
	for {
		val := int64(r.src.Int63())
		for i := 0; i < 8; i++ {
			p[offset] = byte(val & 0xff)
			todo--
			if todo == 0 {
				return len(p), nil
			}
			offset++
			val >>= 8
		}
	}

	panic("unreachable")
}

func randomString(n int, _worker int) string {
	x := bytes.NewBuffer(nil)

	_, err := io.CopyN(x, &randomSrc[_worker], int64(n))
	if err != nil {
		log.Fatalf("Error copying at %v: %v", n, err)
	}
	return string(x.Bytes())
}

func randomWord(n int) string {
	x := make([]byte, n)

	for i := 0; i < n; i++ {
		x[i] = byte(alpha_numeric_chars[rand.Intn(alpha_numeric_chars_len)])
	}

	return string(x)
}

func logData(_title string, _ops string, _count int, _counted int, _time int64) {
	if !stats.IN_WARMUP {
		_mutex.Lock()
		// _logFile.WriteString(fmt.Sprint(time.Now().Format(time.Stamp)))
		_logFile.WriteString(fmt.Sprintf("%s - %s, %d, %d, %4.4f\n", _title, _ops, _count, _counted, float64(_time)/1000000.0))
		_mutex.Unlock()
	}
}

func queryMongo(collection *mgo.Collection, query interface{}, queryLimit int, batchSize int) *mgo.Query {
	var q *mgo.Query

	if queryLimit > 0 && batchSize > 0 {
		q = collection.Find(query).Batch(batchSize).Limit(queryLimit)
	} else if queryLimit > 0 {
		q = collection.Find(query).Limit(queryLimit)
	} else {
		q = collection.Find(query) // unlimited
	}

	return q
}

func panicOnError(err error) {
	if err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}
}

func normalInRange(drange int64, stdDev float64) int64 {
	i := int64(-1)

	for i < 0 || i >= drange {
		i = int64((rand.NormFloat64()*stdDev + 0.5) * float64(drange))
	}
	return i
}

func getOSEnvFlag(f string, low int, _default int) int {
	s := os.Getenv(f)

	if s != "" {
		i, err := strconv.Atoi(s)
		panicOnError(err)

		if i < low {
			log.Panicln(f, " must greater than ", low, ", received ", i)
		}

		return i
	}
	return _default
}
