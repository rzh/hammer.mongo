package hammer

import (
	"fmt"

	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rzh/hammer.mongo/profiles"
	"github.com/rzh/hammer.mongo/stats"
	"gopkg.in/mgo.v2"
)

// some internal variable

// to reduce size of thread, speed up
const SizePerThread = 10000000

var initialized bool
var workers []MongoWorker
var control_channel chan int
var throttle_channel <-chan time.Time
var monitor_channel *time.Ticker

// var stats Stats
// var mongoStats MongoStats

var monitor_interval int64
var warmup int64
var mongo_server string
var silent bool
var totaltime int64
var masterMgoSession *mgo.Session

var traffic_profile string

const MAXRPS = 0

/** func visible to external **/

// to start traffic
func Start(rps int64) {
	/**
		- check RPS make sense
		- init generator
		- and start!!
	**/
	if !silent {
		fmt.Println("Run workers ... with RPS ", rps)
	}

	if rps > MAXRPS {
		_p := time.Duration(rps)
		_interval := 0.99 * 1000000000.0 / _p                     // FIXME: the ratio shall be adaptive
		throttle_channel = time.Tick(_interval * time.Nanosecond) // init according to RPS

		for i, _ := range workers {
			workers[i].Run(control_channel, throttle_channel)
		}
	} else if rps == MAXRPS {
		for i, _ := range workers {
			workers[i].Run(control_channel, nil)
		}
	} else {
		panic("why I am here, RPS shall be greater than 0")
	}

	// fmt.Println(" ")

	// now start monitor
	monitor_channel = time.NewTicker(time.Second * time.Duration(monitor_interval))

	stats.HammerStats.StartMonitoring(monitor_channel)

	// setup call to handle SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGKILL)

	go func() {
		<-c
		stats.PrettyPrint()
		os.Exit(0)
	}()

	// now setup to clear warmup after time is up
	// this will just put this goroutine into sleep, give up control
	if warmup > 0 {
		log.Println("warming up the test profile")
		time.Sleep(time.Second * time.Duration(warmup))
		// now it is time to set flag back
		stats.IN_WARMUP = false
		log.Println("done with warming up the test profile")
	}

	// warmup is done, now it is time to kick start timer for totaltime if it is set
	if totaltime > 0 {
		time.Sleep(time.Second * time.Duration(totaltime+2)) // add two since Stats will alway skip the first two second in printing
		// now stop the program
		// FIXME: pretty print final stats
		// os.Exit(0)
		c <- syscall.SIGTERM
	}
}

// pause
func Pause() {

}

// resume
func Resume() {

}

// adjust speed of sending
func ChangeRate(rps int) {

}

// do init for stats to link profile csv output together
func getProfileCSCHeader() string {
	return profiles.GetProfileCSVHeader()
}

func getProfileCSV(total_time float64) string {
	return profiles.GetCurrentProfileCSV(total_time)
}

func folderExist(f string) bool {
	if _, err := os.Stat(f); err != nil {
		if os.IsNotExist(err) {
			// ./test_reports does not exist
			// make it
			return false // just created the folder, not need to check further
		} else {
			// other error
			log.Println("error when checking test_reports folder", err)
			os.Exit(1)
		}
	}
	return true
}

func validRunId(_id string) {
	// first check to make reports exist, if not, create it
	if !folderExist("./test_reports") {
		if err := os.Mkdir("test_reports", os.ModePerm); err != nil {
			log.Println("Could not create test_reports folder: ", err)
			os.Exit(1)
		}
	}

	// now check whether the reports folder exist
	if folderExist("./test_reports/" + _id) {
		log.Println("test report for run_id ", _id, " exists!")
		os.Exit(1)
	}

	if err := os.Mkdir("./test_reports/"+_id, os.ModePerm); err != nil {
		log.Println("Could not create test_reports/", _id, " folder : ", err)
		os.Exit(1)
	}
}

// init
func Init(
	_num_of_workers int,
	_monitor_channel int64,
	_server string,
	_initdb bool,
	_profile string,
	_total int64,
	_warmup int64,
	_total_time int64,
	_quiet bool,
	_run_id string) {
	/**
		- init core data structure
		- init all workers if specified.
	**/

	if initialized {
		// should never init this twice
		panic("Initialized Hammer Twice!!")
	}

	if _run_id != "" {
		validRunId(_run_id)
	}

	monitor_interval = _monitor_channel
	mongo_server = _server
	warmup = _warmup
	silent = _quiet
	totaltime = _total_time

	// NCPU := runtime.NumCPU()
	// runtime.GOMAXPROCS(NCPU / 2)
	// runtime.GOMAXPROCS(6)

	//make control channel
	control_channel = make(chan int)

	if !silent {
		fmt.Print("Init workers...")
	}

	initialized = true
	workers = make([]MongoWorker, _num_of_workers)
	profiles.InitProfile(_num_of_workers)

	stats.HammerMongoStats.InitMongo_Monitor(mongo_server)
	stats.SetSilent(_quiet)             // pass -quiet flag to stats
	stats.SetNumWorker(_num_of_workers) // make sure stats know how many workers is there

	if _run_id != "" {
		stats.SetRunId(_run_id)
	}

	var sg sync.WaitGroup // have to block until all init is done

	sg.Add(_num_of_workers) // i start
	var worker_id int32
	worker_id = -1

	for i, _ := range workers {
		myid := atomic.AddInt32(&worker_id, 1)

		if !silent {
			log.Println("worker ", myid, " started")
		}

		if myid == 0 {
			go func() {
				_initdb_local := _initdb
				if !silent {
					log.Println("worker ", i, " initialization started")
				}
				_initdb = false
				workers[myid].InitWorker(int(myid), mongo_server, _initdb_local, _profile, _total, nil) // just use array index as worker id, worker will NOT start running immediately
				masterMgoSession = workers[0].GetMgoSession()

				masterMgoSession = nil // not use mgo pool

				if !silent {
					log.Println("worker ", i, " initialization done, and wait for others")
				}
				sg.Done()
			}()
		} else {
			// others will be false
			go func() {
				workers[myid].InitWorker(int(myid), mongo_server, false, _profile, _total, masterMgoSession) // just use array index as worker id, worker will NOT start running immediately
				sg.Done()                                                                                    // I done
				if !silent {
					log.Println("worker ", myid, " initialization done, and wait for others")
				}
			}()
		}
	}
	sg.Wait() // wait for all my friend worker done as well
	if !silent {
		fmt.Println(" ")
	}

	// not check warmup, and make sure proper control
	if warmup > 0 {
		// need warmup period
		stats.IN_WARMUP = true //FIXME, add an stats API for this
	} else {
		stats.IN_WARMUP = false
	}

	// link profile and stats together
	stats.InitProfileStat(getProfileCSCHeader, getProfileCSV)

	// init http here
	go func() {
		s := os.Getenv("HAMMER_REMOTE_UI")

		if s == "" {
			// no webUI
			return
		}
		mhttp := http.NewServeMux()
		mhttp.Handle("/", http.FileServer(http.Dir("./UI/public")))

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			mhttp.ServeHTTP(w, r)
		})

		if runtime.GOOS != "darwin" {
			// if specified HAMMER_REMOTE_UI or non-darwin platform, always binding to ethernet
			log.Fatal(http.ListenAndServe(":6789", nil))
		} else {
			// for darwin, and HAMMER_REMOTE_UI is empty, binding to local
			log.Fatal(http.ListenAndServe("localhost:6789", nil))
		}
	}()
}
