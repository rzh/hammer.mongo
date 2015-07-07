package hammer

import (
	"fmt"
	"strconv"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
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
var sessionTimeout time.Duration
var exitOnError bool

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
		log.Fatal("why I am here, RPS shall be greater than 0")
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
	_run_id string,
	_ssl bool,
	_ssl_ca string,
	_ssl_key string) {
	/**
		- init core data structure
		- init all workers if specified.
	**/

	if initialized {
		// should never init this twice
		log.Fatal("Initialized Hammer Twice!!")
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

	var dialInfo mgo.DialInfo

	if _ssl {
		// To enable SSL on the mgo driver is not as straightforward as other
		// drivers. As of mgo v2, you need to:
		// 1. create a tlsConfig
		// 2. add CA and server certificates to a tlsConfig
		// 3. create a custom dial function that wraps around tls.Dial
		// 4. add the custom dial function to an instance of mgo.DialInfo
		key_data, err1 := ioutil.ReadFile(_ssl_key)
		ca_data, err2 := ioutil.ReadFile(_ssl_ca)

		if err1 != nil {
			log.Fatal("could not read Server Key file")
		}

		if err2 != nil {
			log.Fatal("could not read CA Certificate file")
		}

		//
		tlsConfig := &tls.Config{
			RootCAs:            x509.NewCertPool(),
			ServerName:         "",
			InsecureSkipVerify: true,
			ClientAuth:         tls.RequireAnyClientCert,
		}
		ok1 := tlsConfig.RootCAs.AppendCertsFromPEM(key_data)
		ok2 := tlsConfig.RootCAs.AppendCertsFromPEM(ca_data)

		if !ok1 {
			log.Fatal("server key is not in PEM format")
		}

		if !ok2 {
			log.Fatal("CA Certificate is not in PEM format")
		}

		dial := func(addr net.Addr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			if err != nil {
				log.Println("tls.Dial(%s) failed with %v", addr, err)
				return nil, err
			}
			return conn, nil
		}

		dialInfo = mgo.DialInfo{
			Timeout:  sessionTimeout,
			FailFast: true,
			Addrs:    strings.Split(_server, ","),
			Dial:     dial,
		}

	} else {
		dialInfo = mgo.DialInfo{
			Timeout:  sessionTimeout,
			FailFast: true,
			Addrs:    strings.Split(_server, ","),
		}
	}

	initialized = true
	workers = make([]MongoWorker, _num_of_workers)
	profiles.InitProfile(_num_of_workers)

	stats.HammerMongoStats.InitMongo_Monitor(mongo_server, dialInfo)
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
				workers[myid].InitWorker(int(myid), mongo_server, _initdb_local, _profile, _total, dialInfo, nil) // just use array index as worker id, worker will NOT start running immediately
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
				workers[myid].InitWorker(int(myid), mongo_server, false, _profile, _total, dialInfo, masterMgoSession) // just use array index as worker id, worker will NOT start running immediately
				sg.Done()                                                                                              // I done
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

func init() {
	s := os.Getenv("HT_TIMEOUT")

	if s == "" {
		sessionTimeout = 0
	} else {
		i, err := strconv.Atoi(s)

		if err != nil {
			log.Fatalln("Cannot read HT_TIME: ", s, " with error:", err)
		}

		sessionTimeout = time.Duration(i) * time.Second
	}

	s = os.Getenv("HT_EXIT_ON_ERROR")
	exitOnError = false
	if strings.ToLower(s) == "yes" {
		exitOnError = true
	}
}
