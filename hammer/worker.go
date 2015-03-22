package hammer

import (
	"log"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rzh/hammer.mongo/profiles"
	"github.com/rzh/hammer.mongo/stats"

	"gopkg.in/mgo.v2"
)

var _uid int64 // global to control unique uid

type MongoWorker struct {
	id         int
	server     string
	control    <-chan int
	throttle   <-chan time.Time
	session    *mgo.Session
	collection *mgo.Collection
	c          int
	profile    profiles.Profile
	total      uint64
}

func (w *MongoWorker) GetMgoSession() *mgo.Session {
	return w.session
}

func (w *MongoWorker) Run(c <-chan int, t <-chan time.Time) {
	// var m sync.Mutex

	w.control = c
	w.throttle = t

	// fmt.Print(" ", w.id)

	f := func() {
		// var stat_count uint64 = 0
		var t1, response_time int64 = 0, 0
		var sent uint64
		// var stat_response_time uint64
		var err error
		var _warmup bool

		for {

			/*  TODO: disable control for now
			select {
			case <-w.control:
				// something from control channel here, right now it is only quit
				return

			default:

			*/
			_warmup = stats.IN_WARMUP

			if w.throttle != nil {
				// here if we specified a RPS, otherwise, to find out MAXRPS
				<-w.throttle
			}

			if !_warmup {
				//stat_count++
				//if stat_count == stats.STAT_BATCH_SIZE {
				sent = stats.HammerStats.RecordSend(w.id)
				//}
			}

			if w.total != 0 && sent > w.total {
				syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
			}

			if w.profile == nil {
				log.Fatalln("profile is nil")
			} else if w.session == nil {
				// log.Fatalln("session is nil")
				log.Println("worker session is empty, exit this worker. Worker id is ", w.id)
				return // exit this worker.
			}

			t1 = time.Now().UnixNano()
			err = w.profile.SendNext(w.session, w.id)
			response_time = time.Now().UnixNano() - t1

			if !_warmup {
				if err == nil {
					//if stat_count == stats.STAT_BATCH_SIZE {
					stats.HammerStats.RecordRes(uint64(response_time), "NOP", w.id)
					//stat_response_time = 0
					//stat_count = 0
					//} else {
					//stat_response_time += uint64(response_time)
					//}
				} else {
					stats.HammerStats.RecordError(w.id)
				}
			}

			w.c = w.c + 1
		}
		// }  // TODO, control is disabled for now
	}

	go f()
	// go f() // double aggressive doesn't help with throughtput, just increase wait-for-mutex time
}

func (w *MongoWorker) InitWorker(
	i int,
	_server string,
	_initdb bool,
	_profile string,
	_total int64,
	_dial_info mgo.DialInfo,
	masterMgoSession *mgo.Session) {
	w.id = i

	w.profile = profiles.GetProfile(strings.ToUpper(_profile))

	if w.profile == nil {
		log.Fatalln("what?! the profile I got for ", _profile, " is nil, quit")
	}
	w.total = uint64(_total)

	// we have to do more Mongo session here
	w.server = _server

	var err error

	// FOR SPEED CHANGE: if masterMgoSession == nil {
	// always dial here
	if true {
		// w.session, err = mgo.Dial(w.server)

		w.session, err = mgo.DialWithInfo(&_dial_info)
	} else {
		w.session = masterMgoSession.Copy()
		// log.Println("initiate worker ", w.id, " via mgo.Copy")
	}

	if err != nil {
		log.Fatal("Cannot dial mongod with error: ", err, "\n", string(debug.Stack()))
	}
	// fmt.Print("(", w.id, ", ", float32(time.Since(t).Nanoseconds())/1.0e9, ") ")

	w.session.EnsureSafe(&mgo.Safe{})

	// w.session = session

	w.profile.SetupTest(w.session, _initdb)

	w.c = 0
}

func init() {
	// init counter
	// fmt.Println("init counter for worker")
	atomic.StoreInt64(&_uid, 0)
}
