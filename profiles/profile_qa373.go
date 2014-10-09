package profiles

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

/*
	basic insert only profile, insert a doc of 400 bytes
*/

const (
	// regular
	mode_Update int = iota

	// expand the doc
	mode_AddToSet     int = iota
	mode_SetNewField  int = iota
	mode_SetOnInsert  int = iota
	mode_Inc          int = iota
	mode_Push         int = iota
	mode_PushPosition int = iota

	// shrink the doc
	mode_PopFirst int = iota
	mode_PopLast  int = iota
	mode_Pull     int = iota
	mode_PullAll  int = iota
	mode_Unset    int = iota

	mode_Staging_Unset int = iota
	mode_Run_All       int = iota
)

type qa373Profile struct {
	MaxUID int64

	// UpdateMode  int
	MultiUpdate int

	stagingId int64

	session *mgo.Session
}

var _qa373Profile qa373Profile
var _update_mode int
var _use_normal bool

func (i qa373Profile) SendNext(s *mgo.Session, worker_id int) error {

	c := s.DB(_db_name).C("people")

	if _qa373Profile.MaxUID == 0 {
		// to find out how many records we have
		n, err := c.Count()

		if err != nil {
			panic("cannot count collection")
		}
		_qa373Profile.MaxUID = int64(n)
		fmt.Println("Total doc", n)
	}

	var _u int64

	if _use_normal {
		_u = normalInRange(_qa373Profile.MaxUID, 0.16667)
	} else {
		_u = rand.Int63n(_qa373Profile.MaxUID) // to find a random person

	}

	var err error

	// switch _qa373Profile.UpdateMode {
	switch _update_mode {
	case mode_Update: // regular update
		err = c.Update(bson.M{"_id": _u}, bson.M{"$set": bson.M{"group": rand.Int()}})

	case mode_AddToSet:
		err = c.Update(bson.M{"_id": _u}, bson.M{"$addToSet": bson.M{"payload": rand.Int63n(5000000) + 1}})

	case mode_SetNewField: // $set random field
		err = c.Update(bson.M{"_id": _u}, bson.M{"$set": bson.M{randomWord(10): rand.Int()}})

	case mode_Inc: // $inc
		err = c.Update(bson.M{"_id": _u}, bson.M{"$inc": bson.M{"group": 1}})

	case mode_Push: // $push
		err = c.Update(bson.M{"_id": _u}, bson.M{"$push": bson.M{"payload": rand.Intn(5000)}})

	case mode_PushPosition: // $inc
		err = c.Update(bson.M{"_id": _u}, bson.M{"$push": bson.M{"payload": bson.M{"$each": []int{rand.Intn(5000), rand.Intn(5000)}, "$position": rand.Intn(20)}}})

	case mode_SetOnInsert:
		_, err = c.Upsert(bson.M{"_id": _u}, bson.M{"$setOnInsert": bson.M{"group": rand.Int63n(5000000) + 1}})

	// shrink doc
	case mode_PopFirst:
		err = c.Update(bson.M{"_id": _u}, bson.M{"$pop": bson.M{"payload": -1}})

	case mode_PopLast:
		err = c.Update(bson.M{"_id": _u}, bson.M{"$pop": bson.M{"payload": 1}})

	case mode_PullAll:
		r := []int{rand.Intn(100), rand.Intn(100), rand.Intn(100)}
		err = c.Update(bson.M{"_id": _u}, bson.M{"$pullAll": bson.M{"payload": r}})

	case mode_Pull:
		err = c.Update(bson.M{"_id": _u}, bson.M{"$pull": bson.M{"payload": rand.Intn(20)}})

	case mode_Unset:
		err = c.Update(bson.M{"_id": _u}, bson.M{"$unset": bson.M{fmt.Sprintf("%d", rand.Intn(30)): ""}})

	case mode_Staging_Unset:
		i := atomic.AddInt64(&_qa373Profile.stagingId, 1)
		for j := 0; j < 30; j++ {
			err = c.Update(bson.M{"_id": i}, bson.M{"$set": bson.M{fmt.Sprintf("f%d", j): fmt.Sprint("payload for ", j)}})
		}

		// update doc i with all the field

	// here is just wrong
	default:
		log.Panicln("Unknown update mode : ", _update_mode)
	}

	if err != nil {
		fmt.Println(err, "uid is ", _u)
	}
	return err
}

func (i qa373Profile) SetupTest(s *mgo.Session, _initdb bool) error {
	i.session = s

	return nil
}

func (i qa373Profile) CsvString(total_time float64) string {
	return ""
}

func (i qa373Profile) CsvHeader() string {
	return ""
}

func init() {
	_qa373Profile.MaxUID = 0
	_qa373Profile.stagingId = 0
	_qa373Profile.MultiUpdate = 1

	rand.Seed(time.Now().UnixNano())

	_update_mode = mode_Update

	s := os.Getenv("HT_QA373_MODE")

	switch strings.ToLower(s) {
	case "":
	case "update":
		_update_mode = mode_Update
		log.Println("Update mode : in place update")
	case "addtoset":
		_update_mode = mode_AddToSet
		log.Println("Update mode : $addToSet")
	case "inc":
		_update_mode = mode_Inc
		log.Println("Update mode : $inc")
	case "push":
		_update_mode = mode_Push
		log.Println("Update mode : $push")
	case "pushposition":
		_update_mode = mode_PushPosition
		log.Println("Update mode : $pushposition")
	case "addfield":
		_update_mode = mode_SetNewField
		log.Println("Update mode : $set new field")
	case "setoninsert":
		_update_mode = mode_SetOnInsert
		log.Println("Update mode : $set with upsert")

	// shrink doc
	case "popfirst":
		_update_mode = mode_PopFirst
		log.Println("Update mode : $pop first")
	case "poplast":
		_update_mode = mode_PopLast
		log.Println("Update mode : $pop last")
	case "pull":
		_update_mode = mode_Pull
		log.Println("Update mode : $pull")
	case "pullall":
		_update_mode = mode_PullAll
		log.Println("Update mode : $pullAll")
	case "unset":
		_update_mode = mode_Unset
		log.Println("Update mode : $unset")

	case "stagingunset":
		_update_mode = mode_Staging_Unset
		log.Println("Update mode : staging for unset test")

	case "runall":
		_update_mode = mode_Run_All
		log.Println("Update mode : run all update concurrent")

	default:
		log.Panicln("Unknown HT_QA373_MODE: ", s)
	}

	s = os.Getenv("HT_QA373_MULTI_DOC")
	if s != "" {
		// set

		t, e := strconv.Atoi(s)

		if e != nil {
			log.Panicln("HT_QA373_MULTI_DOC must be a valid number: ", s)
		}
		_qa373Profile.MultiUpdate = t
	}

	_use_normal = false
	s = os.Getenv("HT_QA373_NORMAL_DIST")
	if s != "" {
		_use_normal = true
	}

	registerProfile("qa373", func() Profile {
		return Profile(_qa373Profile) // use the same instance
	})
}
