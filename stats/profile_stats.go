package stats

import (
	"encoding/json"
	"log"
)

// helper to return histogram
var profile_stats_http_registered int64
var profile_print_histogram bool

type Response map[string]interface{}

func (r Response) String() (s string) {
	b, err := json.Marshal(r)
	if err != nil {
		s = ""
		log.Println(err)
		return
	}
	s = string(b)
	return
}

/* disble  histogram

func (p *ProfileStats) retureHistogram(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Content-Type", "application/json")

	fmt.Fprint(rw, Response{"histogram": p.histogram_1ms})
	return
}

func (p *ProfileStats) retureHistogram2(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Content-Type", "application/json")

	fmt.Fprint(rw, Response{"histogram": p.histogram})
	return
}

func (p *ProfileStats) returnHistory(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Content-Type", "application/json")

	fmt.Fprint(rw, Response{"length": p.history_count, "history": p.history, "history_min": p.history_min, "history_max": p.history_max})
	// fmt.Fprint(rw, Response{"histogram": p.histogram})
	return
}

*/

// some constant
const one_millisec_in_nano = 1000000
const one_hundredth_secon_in_nano = one_millisec_in_nano / 10

var bar_length int //:= 120 // how many char max to print

type ProfileStats struct {
	totalReq int64
	lastReq  int64

	totalResponseTime int64
	Name              string // profile stats shall have a name, which shall be title for csv & print

	/* disable histogram

	min int64
	max int64

	// to record response faster than 1 ms, this will be double
	// count in the histogram[0] slots (1ms)
	// for now, this is just for counting purpose to figure out whether we need this
	histogram_1ms [2000]int     // 0-20ms
	histogram     [5000]int     // record in 1 ms accuracy, from 1 to 5 sec
	slowHistogram map[int64]int // this for everything takes longer than 5 sec

	// some helper to figure out what is the max for history gram
	max_1ms       int
	max_histogram int

	first_non_zero_1ms int
	last_non_zero_1ms  int

	m sync.Mutex
	*/

	initied bool

	/* disable histogram

	history       [2000]int64
	history_min   [2000]int64
	history_max   [2000]int64
	history_count int

	results chan int64

	results_2_history chan int64 // FIXME: not sure this this is a good idea to use chan here
	*/
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
	//// disable histogram |   p.m.Lock()

	if !p.initied {
		//// disable histogram |   p.max_1ms = 1
		//// disable histogram |   p.max_histogram = 1
		// go p.print()

		p.initied = true
	}

	p.totalResponseTime += t
	p.totalReq++

	/* disable histogram

	if t > p.max {
		p.max = t
	} else if t < p.min {
		p.min = t
	}

	// tt := time.Now()
	if profile_print_histogram {
		p.updateHistogram(t)
	}
	// fmt.Println("take ", time.Since(tt).Nanoseconds(), " to send result")
	p.m.Unlock()
	*/
}

/* disable histogram

func (p *ProfileStats) updateUI() {
	// a go routine to update UI
	var t int64
	p.results_2_history = make(chan int64, 100)

	go p.updateHistory()

	for true {
		t = <-p.results

		// first update histogram
		p.updateHistogram(t)
		// p.updateHistory(t)
		p.results_2_history <- t
	}
}

func (p *ProfileStats) updateHistogram(t int64) {
	// record histogram
	if t < one_hundredth_secon_in_nano*2000 { // less than 20ms
		i := (int)(t / one_hundredth_secon_in_nano)
		p.histogram[(int)(t/one_millisec_in_nano)]++

		// now update 1/100 th array
		p.histogram_1ms[i]++

		if p.histogram_1ms[i] > p.max_1ms {
			p.max_1ms = p.histogram_1ms[i]
		} else if (bar_length*p.histogram_1ms[i])/p.max_1ms > 0 {
			if i < p.first_non_zero_1ms {
				p.first_non_zero_1ms = i
			} else if i > p.last_non_zero_1ms {
				p.last_non_zero_1ms = i
			}
		}

	} else if t < 5000*one_millisec_in_nano {
		p.histogram[t/one_millisec_in_nano]++
		if p.histogram[t/one_millisec_in_nano] > p.max_histogram {
			p.max_histogram = p.histogram[t/one_millisec_in_nano]
		}

	} else {
		// v := t / (one_millisec_in_nano)
		// if _, ok := p.slowHistogram[v]; ok {
		// 	p.slowHistogram[v]++
		// } else {
		// 	p.slowHistogram[v] = 1
		// }
	}

	p.results_2_history <- t
}

func _min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func _max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func (p *ProfileStats) updateHistory() {
	var min, max, t int64
	var total_t, total_c int64

	controller := time.NewTicker(time.Second * time.Duration(1))

	for true {
		select {
		case <-controller.C:
			// update p.history

			if total_c > 0 {
				p.history_min[p.history_count] = min // to millisec
				p.history[p.history_count] = total_t / total_c
				p.history_max[p.history_count] = max

				// reset
				total_c = 0
				total_t = 0
				max = 0
				min = 50000000

				p.history_count++

				if p.history_count == 2000 {
					// now need collapse
					for i := 0; i < 1000; i++ {
						p.history[i] = (p.history[i*2] + p.history[i*2+1]) / 2
						p.history_min[i] = _min(p.history_min[i*2], p.history_min[i*2+1])
						p.history_max[i] = _max(p.history_max[i*2], p.history_max[i*2+1])
						p.history_count = 999
					}
				}
			}

		case t = <-p.results_2_history:
			// got value in t
			// record here
			total_t += t
			total_c += 1

			if t > max {
				max = t
			} else if t < min {
				min = t
			}
			// end of select
		}
	} // end of the infinite loop

	// this algorithm is not totally accurate, need improve to based on totalTime and count when collapse data range
}

func (p *ProfileStats) Init(udp int) {
	p.results = make(chan int64, 2000)
	p.first_non_zero_1ms = len(p.histogram_1ms)

	// FIXME: temp disabled
	go p.updateUI()

	s := os.Getenv("HAMMER_HISTOGRAM")

	if s != "" {
		t, err := strconv.Atoi(s)

		profile_print_histogram = true

		if err != nil {
			bar_length = 120
		} else {
			bar_length = t
		}
		go p.print()
	} else {
		profile_print_histogram = false
	}

	if atomic.AddInt64(&profile_stats_http_registered, 1) == 1 {
		http.HandleFunc("/histogram", p.retureHistogram)
		http.HandleFunc("/histogram2", p.retureHistogram2)
		http.HandleFunc("/history", p.returnHistory)
	}
}

func (p *ProfileStats) print() {
	// to print nicely

	// to print histogram

	time.Sleep(time.Second * 10)

	for true {
		x := 0
		t := len(p.histogram_1ms) - 1
		for ; t > 0; t-- {
			if (int)((bar_length*p.histogram_1ms[t])/p.max_1ms) > 2 {
				x = t
				// fmt.Println("found t as ", x, " and first_non_zero_1ms as ", p.first_non_zero_1ms)
				t = 0 // this is stupid.
			}
		}

		for i := p.first_non_zero_1ms; i < x; i++ {
			fmt.Printf("%3d | %15d %s\n", i, p.histogram_1ms[i], strings.Repeat("#", (bar_length*p.histogram_1ms[i])/p.max_1ms))
		}

		// fmt.Printf("\n\n")

		// for i := 0; i < 100; i++ {
		// 	fmt.Printf("%3d | %5d %s\n", i, p.histogram[i], strings.Repeat("#", 60*p.histogram[i]/p.max_histogram))
		// }

		time.Sleep(time.Second * 5)
	}
}

*/
