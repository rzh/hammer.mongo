package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"code.google.com/p/gcfg"
	"github.com/rzh/hammer.mongo/hammer"
)

var (
	config    string
	debug     bool
	gen       string
	initdb    bool
	max       bool
	monitor   int64
	profile   string
	quiet     bool
	rps       int64
	run       string // name of the test run
	server    string
	thread    int
	total     int64
	totaltime int64
	warmup    int64
	worker    int
	ssl bool
	sslca string // PEM file for the CA
	sslkey string // PEM key file for the client
)

type HammerConfig struct {
	Config struct {
		Debug     bool
		Initdb    bool
		Max       bool
		Monitor   int64
		Profile   string
		Quiet     bool
		Rps       int64
		Run       string
		Server    string
		Thread    int
		Total     int64
		Totaltime int64
		Warmup    int64
		Worker    int
	}
}

func parseConfig(cfgFile string) {
	var cfg HammerConfig

	// set up default value first
	cfg.Config.Debug = false
	cfg.Config.Initdb = false
	cfg.Config.Max = false
	cfg.Config.Monitor = 1
	cfg.Config.Profile = ""
	cfg.Config.Quiet = false
	cfg.Config.Rps = 500
	cfg.Config.Server = "localhost:27017"
	cfg.Config.Thread = 0
	cfg.Config.Total = 0
	cfg.Config.Totaltime = 0
	cfg.Config.Warmup = 0
	cfg.Config.Worker = 10

	// to parse config file
	err := gcfg.ReadFileInto(&cfg, cfgFile)
	if err != nil {
		log.Fatalf("Failed to parse configure file %s with error: %s", cfgFile, err)
	}

	// set config flags
	debug = cfg.Config.Debug
	initdb = cfg.Config.Initdb
	max = cfg.Config.Max
	monitor = cfg.Config.Monitor
	profile = cfg.Config.Profile
	quiet = cfg.Config.Quiet
	rps = cfg.Config.Rps
	run = cfg.Config.Run
	server = cfg.Config.Server
	thread = cfg.Config.Thread
	total = cfg.Config.Total
	totaltime = cfg.Config.Totaltime
	warmup = cfg.Config.Warmup
	worker = cfg.Config.Worker

	log.Printf("Configuration file %s parsed, runn with following configurations\n")

	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}

	os.Stdout.Write(b)
}

func init() {
	flag.BoolVar(&debug, "debug", false, "debug flag (true|false)")
	flag.BoolVar(&initdb, "initdb", false, "Drop DB before start testing")
	flag.BoolVar(&max, "max", false, "To find out Max")
	flag.Int64Var(&monitor, "monitor", 1, "Monitor interval")
	flag.StringVar(&profile, "profile", "", "to specify a traffic profile, all UPPERCASE")
	flag.BoolVar(&quiet, "quiet", false, "To silent monitor output")
	flag.Int64Var(&rps, "rps", 500, "Set Request Per Second, 0 to find Max possible")
	flag.StringVar(&run, "run", "", "To specify run id, used for archive test report")
	flag.StringVar(&server, "server", "localhost:27017", "Define server to be tested, default to localhost:27017")
	flag.IntVar(&thread, "thread", 0, "Number of system thread to be used")
	flag.Int64Var(&total, "total", 0, "Total request to be sent, default to unlimited (0)")
	flag.Int64Var(&warmup, "warmup", 0, "To set how long (seconds) for warmup DB")
	flag.IntVar(&worker, "worker", 10, "Number of workers, every worker will have two connections to mongodb")
	flag.Int64Var(&totaltime, "totaltime", 0, "To set how long (seconds) to run the test")
	flag.StringVar(&config, "config", "", "To use config file")
	flag.BoolVar(&ssl, "ssl", false, "enable SSL")
	flag.StringVar(&sslca, "sslCAFile", "", "PEM file for CA")
	flag.StringVar(&sslkey, "sslPEMKeyFile", "", "Key file for client")
	// flag.StringVar(&gen,       "gen",       "",                "To generate a sample configuration file")
}

func main() {
	flag.Parse()
	if config != "" {
		parseConfig(config)
	}

	NCPU := runtime.NumCPU()

	if thread != 0 {
		runtime.GOMAXPROCS(thread)
	} else {
		runtime.GOMAXPROCS(NCPU + 2) // two more thread than CPU core, need more research on best value TODO
	}

	hammer.Init(worker, monitor, server, initdb, profile, total, warmup, totaltime, quiet, run)

	if max {
		hammer.Start(0)
	} else {
		hammer.Start(rps)
	}

	var input string
	for true {
		fmt.Scanln(&input)

		// todo: make this response to input, to add some CLI options
	}
}
