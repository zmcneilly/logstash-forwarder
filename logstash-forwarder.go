package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

const no_profiling = ""
const def_idle_timeout_secs = 5 * time.Second
const cpu_profile_period_secs = 60 * time.Second

// harvester buffer size 16kb buffer by default
const def_harverst_buffer_size = 16 << 10

var cpu_profile_fname string
var max_spool_size uint64
var harvester_buffer_size int
var idle_timeout time.Duration
var config_fname string
var use_syslog bool
var seek_from_head bool

func init() {

	flag.StringVar(&config_fname, "config", "", "The config file to load (required)")
	flag.StringVar(&cpu_profile_fname, "cpuprofile", no_profiling, "write cpu profile to file")
	flag.Uint64Var(&max_spool_size, "spool-size", uint64(1024), "Maximum number of events to spool before a flush is forced")
	flag.IntVar(&harvester_buffer_size, "harvest-size", def_harverst_buffer_size, "harvester buffer size")
	flag.DurationVar(&idle_timeout, "idle-flush-time", def_idle_timeout_secs, "Maximum time to wait for a full spool before flushing anyway")
	flag.BoolVar(&use_syslog, "log-to-syslog", false, "Log to syslog instead of stdout")
	flag.BoolVar(&seek_from_head, "from-beginning", false, "Read new files from the beginning, instead of the end")
}

func checkRequiredFlags() {
	if config_fname == "" {
		flag.Usage()
		log.Fatal("configuration file not specified. will exit.")
	}
}

func verifyConfig(config Config) error {
	if len(config.Files) == 0 {
		return fmt.Errorf("No paths given. What files do you want me to watch?")
	}
	return nil
}

func initsplash() {
	log.Println("logstash-forwarder initialzing ...")
	log.Printf("\tconfig-file:         <%s>", config_fname)
	log.Printf("\thostname:            <%s>", hostname)
	log.Printf("\tmax-spool-size:      <%d>", max_spool_size)
	log.Printf("\tidle timeout (msec): <%d>", idle_timeout/time.Millisecond)
	log.Printf("\tusing syslog:        <%t>", use_syslog)
	log.Printf("\tscan files from end: <%t>", !seek_from_head)
	log.Println()
}

func main() {

	// parse flags and emit the startup splash
	// enforce required cmd-line args

	flag.Parse()

	checkRequiredFlags()

	// load and very configuration

	config, e := LoadConfig(config_fname)
	if e != nil {
		log.Fatalf("fatal error: %s - will exit.", e)
	}

	e = verifyConfig(*config)
	if e != nil {
		log.Fatalf("fatal error: %s - will exit.", e)
	}

	/* initialize */

	initsplash()

	// REVU: check semantics of nil event to signal shutdown
	// TODO (joubin)
	event_chan := make(chan *FileEvent, 16) // REVU: magic number ? todo: find out (joubin)
	publisher_chan := make(chan []*FileEvent, 1)
	registrar_chan := make(chan []*FileEvent, 1)

	if cpu_profile_fname != no_profiling {
		f, err := os.Create(cpu_profile_fname)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		go func() {
			defer func() {
				// todo: find out if emitting log would be ok to signal profile end.(joubin)
			}()

			<-time.After(cpu_profile_period_secs)
			pprof.StopCPUProfile()

			panic("done") // REVU don't understand &| like panics in go routines. todo: fix (joubin)
		}()
	}

	// The basic model of execution:
	// - prospector: finds files in paths/globs to harvest, starts harvesters
	// - harvester: reads a file, sends events to the spooler
	// - spooler: buffers events until ready to flush to the publisher
	// - publisher: writes to the network, notifies registrar
	// - registrar: records positions of files read
	// Finally, prospector uses the registrar information, on restart, to
	// determine where in each file to resume a harvester.

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	if use_syslog {
		configureSyslog()
	}

	/* start */

	// Prospect the globs/paths given on the command line and launch harvesters
	for _, fileconfig := range config.Files {
		go Prospect(fileconfig, event_chan)
	}

	// Harvesters dump events into the spooler.
	// REVU: todo: find out how the shutdown behavior is supposed to look.
	go Spool(event_chan, publisher_chan, max_spool_size, idle_timeout)

	go Publishv1(publisher_chan, registrar_chan, &config.Network)

	// registrar records last acknowledged positions in all files.
	Registrar(registrar_chan)

	log.Println("logstash-forwarder started.")

	return
} /* main */
