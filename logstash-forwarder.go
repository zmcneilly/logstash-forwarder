package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
/// global vars and consts
////////////////////////////////////////////////////////////////////////////////

const (
	no_profiling             = ""
	path_stdin               = "-"
	def_idle_timeout_secs    = 5 * time.Second
	cpu_profile_period_secs  = 6 * time.Second
	def_harverst_buffer_size = 16 << 10 /* harvester buffer size 16kb buffer by default */
	harvester_eof_timeout    = 1 * time.Second
)

// REVU: minimize globals TODO wrap these up in an immutable object
var cpu_profile_fname string
var max_spool_size uint64
var harvester_buffer_size int
var idle_timeout time.Duration
var config_fname string
var use_syslog bool
var seek_from_head bool

// parse command line flags
func init() {

	flag.StringVar(&config_fname, "config", "", "The config file to load (required)")
	flag.StringVar(&cpu_profile_fname, "cpuprofile", no_profiling, "write cpu profile to file")
	flag.Uint64Var(&max_spool_size, "spool-size", uint64(1024), "Maximum number of events to spool before a flush is forced")
	flag.IntVar(&harvester_buffer_size, "harvest-size", def_harverst_buffer_size, "harvester buffer size")
	flag.DurationVar(&idle_timeout, "idle-flush-time", def_idle_timeout_secs, "Maximum time to wait for a full spool before flushing anyway")
	flag.BoolVar(&use_syslog, "log-to-syslog", false, "Log to syslog instead of stdout")
	flag.BoolVar(&seek_from_head, "from-beginning", false, "Read new files from the beginning, instead of the end")
}

// should be called after flag.Parse() to assert specification of the required cmd-line arguments
// Calls log.Fatal on error
func checkRequiredFlags() {
	if config_fname == "" {
		flag.Usage()
		log.Fatal("configuration file not specified. will exit.")
	}
}

// check validity of the configuration parameters
// returns error on invalid configuration elements
// REVU: possibly belongns in config.go
func verifyConfig(config Config) error {
	if len(config.Files) == 0 {
		return fmt.Errorf("No paths given. What files do you want me to watch?")
	}
	return nil
}

// emit the important params the server process is using on startup
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

// server main.
// REVU: this is pretty much done.
func main() {

	// parse flags and emit the startup splash
	// enforce required cmd-line args
	flag.Parse()
	checkRequiredFlags()

	// create instance of lsfProcess
	this := newLSFProcess()

	// configure the process
	// sets the config parameter of lsfProcess
	this.configure(config_fname)

	// initialize all components
	this.initialize()

	// create and start active components
	// REVU: TODO these should be created in init ..
	this.startup()

	// shutdown on profile end OR os signal kill|interrupt
	switch {
	case this.isProfiling():
		// wait for end of profiler
		<-this.profiler_chan
		log.Printf("[main] Finished profiling - will exit\n\n\n\n\n\n\n\n\n")
	default:
		sig := <-this.interrupts // wait until interrupted
		log.Printf("[main] interrupted: %d", sig)
		log.Printf("[main] will exit.")
	}

	this.shutdown()

	return
}

////////////////////////////////////////////////////////////////////////////////
/// LSF process & main functionality
////////////////////////////////////////////////////////////////////////////////

// basic struct to hold various stateful components of the logstash-forwarder
// each instance corresponds to a unique lsf process
type lsfProcess struct {
	config      *Config
	registerar  *Registrar
	prospectors map[*Prospecter]time.Time

	profiler_chan  chan interface{} // if not nil, we're profiling
	event_chan     chan *FileEvent
	publisher_chan chan []*FileEvent
	registrar_chan chan []*FileEvent

	interrupts chan os.Signal
}

// instantiate an lsfProcess and create the necessary channels.
func newLSFProcess() *lsfProcess {
	return &lsfProcess{
		prospectors: make(map[*Prospecter]time.Time),
	}
}

// configure the lsfProcess process from the give
// config file.
func (l *lsfProcess) configure(filename string) {
	log.Println("[main] load configuration ..")
	config, e := LoadConfig(filename)
	if e != nil {
		log.Fatalf("[main] fatal error: %s - will exit.", e)
	}

	e = verifyConfig(*config)
	if e != nil {
		log.Fatalf("[main] fatal error: %s - will exit.", e)
	}

	// All is OK so now
	// set and create the necessary components
	l.config = config
	l.event_chan = make(chan *FileEvent, 16)
	l.publisher_chan = make(chan []*FileEvent, 1)
	l.registrar_chan = make(chan []*FileEvent, 1)
}

// returns true if we're running a cpu profile
func (l *lsfProcess) isProfiling() bool {
	return l.profiler_chan != nil
}

// initializes the logstash-forwarder instance
func (l *lsfProcess) initialize() {
	initsplash()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	if use_syslog {
		configureSyslog()
	}

	// set OS signal trap
	l.interrupts = make(chan os.Signal, 1)
	signal.Notify(l.interrupts, os.Interrupt, os.Kill)

}

// start logstash-forwarder active components
//
// The basic model of execution:
// - prospector: finds files in paths/globs to harvest, starts harvesters
// - harvester: reads a file, sends events to the spooler
// - spooler: buffers events until ready to flush to the publisher
// - publisher: writes to the network, notifies registrar
// - registrar: records positions of files read
// Finally, prospector uses the registrar information, on restart, to
// determine where in each file to resume a harvester.
//
func (lsf *lsfProcess) startup() {
	if cpu_profile_fname != no_profiling {
		lsf.runProfiler()
	}

	// Prospect the globs/paths given on the command line and launch harvesters
	for _, fileconfig := range lsf.config.Files {
		// TODO: use worker pattern
		log.Printf("[main] start prospector for %s ..\n", fileconfig.Paths)
		prospector := newProspecter(fileconfig, lsf.event_chan)
		lsf.prospectors[prospector] = time.Now()
		go prospector.run()
	}

	// Harvesters dump events into the spooler.
	// REVU: todo: find out how the shutdown behavior is supposed to look.
	// TODO: use worker pattern
	log.Println("[main] start spooler ..")
	go Spool(lsf.event_chan, lsf.publisher_chan, max_spool_size, idle_timeout)

	// TODO: use worker pattern
	log.Println("[main] start publisher ..")
	go Publishv1(lsf.publisher_chan, lsf.registrar_chan, &lsf.config.Network)

	// registrar records last acknowledged positions in all files.
	// TODO: use worker pattern
	log.Println("[main] start registerar ..")
	lsf.registerar = NewRegistrar(lsf.registrar_chan)
	go lsf.registerar.Run()

	log.Println("[main] logstash-forwarder started.")
}

// cleanly shutdown the server and active components
// REVU this is mostly TODO:
func (l *lsfProcess) shutdown() {
	log.Printf("[main] logstash-forwarder - shutting down ...")
	// shutdown sequence

	// shutdown prospectors
	log.Printf("[main] shutting down prospectors ...")
	for prospector, _ := range l.prospectors {
		prospector.CTL <- 0
	}
	// wait for them REVU: handle non-responsive cases
	for prospector, _ := range l.prospectors {
		<-prospector.SIG
	}

	// shutdown registerar
	log.Printf("[main]  shutting down registrar ...")
	l.registerar.CTL <- 1
	<-l.registerar.SIG
}

// starts the profiler go routine
// profiler is expected to complete in a specified finite time
func (l *lsfProcess) runProfiler() {
	l.profiler_chan = make(chan interface{})
	fn_profile, e := getCpuProfiler(cpu_profile_fname, l.profiler_chan)
	if e != nil {
		log.Fatalf("[main] fatal error: %s - will exit.", e)
	}
	go fn_profile()
}

////////////////////////////////////////////////////////////////////////////////
/// misc and support functions
////////////////////////////////////////////////////////////////////////////////

// return the func for go routine to run the profiler
// See REVU notes for issues
func getCpuProfiler(fname string, done chan interface{}) (func(), error) {

	f, e := os.Create(cpu_profile_fname)
	if e != nil {
		return nil, e
	}

	// REVU: this still starts profile before go on the wait so the time will not be precise
	// TODO: fix it (joubin)
	e = pprof.StartCPUProfile(f)
	if e != nil {
		return nil, e
	}

	fn := func() {
		log.Printf("[profiler] Begin profiling...")

		<-time.After(cpu_profile_period_secs)
		pprof.StopCPUProfile()

		log.Printf("[profiler] Finished profiling - will exit")
		done <- 0
	}
	return fn, nil
}
