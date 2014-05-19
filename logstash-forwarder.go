package main

import (
	"flag"
	"fmt"
	"log"
	"lsf"
	"math/rand"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
/// global vars and consts
////////////////////////////////////////////////////////////////////////////////

var options *lsf.Options // supposed to be readonly ..

// Note: Calls log.Fatal on error
func checkRequiredFlags() {
	if options.CONFIG_FNAME == "" {
		flag.Usage()
		log.Fatal("configuration file not specified. will exit.")
	}
}

func init() {
	// NOTE: this is a 'nil value' options instance
	// If you do not spec. a element of this struct via flags,
	// then you must explicitly set a value (or just set to the equiv in defaults)
	options = &lsf.Options{}

	// default values instance of Options struct
	defaults := lsf.Defaults

	flag.StringVar(&options.CONFIG_FNAME, "config", defaults.CONFIG_FNAME, "The config file to load (required)")
	flag.StringVar(&options.HOSTNAME, "hostname", defaults.HOSTNAME, "LS Host name")
	flag.StringVar(&options.CPU_PROFILE, "cpuprofile", defaults.CPU_PROFILE, "write cpu profile to file")
	flag.StringVar(&options.REGISTRAR_FILENAME, "registrar-path", defaults.REGISTRAR_FILENAME, "registrar db filename")
	flag.BoolVar(&options.USE_SYSLOG, "log-to-syslog", defaults.USE_SYSLOG, "Log to syslog instead of stdout")
	flag.DurationVar(&options.PROSPECTOR_SCAN_DELAY, "prospect-delay", defaults.PROSPECTOR_SCAN_DELAY, "period of scaning log paths")
	flag.IntVar(&options.SPOOL_BUFFER_SIZE, "spool-timeout", defaults.SPOOL_BUFFER_SIZE, "Maximum number of events to spool before a flush is forced")
	flag.DurationVar(&options.SPOOL_IDLE_TIMEOUT, "spool-size", defaults.SPOOL_IDLE_TIMEOUT, "Maximum time wait to spool before a flush is forced")
	flag.IntVar(&options.HARVESTER_BUFFER_SIZE, "harvest-size", defaults.HARVESTER_BUFFER_SIZE, "harvester buffer size")
	flag.DurationVar(&options.HARVESTER_IDLE_TIMEOUT, "idle-flush-time", defaults.HARVESTER_IDLE_TIMEOUT, "Maximum time to wait for a full spool before flushing anyway")
	flag.BoolVar(&options.HARVESTER_SEEK_FROM_HEAD, "from-beginning", defaults.HARVESTER_SEEK_FROM_HEAD, "Read new files from the beginning, instead of the end")

	rand.Seed(time.Now().UnixNano())
}

// logstash-forwarder server main
//
// starts a lsf.pipeline and then waits for
// shutdown via CTL-C or os signal
//
// If a CPU profile is run, the end of profiling
// signals a shutdown and program will terminate
//
func main() {

	flag.Parse()
	checkRequiredFlags()

	// error monitoring routine
	err := make(chan *lsf.WorkerErr, 16)
	go func() {
		for e := range err {
			fmt.Println("error_chan: " + e.Error())
		}
	}()

	// create and configure instance of lsf pipeline
	pipeline := lsf.NewPipeline(options)

	// initialize all components
	e := pipeline.Initialize()
	if e != nil {
		log.Printf("ERROR: %s", e)
	}

	// start the pipeline
	go pipeline.Work(nil, nil, err)

	// shutdown on profile end (if requested)
	// OR subscribe to os.Signal and shutdown on interrupts
	switch {
	case cpuProfileRequested():
		<-runProfiler() // wait till done
		log.Printf("[main] Finished profiling - will exit")
	default:
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, os.Interrupt, os.Kill)

		// response to os Signal (for CTL-c, etc.)
		interrupt := <-sigchan
		log.Printf("[main] interrupted (sig %d) - will shutdown", interrupt)
	}

	pipeline.Shutdown(time.Second)

	log.Println("Goodbye!")
}

////////////////////////////////////////////////////////////////////////////////
/// cpu profiling
////////////////////////////////////////////////////////////////////////////////

func cpuProfileRequested() bool {
	return options.CPU_PROFILE != lsf.Defaults.CPU_PROFILE
}

func runProfiler() chan interface{} {
	done := make(chan interface{})
	fn_profile, e := getCpuProfiler(options.CPU_PROFILE, done)
	if e != nil {
		log.Fatalf("[main] fatal error: %s - will exit.", e)
	}
	go fn_profile()

	return done
}

////////////////////////////////////////////////////////////////////////////////
/// misc and support functions
////////////////////////////////////////////////////////////////////////////////

const CPU_PROFILE_DURATION time.Duration = time.Second * 5

// return the func for go routine to run the profiler
// See REVU notes for issues
func getCpuProfiler(fname string, done chan interface{}) (func(), error) {

	f, e := os.Create(fname)
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

		<-time.After(CPU_PROFILE_DURATION)
		pprof.StopCPUProfile()

		log.Printf("[profiler] Finished profiling - will exit")
		done <- 0
	}
	return fn, nil
}
