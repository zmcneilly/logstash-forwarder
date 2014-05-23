package lsf

import (
	"fmt"
	"log"
	"time"
)

func init() {
	fmt.Printf("lsf/process.go init()")
}

// ----------------------------------------------------------------------
// pipeline
// ----------------------------------------------------------------------

// pipeline supports Worker and embeds WorkerBase just like its components
//
type Pipeline struct {
	WorkerBase

	options     *Options
	config      *Config
	registrar   Registrar
	spooler     Spooler
	prospectors map[Prospecter]time.Time
	publisher   Publisher

	profiler_chan  chan interface{} // if not nil, we're profiling
	event_chan     chan *FileEvent
	publisher_chan chan []*FileEvent
	registrar_chan chan []*FileEvent
}

// New pipeline configured per config file at filename
func NewPipeline(options *Options) *Pipeline {
	pipeline := &Pipeline{
		options:     options,
		prospectors: make(map[Prospecter]time.Time),
	}
	pipeline.WorkerBase = NewWorkerBase(pipeline, "pipeline", process)

	pipeline.configure(options.CONFIG_FNAME)

	return pipeline
}

// ----------------------------------------------------------------------
// pipeline API
// ----------------------------------------------------------------------

// per Worker
// initializes the Pipeline process
//
// - add OS signal hooks (os.Interrupt, os.Kill)
// - create & initialize a Prospector for each config.Files path
//
// returns initialization error. if not nil, worker should not be used.
func (p *Pipeline) Initialize() (inierror *WorkerErr) {

	defer func() {
		if re := recover(); re != nil {
			cause := fmt.Errorf("(recovered) %s", re)
			inierror = NewWorkerErrWithCause(E_INIT, "Pipeline#Initialize()", cause)
		}
	}()
	options := p.options

	// set log format
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	if options.USE_SYSLOG {
		configureSyslog()
	}

	// splash options
	options.display()

	// create components

	// each path in config.files has a dedicated harvester
	// create but do not start them
	for _, fileconfig := range p.config.Files {
		p.log("initialize prospector for %s ..\n", fileconfig.Paths)
		prospector := NewProspecter(fileconfig, options.PROSPECTOR_SCAN_DELAY)
		if e := prospector.Initialize(); e != nil {
			return NewWorkerErrWithCause(E_INIT, "failed to initialize prospector", e)
		}
		p.prospectors[prospector] = time.Now()
	}

	// create the Spooler
	p.log("initialize spooler ..")
	spooler := NewSpooler(options.SPOOL_IDLE_TIMEOUT, options.SPOOL_BUFFER_SIZE)
	if e := spooler.Initialize(); e != nil {
		return NewWorkerErrWithCause(E_INIT, "failed to initialize spooler", e)
	}
	p.spooler = spooler

	// create the Publisher
	p.log("initialize publisher ..")
	publisher := NewPublisher(&p.config.Network, options.HOSTNAME)
	if e := publisher.Initialize(); e != nil {
		return NewWorkerErrWithCause(E_INIT, "failed to initialize pubisher", e)
	}
	p.publisher = publisher

	// create the Registrar
	p.log("initialize registrar ..")
	registrar := NewRegistrar(options.REGISTRAR_FILENAME)
	if e := registrar.Initialize(); e != nil {
		return NewWorkerErrWithCause(E_INIT, "failed to initialize registrar", e)
	}
	p.registrar = registrar

	p.log("initialized.")

	return nil
}

// per Worker
// shutdown in sequence
// - prospectors (these will shutdown their harvesters)
// - spooler
// - publisher
// - registrar
// close all channels at shutdown completion
func (p *Pipeline) Shutdown(wait time.Duration) {

	p.log("shutting down ...")
	for prospector, _ := range p.prospectors {
		p.log("shutting down prospector")
		//		log.Printf("[pipeline] shutdown prospector\n")
		prospector.Shutdown(wait)
	}

	p.log("shutting down spooler")
	p.spooler.Shutdown(wait)

	p.log("shutting down publisher")
	p.publisher.Shutdown(wait)

	p.log("shutting down registrar")
	p.registrar.Shutdown(wait)

	p.WorkerBase.Shutdown(wait)

	if p.profiler_chan != nil {
		close(p.profiler_chan)
	}
	if p.event_chan != nil {
		close(p.event_chan)
	}
	if p.publisher_chan != nil {
		close(p.publisher_chan)
	}
	if p.registrar_chan != nil {
		close(p.registrar_chan)
	}
}

func (p *Pipeline) IsProfiling() bool {
	return p.profiler_chan != nil
}

// ----------------------------------------------------------------------
// pipeline ops
// ----------------------------------------------------------------------

// The managed task of the Pipeline
//
// function simply needs to start each of the pipeline components and
// hook them up via their in/out channels
//
// this is a short-lived goroutine/task
func process(self interface{}, in0, out0 interface{}, err chan<- *WorkerErr) {

	p := self.(*Pipeline)

	// TODO: profiling

	p.log("start prospectors ...\n")
	for prospector, _ := range p.prospectors {
		p.log("start prospector\n")
		var harvest_out chan<- *FileEvent = p.event_chan // being pedantic
		go prospector.Work(nil, harvest_out, err)
	}

	p.log("start spooler\n")
	var spool_in <-chan *FileEvent = p.event_chan        // being pedantic
	var spool_out chan<- []*FileEvent = p.publisher_chan // being pedantic
	go p.spooler.Work(spool_in, spool_out, err)

	p.log("start publisher\n")
	var pub_in <-chan []*FileEvent = p.publisher_chan  // being pedantic
	var pub_out chan<- []*FileEvent = p.registrar_chan // being pedantic
	go p.publisher.Work(pub_in, pub_out, err)

	p.log("start registrar\n")
	var registrar_in <-chan []*FileEvent = p.registrar_chan // being pedantic
	go p.registrar.Work(registrar_in, nil, err)

	p.log("pipeline activated and working.")
}

// ----------------------------------------------------------------------
// pipeline configuration
// ----------------------------------------------------------------------
// configure the Pipeline process using the configuration (filename)
func (p *Pipeline) configure(filename string) {

	options := p.options

	// set log format
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	if options.USE_SYSLOG {
		configureSyslog()
	}

	log.Println("[pipeline] load configuration ..")
	config, e := LoadConfig(filename)
	if e != nil {
		log.Fatalf("[pipeline] fatal error: %s - will exit.", e)
	}

	e = verifyConfig(*config)
	if e != nil {
		log.Fatalf("[pipeline] fatal error: %s - will exit.", e)
	}

	// All is OK so now
	// set and create the necessary components
	p.config = config
	p.event_chan = make(chan *FileEvent, 16)
	p.publisher_chan = make(chan []*FileEvent, 1)
	p.registrar_chan = make(chan []*FileEvent, 1)

	log.Println("[pipeline] configured")

}

// check validity of the configuration parameters
// returns error on invalid configuration elements
// REVU: possibly belongns in config.go
func verifyConfig(config Config) error {
	if len(config.Files) == 0 {
		return fmt.Errorf("No paths given. What files do you want me to watch?")
	}
	log.Println("[pipeline] verified configuration")
	return nil
}

// ----------------------------------------------------------------------
// generic pipeline component support funcs
// ----------------------------------------------------------------------

// send an event to the channel.
// apply timeout.
// create send error and send to err channel.
// log as worker
// returns true if send was not successful
func sendFileEvent(logical_channel string, event *FileEvent, out chan<- *FileEvent, timeout time.Duration, worker WorkerBase, err chan<- *WorkerErr) (timedout bool) {
	select {
	case out <- event:
	case <-time.After(timeout):
		msg := fmt.Sprintf("timeout on send to out (%s)", logical_channel)
		e := NewWorkerErr(E_SEND_BLOCK, msg)
		worker.log(msg)
		err <- e
		timedout = true
	}
	return
}

// send an event array to the channel.
// apply timeout.
// create send error and send to err channel.
// log as worker
// returns true if send was not successful
func sendFileEventArray(logical_channel string, events []*FileEvent, out chan<- []*FileEvent, timeout time.Duration, worker WorkerBase, err chan<- *WorkerErr) (timedout bool) {
	select {
	case out <- events:
		worker.log("sent []*FileEvent cnt: %d", len(events))
	case <-time.After(timeout):
		msg := fmt.Sprintf("timeout on send to out (%s)", logical_channel)
		e := NewWorkerErr(E_SEND_BLOCK, msg)
		worker.log(msg)
		err <- e
		timedout = true
	}
	return
}
