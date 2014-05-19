package main

import (
	"log"
	"time"
)

// todo: document this
// todo: add shutdown event/hook
// todo: see if can be consolidated with Publisher event life-cycle
// todo: findout why we are using array of events on output - (double buffering? why?)

type Spooler struct {
	idle_timeout time.Duration
	max_size     uint64

	ctl_ch chan int
	CTL    chan<- int
	sig_ch chan interface{}
	SIG    <-chan interface{}
}

func NewSpooler(timeout time.Duration, size uint64) *Spooler {
	ctl_ch := make(chan int)
	sig_ch := make(chan interface{})
	return &Spooler{
		idle_timeout: timeout,
		max_size:     size,

		//		output: output,
		ctl_ch: ctl_ch,
		CTL:    ctl_ch,
		sig_ch: sig_ch,
		SIG:    sig_ch,
	}
}

// REVU: todo make this interruptible
func (s *Spooler) Run(inport chan *FileEvent, outport chan []*FileEvent, errport chan<- error) {

	// heartbeat periodically. If the last flush was longer than
	// 'idle_timeout' time ago, then we'll force a flush to prevent us from
	// holding on to spooled events for too long.

	timeout := s.idle_timeout

	// slice for spooling into
	// TODO(sissel): use container.Ring?
	spool := make([]*FileEvent, s.max_size)

	// Current write position in the spool
	var spool_i int = 0
	for {
		select {
		case <-s.ctl_ch:
			// REVU: todo: find out how the shutdown behavior is supposed to look.
			log.Printf("[spooler] shutdown event - will exit")
			s.sig_ch <- "exit"
			return
		case event := <-inport:
			//append(spool, event)
			spool[spool_i] = event
			spool_i++

			// Flush if full
			if spool_i == cap(spool) {
				//spoolcopy := make([]*FileEvent, max_size)
				var spoolcopy []*FileEvent
				spoolcopy = append(spoolcopy, spool[:]...)
				outport <- spoolcopy
				spool_i = 0
			}
		case <-time.After(timeout):
			// Flush what we have, if anything
			if spool_i > 0 {
				var spoolcopy []*FileEvent
				spoolcopy = append(spoolcopy, spool[0:spool_i]...)
				outport <- spoolcopy
				spool_i = 0
			}
		}
	}
}

func flushSpool() {
	// todo: (joubin)
}
