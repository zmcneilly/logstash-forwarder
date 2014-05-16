package main

import (
	"time"
)

// revu: (joubin)
// todo: document this
// todo: add shutdown event/hook
// todo: see if can be consolidated with Publisher event life-cycle
// todo: findout why we are using array of events on output - (double buffering? why?)
func Spool(input chan *FileEvent, output chan []*FileEvent, max_size uint64, idle_timeout time.Duration) {

	// heartbeat periodically. If the last flush was longer than
	// 'idle_timeout' time ago, then we'll force a flush to prevent us from
	// holding on to spooled events for too long.

	timeout := idle_timeout

	// slice for spooling into
	// TODO(sissel): use container.Ring?
	spool := make([]*FileEvent, max_size)

	// Current write position in the spool
	var spool_i int = 0
	for {
		select {
		case event := <-input:
			//append(spool, event)
			spool[spool_i] = event
			spool_i++

			// Flush if full
			if spool_i == cap(spool) {
				//spoolcopy := make([]*FileEvent, max_size)
				var spoolcopy []*FileEvent
				spoolcopy = append(spoolcopy, spool[:]...)
				output <- spoolcopy
				spool_i = 0
			}
		case <-time.After(timeout):
			// Flush what we have, if anything
			if spool_i > 0 {
				var spoolcopy []*FileEvent
				spoolcopy = append(spoolcopy, spool[0:spool_i]...)
				output <- spoolcopy
				spool_i = 0
			}
		}
	}
}

func flushSpool() {
	// todo: (joubin)
}
