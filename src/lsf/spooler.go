package lsf

import (
	"fmt"
	"time"
)

// boiler plate - sanity check
func init() {
	fmt.Println("lsf/spooler.go init")
}

// ----------------------------------------------------------------------
// constants and support types
// ----------------------------------------------------------------------

// ----------------------------------------------------------------------
// spooler
// ----------------------------------------------------------------------
type spooler struct {
	WorkerBase

	timeout time.Duration
	size    int
}
type Spooler interface {
	Worker
}

// ----------------------------------------------------------------------
// spooler API
// ----------------------------------------------------------------------
func NewSpooler(timeout time.Duration, size int) Spooler {
	worker := &spooler{
		timeout: timeout,
		size:    size,
	}
	worker.WorkerBase = NewWorkerBase(worker, "spooler", spool)
	return worker
}

func (w *spooler) Initialize() *WorkerErr {
	/* nothing to do */
	return nil
}

// ----------------------------------------------------------------------
// task
// ----------------------------------------------------------------------
func spool(self interface{}, in0, out0 interface{}, err chan<- *WorkerErr) {

	s := self.(*spooler)
	in := in0.(<-chan *FileEvent)
	out := out0.(chan<- []*FileEvent)

	s.log("size: %d", s.size)
	spool := make([]*FileEvent, s.size)
	idx := -1

	timer := time.NewTimer(s.timeout)
	for {
		flush := false
		select {
		case <-s.ctl_ch:
			s.log("shutdown command rcvd")
			return
		case <-timer.C:
			if idx >= 0 {
				flush = true
				// time to flush
			}
			timer = time.NewTimer(s.timeout) // flushed or not, we reset the timer
		case event, ok := <-in:
			switch {
			case !ok:
			case event == nil:
			default:
				s.log("event @ idx %d len %d cap %d", idx, len(spool), cap(spool))
				// spool the event
				idx++
				spool[idx] = event
				if idx == s.size {
					// time to flush
					flush = true
				}
			}
		}

		if flush {
			s.log("flushing events: %d", idx+1)
			// copy the spool data
			production := make([]*FileEvent, idx+1)
			copy(production, spool)
			idx = -1

			// send it
			sndtimeout := time.Second // Config this
			timedout := sendFileEventArray("spooler-out", production, out, sndtimeout, s.WorkerBase, err)
			if timedout {
				// TODO: err<- ... log()
				return
			}
			flush = false // not really necessary - being explicit
		}
	}
}
