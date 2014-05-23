package lsf

import (
	"fmt"
)

// boiler plate - sanity check
func init() {
	fmt.Println("lsf/registrar.go init")
}

// ----------------------------------------------------------------------
// constants and support types
// ----------------------------------------------------------------------

// ----------------------------------------------------------------------
// registrar
// ----------------------------------------------------------------------
type registrar struct {
	WorkerBase

	filename string
}
type Registrar interface {
	Worker
}

// ----------------------------------------------------------------------
// registrar API
// ----------------------------------------------------------------------
func NewRegistrar(filename string) Registrar {
	worker := &registrar{
		filename: filename,
	}
	worker.WorkerBase = NewWorkerBase(worker, "registrar", register)
	return worker
}

func (w *registrar) Initialize() *WorkerErr {
	/// initialize ////////////////////////////////////
	/// initialize ////////////////////////////////////
	return nil
}

// ----------------------------------------------------------------------
// task
// ----------------------------------------------------------------------
func register(self interface{}, in0, out0 interface{}, err chan<- *WorkerErr) {

	w := self.(*registrar)
	in := in0.(<-chan []*FileEvent)

	// registry table
	state := make(map[string]*FileState)

	w.log("Registrar working ..")

	for {
		select {
		case <-w.ctl_ch: // worker interrupt
			msg := fmt.Sprintf("shutdown recieved")
			w.log(msg)
			return
		case events, ok := <-in:
			if !ok {
				// handle closed pipe -- shutdown?
				w.sig_ch <- "fault on <-in"
				return
			}

			// process published events

			dirty := false
			for _, eptr := range events {
				w.log("on event ..")
				event := *eptr // for convenience

				// skip stdin sources
				if *event.Source == path_stdin {
					w.log("ignoring event from Stdin: %s", *event.Text)
					continue
				}

				// have to dereference the FileInfo here because os.FileInfo is an
				// interface, not a struct, so Go doesn't have smarts to call the Sys()
				// method on a pointer to os.FileInfo. :(
				ino, dev := file_ids(event.fileinfo)
				state[*event.Source] = &FileState{
					Source: event.Source,
					// take the offset + length of the line + newline char and
					// save it as the new starting offset.
//					Offset: event.Offset + int64(len(*event.Text)) + 1, // REVU this is a BUG TODO
					Offset: event.Offset + int64(len(*event.Text)),
					Inode:  ino,
					Device: dev,
				}
				dirty = true
				w.log("registry updated for %s", *event.Source)
			}

			// save state
			if dirty {
				if e := WriteRegistry(state, w.filename); e != nil {
					e0 := NewWorkerErrWithCause(E_ERROR, "on WriteRegistry", e)
					err <- e0
					w.log("fault on registry write. shutting down")
					return
				}
				w.log("registry saved")
				dirty = false // not necessary - being explicit
			}
		}
	}
}
