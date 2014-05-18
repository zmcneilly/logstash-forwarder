package main

import (
	"log"
)

type Registrar struct {
	input <-chan []*FileEvent

	ctl_ch chan int
	CTL    chan<- int
	sig_ch chan interface{}
	SIG    <-chan interface{}
}

func NewRegistrar(input <-chan []*FileEvent) *Registrar {
	ctl_ch := make(chan int)
	sig_ch := make(chan interface{})
	return &Registrar{
		input:  input,
		ctl_ch: ctl_ch,
		CTL:    ctl_ch,
		sig_ch: sig_ch,
		SIG:    sig_ch,
	}
}

func (r *Registrar) Run() {
	log.Printf("[registerar] Registrar started.\n")

	state := make(map[string]*FileState)
	for {
		select {
		/* quit on control signal */
		case code, _ := <-r.ctl_ch:
			log.Printf("[registerar] exit on control-code:%d\n", code)
			r.sig_ch <- "exit"
			return
		/* wait for events to process */
		case events, _ := <-r.input:
			log.Printf("[registerar] Registrar received %d events\n", len(events))
			// Take the last event found for each file source
			for _, event := range events {
				// skip stdin sources
				if *event.Source == path_stdin {
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
					Offset: event.Offset + int64(len(*event.Text)) + 1,
					Inode:  ino,
					Device: dev,
				}
				//log.Printf("State %s: %d\n", *event.Source, event.Offset)
			}

			if len(state) > 0 {
				WriteRegistry(state, ".logstash-forwarder")
			}
		}
	}
}
