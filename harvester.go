package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os" // for File and friends
	"time"
)

type Harvester struct {
	path   string
	fields map[string]string
	offset int64
	file   *os.File

	//	output chan<- *FileEvent
	ctl_ch chan int
	CTL    chan<- int
	sig_ch chan interface{}
	SIG    <-chan interface{}
}

const (
	read_timeout       = 10 * time.Second
	file_read_deadline = 24 * time.Hour
)

// create new Harvester
// output: channel to emit harvest FileEvent
// path: harvest file
// fields: file fields
func newHarvester(path string, init_offset int64, fields map[string]string) *Harvester {
	ctl_ch := make(chan int)
	sig_ch := make(chan interface{})
	return &Harvester{
		path:   path,
		fields: fields,
		offset: init_offset,

		//		output: output,
		ctl_ch: ctl_ch,
		CTL:    ctl_ch,
		sig_ch: sig_ch,
		SIG:    sig_ch,
	}
}

// run harvester at offset. Also see HarvestAtOffset(int64)
func (h *Harvester) Run(inport <-chan *FileEvent, outport chan<- *FileEvent, errport chan<- error) {

	loginfo := "" // extra info for offset harvesters is non-nil
	if h.offset > 0 {
		loginfo = fmt.Sprintf("at postion %d", h.offset)
	}
	log.Printf("[harvester] Harvesting file %s %s\n", h.path, loginfo)

	// REVU: who closes this?
	h.open()

	info, err := h.file.Stat()
	if err != nil { // can only be a os.PathErr
		log.Printf("[harvester] unexpected error reading %s - %s", h.path, err)
		//		h.sig_ch<- err // REVU: should post its error - TODO: wire this stuff up
		return
	}
	defer h.file.Close()

	var line uint64 = 0 // Ask registrar about the line number

	// get current offset in file
	offset, _ := h.file.Seek(0, os.SEEK_CUR)

	log.Printf("[harvester] Current file offset: %d\n", offset)

	// TODO(sissel): Make the buffer size tunable at start-time
	reader := bufio.NewReaderSize(h.file, harvester_buffer_size)

	var deadline time.Time
	last_read_time := time.Now()
	for {
		text, err := h.readline(reader)
		switch err {
		case io.EOF: // timed out waiting for data, got eof.
			// Check to see if the file was truncated
			info, _ := h.file.Stat()
			if info.Size() < offset {
				log.Printf("[harvester] File truncated, seeking to beginning: %s\n", h.path)
				h.file.Seek(0, os.SEEK_SET)
				offset = 0
			} else if time.Now().After(deadline) {
				// assume file is probably dead. Stop harvesting it
				log.Printf("[harvester] Stopping harvest of %s; last change was %d seconds ago\n", h.path, time.Now().Sub(last_read_time))
				return
			}
			continue
		default: // unexpected error
			log.Printf("[harvester] Unexpected state reading from %s; error: %s\n", h.path, err)
			return
		}
		last_read_time = time.Now()
		deadline = last_read_time.Add(file_read_deadline)

		line++
		event := &FileEvent{
			Source:   &h.path,
			Offset:   offset,
			Line:     line,
			Text:     text,
			Fields:   &h.fields,
			fileinfo: &info,
		}
		offset += int64(len(*event.Text)) + 1 // +1 because of the line terminator - todo revu for all os

		outport <- event // ship the new event downstream

		// REVU: all active components should do this. todo
		// poll ctl channel for directive
		select {
		case <-h.ctl_ch:
			// TODO: read the ctl code and differentiate between STAT and SHUTDOWN
			// REVU: for now input on ctl channel interpreted as stop
			log.Printf("[harvester] shutdown event - will exit")
			h.sig_ch <- "exit"
			break
		default:
		}
	} /* forever */
}

func (h *Harvester) open() *os.File {
	// handle special case of harvesting stdin
	if h.path == path_stdin {
		h.file = os.Stdin
		return h.file
	}

	for {
		var err error
		h.file, err = os.Open(h.path)

		if err != nil {
			// retry on failure.
			log.Printf("[harvester] Failed opening %s: %s\n", h.path, err)
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	// TODO(sissel): Only seek if the file is a file, not a pipe or socket.
	if h.offset > 0 {
		h.file.Seek(h.offset, os.SEEK_SET)
	} else if seek_from_head {
		h.file.Seek(0, os.SEEK_SET)
	} else {
		h.file.Seek(0, os.SEEK_END)
	}

	return h.file
}

// attempts to read and return a line from harvested file.
// error is raised on EOF timeout or general io error.
func (h *Harvester) readline(reader *bufio.Reader) (*string, error) {

	deadline := time.Now().Add(read_timeout)

	var buffer bytes.Buffer
	for {
		segment, is_partial, err := reader.ReadLine()
		switch err {
		case io.EOF:
			time.Sleep(harvester_eof_timeout) // TODO(sissel): Implement backoff

			// Give up waiting for data after a certain amount of time.
			// If we time out, return the error (eof)
			if time.Now().After(deadline) {
				log.Println("[harvester] Harvester timeout reading line")
				return nil, err
			}
			continue
		default:
			log.Println(err)
			return nil, err // TODO(sissel): don't do this?
		}

		// TODO(sissel): if buffer exceeds a certain length, maybe report an error condition? chop it?
		buffer.Write(segment)

		if !is_partial {
			// If we got a full line, return the whole line.
			str := buffer.String()
			return &str, nil
		}
	} /* until full line read or err/timeout */

	return nil, nil
}
