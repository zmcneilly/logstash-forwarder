package main

import (
	"bufio"
	"bytes"
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

	output chan *FileEvent
}

func newHarvester(path string, fields map[string]string) *Harvester {
	return newHarvesterAtOffset(path, fields, 0)
}
func newHarvesterAtOffset(path string, fields map[string]string, offset int64) *Harvester {
	return &Harvester{path: path, fields: fields, offset: offset}
}

// Harvester.Harverst
// reads a file, sends events to the spooler
// via output channel.
func (h *Harvester) Harvest(output chan *FileEvent) {
	if h.offset > 0 {
		log.Printf("Starting harvester at position %d: %s\n", h.offset, h.path)
	} else {
		log.Printf("Starting harvester: %s\n", h.path)
	}

	h.open()
	info, _ := h.file.Stat() // TODO(sissel): Check error
	defer h.file.Close()
	//info, _ := file.Stat()

	var line uint64 = 0 // Ask registrar about the line number

	// get current offset in file
	offset, _ := h.file.Seek(0, os.SEEK_CUR)

	log.Printf("Current file offset: %d\n", offset)

	// TODO(sissel): Make the buffer size tunable at start-time
	reader := bufio.NewReaderSize(h.file, harvester_buffer_size)

	var read_timeout = 10 * time.Second
	last_read_time := time.Now()
	for {
		text, err := h.readline(reader, read_timeout)

		if err != nil {
			if err == io.EOF {
				// timed out waiting for data, got eof.
				// Check to see if the file was truncated
				info, _ := h.file.Stat()
				if info.Size() < offset {
					log.Printf("File truncated, seeking to beginning: %s\n", h.path)
					h.file.Seek(0, os.SEEK_SET)
					offset = 0
				} else if age := time.Since(last_read_time); age > (24 * time.Hour) {
					// if last_read_time was more than 24 hours ago, this file is probably
					// dead. Stop watching it.
					// TODO(sissel): Make this time configurable
					// This file is idle for more than 24 hours. Give up and stop harvesting.
					log.Printf("Stopping harvest of %s; last change was %d seconds ago\n", h.path, age.Seconds())
					return
				}
				continue
			} else {
				log.Printf("Unexpected state reading from %s; error: %s\n", h.path, err)
				return
			}
		}
		last_read_time = time.Now()

		line++
		event := &FileEvent{
			Source:   &h.path,
			Offset:   offset,
			Line:     line,
			Text:     text,
			Fields:   &h.fields,
			fileinfo: &info,
		}
		offset += int64(len(*event.Text)) + 1 // +1 because of the line terminator

		output <- event // ship the new event downstream
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
			log.Printf("Failed opening %s: %s\n", h.path, err)
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

func (h *Harvester) readline(reader *bufio.Reader, eof_timeout time.Duration) (*string, error) {
	var buffer bytes.Buffer
	start_time := time.Now()
	for {
		segment, is_partial, err := reader.ReadLine()

		if err != nil {
			if err == io.EOF {
				time.Sleep(1 * time.Second) // TODO(sissel): Implement backoff

				// Give up waiting for data after a certain amount of time.
				// If we time out, return the error (eof)
				if time.Since(start_time) > eof_timeout {
					return nil, err
				}
				continue
			} else {
				log.Println(err)
				return nil, err // TODO(sissel): don't do this?
			}
		}

		// TODO(sissel): if buffer exceeds a certain length, maybe report an error condition? chop it?
		buffer.Write(segment)

		if !is_partial {
			// If we got a full line, return the whole line.
			str := new(string)
			*str = buffer.String()
			return str, nil
		}
	} /* forever read chunks */

	return nil, nil
}
