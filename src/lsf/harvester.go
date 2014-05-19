package lsf

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

// boiler plate - sanity check
func init() {
	fmt.Println("lsf/prospector.go init")
}

// ----------------------------------------------------------------------
// constants and support types
// ----------------------------------------------------------------------
const (
	path_stdin               = "-"
	harvester_open_file_wait = 5 * time.Second
	open_file_retries        = 3
	harvester_idle_wait      = time.Second * 55
	harvester_read_wait      = time.Millisecond * 1000
)

// harvester initial offset seek mode.
type SeekMode int

const (
	SEEK_TAIL SeekMode = iota
	SEEK_CONTINUE
	SEEK_HEAD
	SEEK_NONE // for stdin
)

// ----------------------------------------------------------------------
// harvester
// ----------------------------------------------------------------------
type harvester struct {
	WorkerBase

	path   string
	fields map[string]string

	file     *os.File
	fileinfo *os.FileInfo
	offset   int64
	linenum  uint64
	mode     SeekMode
}

type Harvester interface {
	Worker
}

// ----------------------------------------------------------------------
// harvester API
// ----------------------------------------------------------------------
func NewHarvester(path string, init_offset int64, fields map[string]string, seekMode SeekMode) Harvester {

	name := nameFromPath(path)
	worker := &harvester{
		path:   path,
		fields: fields,
		offset: init_offset,
		mode:   seekMode,
	}
	worker.WorkerBase = NewWorkerBase(worker, name, harvest)

	return worker
}

func (w *harvester) Initialize() *WorkerErr {
	/// initialize ////////////////////////////////////
	var e error
	w.file, w.offset, e = openFile(w.path, w.offset, w.mode)
	if e != nil {
		return NewWorkerErrWithCause(E_INIT, "openFile", e)
	}

	fileinfo, e := w.file.Stat()
	if e != nil { // can only be a os.PathErr
		return NewWorkerErrWithCause(E_INIT, "file.Stat", e)
	}
	w.fileinfo = &fileinfo
	w.linenum = 0 // TODO: get last line num from Registrar
	/// initialize ////////////////////////////////////

	return nil
}

// ----------------------------------------------------------------------
// task
// ----------------------------------------------------------------------

// read lines from the file.
// sleep a bit for EOF
// stop if EOF persists beyond a limit
//func harvest(self interface{}, in <-chan *FileEvent, out chan<- *FileEvent, err chan<- *WorkerErr) {
func harvest(self interface{}, in0, out0 interface{}, err chan<- *WorkerErr) {

	w := self.(*harvester)
	out := out0.(chan<- *FileEvent)

	w.log("Begin harvesting %s at offset %d\n", w.path, w.offset)
	reader := bufio.NewReaderSize(w.file, 1024)
	deadline := time.Now().Add(harvester_idle_wait)

	var buff []byte
	for {
		select {
		case <-w.ctl_ch:
			msg := fmt.Sprintf("shutdown at offset %d", w.offset)
			w.log(msg)
			return
		default:
			segment, is_partial, e := reader.ReadLine()
			switch {
			case e == io.EOF:
				if time.Now().After(deadline) {
					msg := fmt.Sprintf("deadline expired at offset %d", w.offset)
					w.log(msg)
					err <- NewWorkerErr(E_TIMEOUT, msg)
					return
				}
				time.Sleep(harvester_read_wait)
			case e != nil:
				msg := fmt.Sprintf("ReadLine() error at offset %d", w.offset)
				w.log(msg)
				err <- NewWorkerErrWithCause(E_ERROR, "ReadLine error", e)
				return
			case is_partial:
				buff = append(buff, segment...)
				deadline = time.Now().Add(harvester_idle_wait)
			default:
				buff = append(buff, segment...)
				deadline = time.Now().Add(harvester_idle_wait)
				line := string(buff) // TODO
				buff = nil

				// process line
				w.linenum++
				event := &FileEvent{
					Source:   &w.path,
					Offset:   w.offset,
					Line:     w.linenum,
					Text:     &line,
					Fields:   &w.fields,
					fileinfo: w.fileinfo,
				}

				// emit
				select {
				case out <- event:
				case <-time.After(time.Millisecond):
					sndtimeout := time.Second // Config this
					timedout := sendFileEvent("harvest-out", event, out, sndtimeout, w.WorkerBase, err)
					if timedout {
						return
					}
				}

				w.offset, e = w.file.Seek(0, os.SEEK_CUR) // compensate for ReadLine() eating CR/LF
				// todo update registrar with new linenum
			}
		}
	}
}

// ----------------------------------------------------------------------
// support funcs
// ----------------------------------------------------------------------
func openFile(path string, offset int64, seekMode SeekMode) (file *os.File, xoff int64, e error) {
	// handle special case of harvesting stdin
	if path == path_stdin {
		return os.Stdin, 0, nil
	}

	file, e = openFileWaitRetry(path, harvester_open_file_wait, open_file_retries)
	if e != nil {
		return
	}

	// TODO(sissel): Only seek if the file is a file, not a pipe or socket.
	switch seekMode {
	case SEEK_HEAD:
		xoff, e = file.Seek(0, os.SEEK_SET)
	case SEEK_TAIL:
		xoff, e = file.Seek(0, os.SEEK_END)
	case SEEK_CONTINUE:
		//		fallthrough
		//	default:
		xoff, e = file.Seek(offset, os.SEEK_SET)
	}

	return
}

func openFileWaitRetry(path string, wait time.Duration, retries int) (file *os.File, e error) {
	for tries := 0; tries < retries; tries++ {
		file, e = os.Open(path)
		if e == nil {
			return file, nil
		}
		log.Printf("[harvester] Failed opening %s: %s", path, e)
		time.Sleep(wait)
	}
	return nil, e
}

func nameFromPath(path string) string {
	len := len(path)
	if len > 20 {
		len = 20
	}
	return fmt.Sprintf("harvester <%s>", path[:len])
}
