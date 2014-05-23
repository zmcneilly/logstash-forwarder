package lsf

import (
	//	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// boiler plate
func init() {
	fmt.Println("lsf/prospecter.go init")
}

// ----------------------------------------------------------------------
// constants and support types
// ----------------------------------------------------------------------

// ----------------------------------------------------------------------
// prospecter
// ----------------------------------------------------------------------

// REVU: instantiate one per tracked file.
// TODO: move HarvestMode here
type TrackingInfo struct {
	fileinfo  map[string]os.FileInfo
	harvester Harvester
}

type prospecter struct {
	WorkerBase

	fileconfig FileConfig
	fileinfo   map[string]os.FileInfo
	harvesters map[Harvester]time.Time // REVU: change: map[details]Harvester
	period     time.Duration

	h_stdin Harvester
}

type Prospecter interface {
	Worker
}

// ----------------------------------------------------------------------
// harvester API
// ----------------------------------------------------------------------
func NewProspecter(fileconfig FileConfig, scanPeriod time.Duration) Prospecter {
	worker := &prospecter{
		fileconfig: fileconfig,
		harvesters: make(map[Harvester]time.Time),
		fileinfo:   make(map[string]os.FileInfo),
		period:     scanPeriod,
		h_stdin:    nil,
	}
	worker.WorkerBase = NewWorkerBase(worker, "prospector", prospect)
	return worker
}

// Prospector initialization
//
// - create harvester for stdin path (if any)
// - create harvester for each historic instance (if any)
func (p *prospecter) Initialize() *WorkerErr {

	// Handle any stdin paths
	// launch harvester id specified
	for i, path := range p.fileconfig.Paths {
		if path == path_stdin {
			if e := p.addStdinHarvester(i); e != nil {
				return NewWorkerErrWithCause(E_INIT, "on reinstateHarvesters()", e)
			}
			// Remove path from the file list
			p.fileconfig.Paths = append(p.fileconfig.Paths[:i], p.fileconfig.Paths[i+1:]...)
		}
	}

	// check history
	// resume prior harvesters if any
	history, e := getHistory(".logstash-forwarder")
	if e != nil {
		// assuming no history so we're done
		p.log("assuming fresh start & ignoring error on getHistory() - e: %s", e)
		return nil
	}
	e0 := p.reinstateHarvesters(history)
	if e0 != nil {
		return NewWorkerErrWithCause(E_INIT, "on reinstateHarvesters()", e)
	}

	//	panic("prospecter Initialize() not implemented!")
	log.Printf("[prospector] %s initialized", p.Name())

	return nil
}

// ----------------------------------------------------------------------
// task
// ----------------------------------------------------------------------
//func prospect(self interface{}, in <-chan *FileEvent, out chan<- *FileEvent, err chan<- *WorkerErr) {
func prospect(self interface{}, in0, out0 interface{}, err chan<- *WorkerErr) {

	p := self.(*prospecter)
	out := out0.(chan<- *FileEvent)

	for harvester, _ := range p.harvesters {
		go harvester.Work(nil, out, err)
	}

	for {
		select {
		case <-p.ctl_ch:
			p.onShutdown(err)
			return
		case <-time.After(p.period):
			p.log("scanning ..")
			for _, path := range p.fileconfig.Paths {
				// REVU: a bug seems to launch multiple harvesters for the same file
				/* this call should merely identify new prospects
				 * after call determine if harvesters for these are already running
				 * TODO
				 */
				harvesters, e := p.scanPath(path)
				if e != nil {
					panic(NewWorkerErrWithCause(E_RECOVERED_PANIC, "goroutine defer block", e))
				}
				for harvester, reason := range harvesters {
					p.harvesters[harvester] = time.Now()
					p.log("launching new harvestor - %s", reason)
					go harvester.Work(nil, out, err)
				}
			}
		}
	}
}

// ----------------------------------------------------------------------
// support funcs
// ----------------------------------------------------------------------

// shutdown group?
func (p *prospecter) onShutdown(err chan<- *WorkerErr) {
	p.log("shutting down ...")
	for harvester, _ := range p.harvesters {
		p.log("shutting down harvester %s", harvester.Name())
		if we := harvester.Shutdown(time.Second); we != nil {
			err <- we
		}
	}
}

// creates a new Harvestor for the given path and adds it to
// map of harvesters.
func (p *prospecter) addNewHarvester(path string, offset int64, harvestMode HarvestMode) Harvester {
	h := NewHarvester(path, offset, p.fileconfig.Fields, SEEK_HEAD, harvestMode)
	p.harvesters[h] = time.Now()
	return h
}

func (p *prospecter) addStdinHarvester(n int) error {
	// add only once
	if p.h_stdin != nil {
		return nil
	}
	p.log("initialize harvester for <stdin> ..\n")
	h := NewHarvester(path_stdin, 0, p.fileconfig.Fields, NA_SEEK_STREAM, NA_HARVEST_MODE)
	if e := h.Initialize(); e != nil {
		return e
	}
	p.harvesters[h] = time.Now()
	p.h_stdin = h
	return nil
}

// Adds to harvesters
func (p *prospecter) reinstateHarvesters(history map[string]*FileState) error {

	for path, state := range history {
		// if the file is the same inode/device as we last saw,
		// start a harvester on it at the last known position
		// TODO: last state should be in registrar
		info, e := os.Stat(path)
		if e != nil {
			// TODO: should log it
			continue
		}

		if is_file_same(path, info, state) {
			// same file, seek to last known position
			p.fileinfo[path] = info

			for _, pathglob := range p.fileconfig.Paths {
				match, _ := filepath.Match(pathglob, path)
				if match {
					p.log("resume harvester for %s at offset %d..\n", path, state.Offset)
					h := NewHarvester(path, state.Offset, p.fileconfig.Fields, SEEK_CONTINUE, KNOWN_FILE)
					if e := h.Initialize(); e != nil {
						return NewWorkerErrWithCause(E_INIT, "failed to initialize harvester", e)
					}
					p.harvesters[h] = time.Now()
					break
				}
			}
		}
	}
	return nil
}

// scan path and find if we need to launch new harvesters.
// return map of harvester to reason for creation.
// on error map will be nil
func (p *prospecter) scanPath(path string) (map[Harvester]string, error) {
	p.log("scanning path %s\n", path)

	// map of new harvesters and reason why
	harvesters := make(map[Harvester]string)

	// Evaluate the path as a wildcards/shell glob
	matches, e := filepath.Glob(path)
	if e != nil {
		p.log("glob(%s) failed: %v\n", path, e)
		return nil, e
	}
	for _, match := range matches {
		p.log("%s matches ..", match)
	}

	// If the glob matches nothing, use the path itself as a literal.
	if len(matches) == 0 && path == path_stdin {
		matches = append(matches, path)
	}

	// Check any matched files to see if we need to start a harvester
	for _, file := range matches {
		// Stat the file, following any symlinks.
		info, e := os.Stat(file)
		// TODO(sissel): check err
		if e != nil {
			p.log("stat(%s) failed: %s\n", file, e)
			continue
		}

		if info.IsDir() {
			p.log("Skipping directory: %s\n", file)
			continue
		}

		// Check the current info against fileinfo[file]
		lastinfo, is_known := p.fileinfo[file]
		// Track the stat data for this file for later comparison to check for
		// rotation/etc
		p.fileinfo[file] = info

		// Conditions for starting a new harvester:
		// - file path hasn't been seen before
		// - the file's inode or device changed
		if !is_known {
			// TODO(sissel): Skip files with modification dates older than N
			// TODO(sissel): Make the 'ignore if older than N' tunable
			if time.Since(info.ModTime()) > 24*time.Hour {
				p.log("Skipping old file: %s\n", file)
			} else if is_file_renamed(file, info, p.fileinfo) {
				// Check to see if this file was simply renamed (known inode+dev)
			} else {
				// Most likely a new file. Harvest it!
				p.log("Adding new harvester on new file: %s\n", file)
				h := p.addNewHarvester(path, 0, NEW_FILE)
				if e0 := h.Initialize(); e0 != nil {
					return nil, fmt.Errorf("error initializing harvester for path: %s file: %s - error: %s", path, file, e0)
				}
				harvesters[h] = "new-file " + file
			}
		} else if !is_fileinfo_same(lastinfo, info) {
			p.log("Adding new harvester on rotated file: %s\n", file)
			// TODO(sissel): log 'file rotated' or osmething
			// Start a harvester on the path; a new file appeared with the same name.
			h := p.addNewHarvester(path, 0, ROTATED_FILE)
			if e0 := h.Initialize(); e0 != nil {
				return nil, fmt.Errorf("error initializing harvester for path: %s file: %s - error: %s", path, file, e0)
			}
			harvesters[h] = "rotated-file " + file
		}
	} // for each file matched by the glob

	return harvesters, nil
}
