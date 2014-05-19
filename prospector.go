package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"
)

type Prospecter struct {
	fileconfig FileConfig
	fileinfo   map[string]os.FileInfo
	harvesters map[*Harvester]*Harvester
	output     chan<- *FileEvent

	ctl_ch chan int
	CTL    chan<- int
	sig_ch chan interface{}
	SIG    <-chan interface{}
}

func newProspecter(fileconfig FileConfig, output chan *FileEvent) *Prospecter {
	ctl_ch := make(chan int)
	sig_ch := make(chan interface{})
	return &Prospecter{
		fileconfig: fileconfig,
		harvesters: make(map[*Harvester]*Harvester),
		fileinfo:   make(map[string]os.FileInfo),
		output:     output,
		ctl_ch:     ctl_ch,
		CTL:        ctl_ch,
		sig_ch:     sig_ch,
		SIG:        sig_ch,
	}
}

// run the Prospector - events to be sent to the provided channel
func (p *Prospecter) run() {

	log.Printf("[prospector] started - paths: %s\n", p.fileconfig.Paths)

	// Handle any stdin paths
	for i, path := range p.fileconfig.Paths {
		// REVU: what would happen if there are multiple "-" spec'd? TODO review (joubin)
		if path == path_stdin {
			log.Printf("[prospector] start harvester for %s ..\n", path)
			harvester := p.addNewHarvester(path)
			go harvester.Harvest()

			// Remove path from the file list
			p.fileconfig.Paths = append(p.fileconfig.Paths[:i], p.fileconfig.Paths[i+1:]...)
		}
	}
	// Use the registrar db to reopen any files at their last positions
	p.resume_tracking()

	for {
		select {
		case <-p.ctl_ch:
			p.shutdown()
			p.sig_ch <- "exit"
			return
		case <-time.After(time.Second * 10):
			for _, path := range p.fileconfig.Paths {
				p.scanPath(path)
			}
		}
	}
}

func (p *Prospecter) shutdown() {
	log.Printf("[prospector] shutting down ...")
	p.sig_ch <- "exit"

	log.Printf("[prospector] shutting down harvesters ...")
	for _, harvester := range p.harvesters {
		harvester.CTL <- 0
	}
	// wait for them REVU: handle non-responsive cases
	for _, harvester := range p.harvesters {
		<-harvester.SIG
	}
}

func (p *Prospecter) resume_tracking() {
	// Start up with any registrar data.
	history, err := os.Open(".logstash-forwarder")
	if err == nil {
		historical_state := make(map[string]*FileState)
		log.Println("[prospector] Loading registrar data")
		decoder := json.NewDecoder(history)
		decoder.Decode(&historical_state)
		history.Close()

		for path, state := range historical_state {
			// if the file is the same inode/device as we last saw,
			// start a harvester on it at the last known position
			info, err := os.Stat(path)
			if err != nil {
				continue
			}

			if is_file_same(path, info, state) {
				// same file, seek to last known position
				p.fileinfo[path] = info

				for _, pathglob := range p.fileconfig.Paths {
					match, _ := filepath.Match(pathglob, path)
					if match {
						// run harvester at last offset
						harvester := p.addNewHarvester(path)
						go harvester.HarvestAtOffset(state.Offset)
						//						go newHarvester(p.output, path, p.fileconfig.Fields).HarvestAtOffset(state.Offset)
						break
					}
				}
			}
		}
	}
}

// creates a new Harvestor for the given path and adds it to
// map of harvesters.
func (p *Prospecter) addNewHarvester(path string) *Harvester {
	h := newHarvester(p.output, path, p.fileconfig.Fields)
	p.harvesters[h] = h
	return h
}

func (p *Prospecter) scanPath(path string) {
	//log.Printf("Prospecting %s\n", path)

	// Evaluate the path as a wildcards/shell glob
	matches, err := filepath.Glob(path)
	if err != nil {
		log.Printf("[prospector] glob(%s) failed: %v\n", path, err)
		return
	}

	// If the glob matches nothing, use the path itself as a literal.
	if len(matches) == 0 && path == path_stdin {
		matches = append(matches, path)
	}

	// Check any matched files to see if we need to start a harvester
	for _, file := range matches {
		// Stat the file, following any symlinks.
		info, err := os.Stat(file)
		// TODO(sissel): check err
		if err != nil {
			log.Printf("[prospector] stat(%s) failed: %s\n", file, err)
			continue
		}

		if info.IsDir() {
			log.Printf("[prospector] Skipping directory: %s\n", file)
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
				log.Printf("[prospector] Skipping old file: %s\n", file)
			} else if is_file_renamed(file, info, p.fileinfo) {
				// Check to see if this file was simply renamed (known inode+dev)
			} else {
				// Most likely a new file. Harvest it!
				log.Printf("[prospector] Launching harvester on new file: %s\n", file)
				go newHarvester(p.output, path, p.fileconfig.Fields).Harvest()
			}
		} else if !is_fileinfo_same(lastinfo, info) {
			log.Printf("[prospector] Launching harvester on rotated file: %s\n", file)
			// TODO(sissel): log 'file rotated' or osmething
			// Start a harvester on the path; a new file appeared with the same name.
			go newHarvester(p.output, path, p.fileconfig.Fields).Harvest()
		}
	} // for each file matched by the glob
}
