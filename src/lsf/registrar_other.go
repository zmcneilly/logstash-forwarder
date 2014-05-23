// +build !windows

package lsf

import (
	"encoding/json"
	"log"
	"os"
)

func WriteRegistry(state map[string]*FileState, path string) error {
	// Open tmp file, write, flush, rename
	file, e := os.Create(".logstash-forwarder.new")
	if e != nil {
		log.Printf("[registrar] Failed to open .logstash-forwarder.new for writing: %s\n", e)
		return e
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.Encode(state)

	os.Rename(".logstash-forwarder.new", path)

	return nil
}
