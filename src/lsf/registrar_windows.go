package lsf

import (
	"encoding/json"
	"log"
	"os"
)

func WriteRegistry(state map[string]*FileState, path string) error {
	tmp := path + ".new"
	file, e := os.Create(tmp)
	if e != nil {
		log.Printf("[registrar] Failed to open .logstash-forwarder.new for writing: %s\n", e)
		return e
	}

	encoder := json.NewEncoder(file)
	encoder.Encode(state)
	file.Close()

	old := path + ".old"
	os.Rename(path, old)
	os.Rename(tmp, path)

	return nil
}
