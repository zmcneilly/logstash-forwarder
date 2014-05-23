package lsf

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func getHistory(filename string) (map[string]*FileState, error) {
	file, e := os.Open(filename)
	if e != nil {
		e0 := fmt.Errorf("[lsf] could not open history file - %s", e)
		log.Println(e0)
		return nil, e0
	}
	defer file.Close()

	// decode json file and return history (map)
	log.Printf("[lsf] Loading registrar data (%s)", filename)
	history := make(map[string]*FileState)
	codec := json.NewDecoder(file)
	if e = codec.Decode(&history); e != nil {
		e0 := fmt.Errorf("json.Decode of history file failed - %s", e)
		return nil, e0
	}
	return history, nil
}
