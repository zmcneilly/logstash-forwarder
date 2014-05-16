package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type Config struct {
	Network NetworkConfig `json:network`
	Files   []FileConfig  `json:files`
}

type NetworkConfig struct {
	Servers        []string `json:servers`
	SSLCertificate string   `json:"ssl certificate"`
	SSLKey         string   `json:"ssl key"`
	SSLCA          string   `json:"ssl ca"`
	Timeout        int64    `json:timeout`
	timeout        time.Duration
}

type FileConfig struct {
	Paths  []string          `json:paths`
	Fields map[string]string `json:fields`
	//DeadTime time.Duration `json:"dead time"`
}

func LoadConfig(filename string) (*Config, error) {
	fileinfo, e := os.Stat(filename)
	if e != nil {
		return onError(e, "error accessing fileinfo", filename)
	}

	filesize := fileinfo.Size()
	if filesize > (10 << 20) {
		return onError(e, fmt.Sprint("Config file too large? Aborting, just in case ('%s' is %d bytes)", filename, filesize), filename)
	}

	buffer, e := ioutil.ReadFile(filename)
	if e != nil {
		return onError(e, "error reading file", filename)
	}

	var config Config
	e = json.Unmarshal(buffer, config)
	if e != nil {
		return onError(e, fmt.Sprint("json unmarshal fault for buffer <%s>", buffer), filename)
	}

	if config.Network.Timeout == 0 {
		config.Network.Timeout = 15
	}
	config.Network.timeout = time.Duration(config.Network.Timeout) * time.Second

	return &config, nil
}

func onError(e error, format, filename string) (*Config, error) {
	_fmt := format + " - e: %s"
	err := fmt.Sprintf(_fmt, e)
	log.Print(err)
	return nil, fmt.Errorf("failed to load config file '%s' due to error <%s>", filename, e)
}
