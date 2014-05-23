package lsf

import (
	"log"
	"os"
	"time"
)

/* options.go
 * All system and component level options and defaults go here.
 */
type Options struct {
	CONFIG_FNAME string
	//	CPU_PROFILE_FNAME        string
	USE_SYSLOG               bool
	HOSTNAME                 string
	CPU_PROFILE              string
	DEBUG_PIPELINE           bool
	SPOOL_BUFFER_SIZE        int
	REGISTRAR_FILENAME       string
	PROSPECTOR_SCAN_DELAY    time.Duration
	SPOOL_IDLE_TIMEOUT       time.Duration
	HARVESTER_BUFFER_SIZE    int
	HARVESTER_SEEK_FROM_HEAD bool
	HARVESTER_IDLE_TIMEOUT   time.Duration // revu: rename to .._READ_TIMEOUT
	HARVESTER_OPEN_TIMEOUT   time.Duration
	HARVESTER_OPEN_RETRIES   int
	HARVESTER_EOF_DEADLINE   time.Duration
}

var Defaults Options

func init() {
	hostname, _ := os.Hostname()
	Defaults = Options{}

	Defaults.CONFIG_FNAME = ""
	Defaults.HOSTNAME = hostname
	Defaults.CPU_PROFILE = ""
	Defaults.USE_SYSLOG = false
	Defaults.DEBUG_PIPELINE = false
	Defaults.REGISTRAR_FILENAME = ".logstash-forwarder"
	Defaults.SPOOL_BUFFER_SIZE = 1024
	Defaults.PROSPECTOR_SCAN_DELAY = time.Second * 10
	Defaults.SPOOL_IDLE_TIMEOUT = time.Second * 5
	Defaults.HARVESTER_SEEK_FROM_HEAD = false
	Defaults.HARVESTER_BUFFER_SIZE = 16 << 10 /* harvester buffer size 16kb buffer by default */
	Defaults.HARVESTER_IDLE_TIMEOUT = time.Second * 5
	Defaults.HARVESTER_EOF_DEADLINE = time.Hour * 24
	Defaults.HARVESTER_OPEN_TIMEOUT = time.Second * 5
	Defaults.HARVESTER_OPEN_RETRIES = 3
}

func (o *Options) display() {
	log.Printf("options:")
	log.Printf("\t\tconfig-file:                   <%s>", o.CONFIG_FNAME)
	log.Printf("\t\thostname:                      <%s>", o.HOSTNAME)
	log.Printf("\t\tusing syslog:                  <%t>", o.USE_SYSLOG)
	log.Printf("\t\tpipeline-debug:                <%t>", o.DEBUG_PIPELINE)
	log.Printf("\t\tregistrar file:                <%s>", o.REGISTRAR_FILENAME)
	log.Printf("\t\tmax-spool-size:                <%d>", o.SPOOL_BUFFER_SIZE)
	log.Printf("\t\tprospector scan delay (msec):  <%d>", o.PROSPECTOR_SCAN_DELAY/time.Millisecond)
	log.Printf("\t\tspool timeout (msec):          <%d>", o.SPOOL_IDLE_TIMEOUT/time.Millisecond)
	log.Printf("\t\tharvester read timeout (msec): <%d>", o.HARVESTER_IDLE_TIMEOUT/time.Millisecond)
	log.Printf("\t\tharvester EOF deadline (secs): <%d>", o.HARVESTER_EOF_DEADLINE/time.Second)
	log.Printf("\t\tharvester buffer size:         <%d>", o.HARVESTER_BUFFER_SIZE)
	log.Printf("\t\tscan files from end:           <%t>", !o.HARVESTER_SEEK_FROM_HEAD)
}
