package lsf

import (
	"bytes"
	"compress/zlib"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	//	"os"
	"regexp"
	"strconv"
	"time"
)

// Support for newer SSL signature algorithms
import _ "crypto/sha256"
import _ "crypto/sha512"

// boiler plate - sanity check
func init() {
	fmt.Println("lsf/publisher.go init")
}

// ----------------------------------------------------------------------
// constants and support types
// ----------------------------------------------------------------------

// TODO move to top level main.go
//var hostname string
var hostport_re, _ = regexp.Compile("^(.+):([0-9]+)$")

// ----------------------------------------------------------------------
// publisher
// ----------------------------------------------------------------------
type publisher struct {
	WorkerBase
	config   *NetworkConfig
	hostname string
	socket   *tls.Conn
}
type Publisher interface {
	Worker
}

// ----------------------------------------------------------------------
// publisher API
// ----------------------------------------------------------------------
func NewPublisher(netconfig *NetworkConfig, hostname string) Publisher {
	worker := &publisher{
		config:   netconfig,
		hostname: hostname,
	}
	worker.WorkerBase = NewWorkerBase(worker, "publisher", publish)
	return worker
}

func (w *publisher) Initialize() *WorkerErr {
	/// initialize ////////////////////////////////////
	/// initialize ////////////////////////////////////
	return nil
}

// ----------------------------------------------------------------------
// task
// ----------------------------------------------------------------------
func publish(self interface{}, in0, out0 interface{}, err chan<- *WorkerErr) {
	p := self.(*publisher)
	in := in0.(<-chan []*FileEvent)
	out := out0.(chan<- []*FileEvent)

	// TEMP - uncomment
	//	p.connect()
	//	defer p.socket.Close()

	for {
		select {
		case <-p.ctl_ch:
			p.log("shutdown command rcvd")
			return
		case events, ok := <-in:
			switch {
			case !ok:
				/* todo error */
				return
			case events == nil:
				/* todo error */
				return
			default:

				// publish events to server

				// TEMP - uncomment
				//				e := p.publishEvents(events)
				//				if e != nil {
				//					/* todo error */
				//					return
				//				}

				// forward events to registrar

				sndtimeout := time.Second // Config this
				timedout := sendFileEventArray("publisher-out", events, out, sndtimeout, p.WorkerBase, err)
				if timedout {
					p.log("sending %d events ..", len(events))
					// TODO: err<- ... log()
					return
				}
			}
		}
	}
}

// ----------------------------------------------------------------------
// support funcs
// ----------------------------------------------------------------------

func (p *publisher) disconnect() error {
	return p.socket.Close()
}

func (p *publisher) publishEvents(events []*FileEvent) error {
	var buffer bytes.Buffer
	//	var socket *tls.Conn
	var sequence uint32
	var err error

	/// compress //////////////////////////////////////

	buffer.Truncate(0)
	compressor, _ := zlib.NewWriterLevel(&buffer, 3)

	for _, event := range events {
		sequence += 1
		p.writeDataFrame(event, sequence, compressor)
	}
	compressor.Flush()
	compressor.Close()

	compressed_payload := buffer.Bytes()

	// Send buffer until we're successful...
	oops := func(err error) {
		// TODO(sissel): Track how frequently we timeout and reconnect. If we're
		// timing out too frequently, there's really no point in timing out since
		// basically everything is slow or down. We'll want to ratchet up the
		// timeout value slowly until things improve, then ratchet it down once
		// things seem healthy.
		p.log("Socket error, will reconnect: %s\n", err)
		time.Sleep(1 * time.Second)
		p.disconnect()
		p.connect()
	}

	/// tranmist //////////////////////////////////////

SendPayload:
	for {
		// Abort if our whole request takes longer than the configured
		// network timeout.
		p.socket.SetDeadline(time.Now().Add(p.config.timeout))

		// Set the window size to the length of this payload in events.
		_, err = p.socket.Write([]byte("1W"))
		if err != nil {
			oops(err)
			continue
		}
		binary.Write(p.socket, binary.BigEndian, uint32(len(events)))
		if err != nil {
			oops(err)
			continue
		}

		// Write compressed frame
		p.socket.Write([]byte("1C"))
		if err != nil {
			oops(err)
			continue
		}
		binary.Write(p.socket, binary.BigEndian, uint32(len(compressed_payload)))
		if err != nil {
			oops(err)
			continue
		}
		_, err = p.socket.Write(compressed_payload)
		if err != nil {
			oops(err)
			continue
		}

		// read ack
		response := make([]byte, 0, 6)
		ackbytes := 0
		for ackbytes != 6 {
			n, err := p.socket.Read(response[len(response):cap(response)])
			if err != nil {
				p.log("Read error looking for ack: %s\n", err)
				p.disconnect()
				p.connect()
				continue SendPayload // retry sending on new connection
			} else {
				ackbytes += n
			}
		}

		// TODO(sissel): verify ack
		// Success, stop trying to send the payload.
		break
	}

	return nil
}

// TODO: get rid of log.Fatal and return errors.
func (p *publisher) connect() { // TODO error?

	var config *NetworkConfig = p.config // temp convenience

	var tlsconfig tls.Config

	if len(config.SSLCertificate) > 0 && len(config.SSLKey) > 0 {
		p.log("Loading client ssl certificate: %s and %s\n", config.SSLCertificate, config.SSLKey)
		cert, err := tls.LoadX509KeyPair(config.SSLCertificate, config.SSLKey)
		if err != nil {
			log.Fatalf("Failed loading client ssl certificate: %s\n", err)
		}
		tlsconfig.Certificates = []tls.Certificate{cert}
	}

	if len(config.SSLCA) > 0 {
		p.log("Setting trusted CA from file: %s\n", config.SSLCA)
		tlsconfig.RootCAs = x509.NewCertPool()

		pemdata, err := ioutil.ReadFile(config.SSLCA)
		if err != nil {
			log.Fatalf("[publisher] Failure reading CA certificate: %s\n", err)
		}

		block, _ := pem.Decode(pemdata)
		if block == nil {
			log.Fatalf("[publisher] Failed to decode PEM data, is %s a valid cert?\n", config.SSLCA)
		}
		if block.Type != "CERTIFICATE" {
			log.Fatalf("[publisher] This is not a certificate file: %s\n", config.SSLCA)
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			log.Fatalf("[publisher] Failed to parse a certificate: %s\n", config.SSLCA)
		}
		tlsconfig.RootCAs.AddCert(cert)
	}

	for {
		// Pick a random server from the list.
		hostport := config.Servers[rand.Int()%len(config.Servers)]
		submatch := hostport_re.FindSubmatch([]byte(hostport))
		if submatch == nil {
			log.Fatalf("[publisher] Invalid host:port given: %s", hostport)
		}
		host := string(submatch[1])
		port := string(submatch[2])
		addresses, err := net.LookupHost(host)

		if err != nil {
			p.log("DNS lookup failure \"%s\": %s\n", host, err)
			time.Sleep(1 * time.Second)
			continue
		}

		address := addresses[rand.Int()%len(addresses)]
		addressport := fmt.Sprintf("%s:%s", address, port)

		p.log("Connecting to %s (%s) \n", addressport, host)

		tcpsocket, err := net.DialTimeout("tcp", addressport, config.timeout)
		if err != nil {
			p.log("Failure connecting to %s: %s\n", address, err)
			time.Sleep(1 * time.Second)
			continue
		}

		tlsconfig.ServerName = host // pull #205 -Go 1.3 tls change -backwards compatible

		p.socket = tls.Client(tcpsocket, &tlsconfig)
		p.socket.SetDeadline(time.Now().Add(config.timeout))
		err = p.socket.Handshake()
		if err != nil {
			p.log("Failed to tls handshake with %s %s\n", address, err)
			time.Sleep(1 * time.Second)
			p.disconnect()
			continue
		}

		p.log("Connected to %s\n", address)

		// connected, let's rock and roll.
		return
	}
	return
}
func (p *publisher) writeDataFrame(event *FileEvent, sequence uint32, output io.Writer) {
	//log.Printf("event: %s\n", *event.Text)
	// header, "1D"
	output.Write([]byte("1D"))
	// sequence number
	binary.Write(output, binary.BigEndian, uint32(sequence))
	// 'pair' count
	binary.Write(output, binary.BigEndian, uint32(len(*event.Fields)+4))

	writeKV("file", *event.Source, output)
	writeKV("host", p.hostname, output)
	writeKV("offset", strconv.FormatInt(event.Offset, 10), output)
	writeKV("line", *event.Text, output)
	for k, v := range *event.Fields {
		writeKV(k, v, output)
	}
}

func writeKV(key string, value string, output io.Writer) {
	//log.Printf("kv: %d/%s %d/%s\n", len(key), key, len(value), value)
	binary.Write(output, binary.BigEndian, uint32(len(key)))
	output.Write([]byte(key))
	binary.Write(output, binary.BigEndian, uint32(len(value)))
	output.Write([]byte(value))
}
