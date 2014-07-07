package main

import (
	"testing"
)

// ----------------------------------------------------------------------
// not-strict
// ----------------------------------------------------------------------

// NOTE: these tests fails intermittently - not clear why.

func TestInsecureConnectValidCertificate(t *testing.T) {
	go listenWithCert("localhost", "0.0.0.0:19876")
	if err := <-tryConnect("localhost:19876", insecure); err != nil {
		t.Fatal("Should have succeeded", err)
	}
}

func TestInsecureConnectMismatchedCN(t *testing.T) {
	go listenWithCert("localalt", "0.0.0.0:19876")
	if err := <-tryConnect("localhost:19876", insecure); err != nil {
		t.Fatal("Should have succeeded", err)
	}
}

func TestInsecureConnectToIpWithoutSAN(t *testing.T) {
	go listenWithCert("localhost", "0.0.0.0:19876")
	if err := <-tryConnect("127.0.0.1:19876", insecure); err != nil {
		t.Fatal("Should have succeeded", err)
	}
}

func TestInsecureConnectToIpWithSAN(t *testing.T) {
	go listenWithCert("127.0.0.1", "0.0.0.0:19876")
	if err := <-tryConnect("127.0.0.1:19876", insecure); err != nil {
		t.Fatal("Should have succeeded", err)
	}
}
