package lsf

import (
	"fmt"
)

/// error codes ///////////////////////////////////////

type ErrCode int

/* general and component specific errors all should be listed here
   and also added to the String() method */

const (
	E_NONE ErrCode = 0 - iota
	E_ERROR
	E_RECOVERED_PANIC
	E_TIMEOUT
	E_NOWORK
	E_SEND_BLOCK
	E_RECV_BLOCK
	E_INIT
)

func (code ErrCode) String() string {
	s := "BUG:UNKNOWN-ERROR-CODE"
	switch code {
	case E_RECOVERED_PANIC:
		s = "E_RECOVERED_PANIC"
	case E_TIMEOUT:
		s = "E_TIMEOUT"
	case E_NOWORK:
		s = "E_NOWORK"
	case E_SEND_BLOCK:
		s = "E_SEND_BLOCK"
	case E_INIT:
		s = "E_INIT"
	}
	return fmt.Sprintf("(%03d) %s", code, s)
}
