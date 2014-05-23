package lsf

import (
	"errors"
	"fmt"
	"log"
	"time"
)

/// error /////////////////////////////////////////////

type WorkerErr struct {
	Code  ErrCode
	Cause error
	msg   string
}

var NoError *WorkerErr = &WorkerErr{E_NONE, nil, ""}

func NewWorkerErr(code ErrCode, info string) *WorkerErr {
	return NewWorkerErrWithCause(code, info, nil)
}
func NewWorkerErrWithCause(code ErrCode, info string, cause error) *WorkerErr {
	var extra string = ""
	if cause != nil {
		extra = fmt.Sprintf(" - cause: %s", cause)
	}
	msg := fmt.Sprintf("WorkerErr %s - %s%s", code.String(), info, extra)
	return &WorkerErr{code, cause, msg}
}
func (e *WorkerErr) Error() string {
	return e.msg
}

/// Worker ////////////////////////////////////////////////

type Worker interface {
	Name() string
	Initialize() *WorkerErr
	Work(in, out interface{}, err chan<- *WorkerErr)
	Shutdown(wait time.Duration) *WorkerErr
}

// interruptible function used in Work.
type Task func(self interface{}, in, out interface{}, err chan<- *WorkerErr)

/// WorkerBase ////////////////////////////////////////////

type WorkerBase struct {
	name    string
	ctl_ch  chan int
	sig_ch  chan interface{}
	timeout time.Duration
	task    Task
	self    interface{}
}

func NewWorkerBase(self interface{}, name string, task Task) WorkerBase {
	w := WorkerBase{
		self:   self,
		name:   name,
		task:   task,
		ctl_ch: make(chan int, 1),
		sig_ch: make(chan interface{}, 0), // close to signal ack
	}
	return w
}

func (w *WorkerBase) Name() string {
	return w.name
}

func (w *WorkerBase) Initialize() *WorkerErr {
	panic("generic Initialize() not implemented!")
}

func (w *WorkerBase) Work(in, out interface{}, err chan<- *WorkerErr) {

	/// boilerplate ////////////////////////////////////
	defer func() {
		if re := recover(); re != nil {
			cause := errors.New(fmt.Sprintf("%v", re))
			e := NewWorkerErrWithCause(E_RECOVERED_PANIC, "WorkerBase#Run() defer block", cause)
			err <- e
		}
		return
	}()
	// signal channel always closed on go routine exit
	defer close(w.sig_ch)
	/// boilerplate ////////////////////////////////////

	w.log("working ..")
	// interruptible function call
	w.task(w.self, in, out, err)
	//	w.harvest(in, out, err)
}

func (w *WorkerBase) Shutdown(wait time.Duration) *WorkerErr {
	select {
	case w.ctl_ch <- 0:
	case <-time.After(wait):
		msg := fmt.Sprintf("timeout on shutdown command send to %s", w.Name())
		return NewWorkerErr(E_TIMEOUT, msg)
	}
	select {
	case <-w.sig_ch:
	case <-time.After(wait):
		msg := fmt.Sprintf("%s sig_ch ack for shutdown timedout", w.Name())
		w.log(msg)
		return NewWorkerErr(E_TIMEOUT, msg)
	}
	w.log("shutdown")
	return nil
}

// Note: for use by extensions only
func (w *WorkerBase) log(format string, args ...interface{}) {
	fmt0 := fmt.Sprintf("[%-20s] %s", w.Name(), format)
	log.Printf(fmt0, args...)
}
