package debug

import (
	"fmt"
	"lsf"
)

func init() {
	fmt.Println("lsf/debug/debug-stage.go init()")
}

type debug struct {
	lsf.WorkerBase

	in  <-chan *lsf.FileEvent
	Out <-chan *lsf.FileEvent
}

func NewDebugStage(name string, in <-chan *lsf.FileEvent) lsf.Worker {
	worker := &debug{
		in: in,
	}
	worker.WorkerBase = lsf.NewWorkerBase(worker, name, debug_task)
	return worker
}

func debug_task(self interface{}, in0, out0 interface{}, err chan<- *lsf.WorkerErr) {
}
