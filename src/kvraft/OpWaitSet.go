package kvraft

import (
	"github.com/sasha-s/go-deadlock"
	"sync"
)

type _OpWaiter struct {
	cond   *sync.Cond
	done   bool
	result *ExecutionResult
}

type OpWaitSet struct {
	waiterMap map[int]*_OpWaiter
	rwmu      deadlock.RWMutex
}

func makeOpWaitSet() *OpWaitSet {
	return &OpWaitSet{
		waiterMap: make(map[int]*_OpWaiter),
	}
}

const addRedundentCount int = 20

// use commitIndex as recvId
func (s *OpWaitSet) addOpWait(recvId int) {
	s.rwmu.Lock()
	if s.waiterMap[recvId] == nil {
		// allocate some cond for future adds
		for i := 0; i < addRedundentCount; i++ {
			s.waiterMap[recvId+i] = &_OpWaiter{
				cond: sync.NewCond(&deadlock.Mutex{}),
				done: false,
			}
		}
	}
	s.rwmu.Unlock()
}

func (s *OpWaitSet) removeOpWait(recvId int) {
	s.rwmu.Lock()
	// remove all that is smaller
	for i := recvId; i >= 0; i-- {
		if s.waiterMap[i] != nil {
			delete(s.waiterMap, i)
		} else {
			break
		}
	}
	s.rwmu.Unlock()
}

func (s *OpWaitSet) waitOp(recvId int) *ExecutionResult {
	s.rwmu.RLock()
	waiter := s.waiterMap[recvId]
	s.rwmu.RUnlock()
	if waiter == nil {
		return nil
	}
	waiter.cond.L.Lock()
	defer waiter.cond.L.Unlock()
	for !waiter.done {
		waiter.cond.Wait()
	}
	return waiter.result
}

func (s *OpWaitSet) doneOp(recvId int, result *ExecutionResult) {
	s.rwmu.RLock()
	waiter := s.waiterMap[recvId]
	s.rwmu.RUnlock()
	if waiter == nil {
		// nothing to send
		return
	}
	waiter.cond.L.Lock()
	defer waiter.cond.L.Unlock()
	waiter.done = true
	waiter.result = result
	waiter.cond.Broadcast()
}
