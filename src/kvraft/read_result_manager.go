package kvraft

import (
	"github.com/sasha-s/go-deadlock"
)

type ExecutionResult struct {
	executed  bool
	timeout   bool
	hasKey    bool
	vi        ValueIndex
	notLeader bool
	term      int
}

type ResultManager struct {
	resultSet map[int]map[int]*ExecutionResult
	mu        deadlock.Mutex
}

func makeCondManager() *ResultManager {
	return &ResultManager{
		resultSet: make(map[int]map[int]*ExecutionResult),
	}
}

//func (m *ResultManager) make() (int, *sync.Cond) {
//	m.mu.Lock()
//	defer m.mu.Unlock()
//	id := m.curId
//	m.curId++
//	cond := sync.NewCond(&deadlock.Mutex{})
//	m.conds[id] = &ExecutionResult{
//		cond:     cond,
//		executed: false,
//		timeout:  false,
//		hasKey:   false,
//	}
//	return id, cond
//}

func (m *ResultManager) getByClerkOpId(clerkId int, opId int) *ExecutionResult {
	clerkSet := m.resultSet[clerkId]
	if clerkSet == nil {
		// this clerk has not sent anything
		return nil
	}
	result := clerkSet[opId]
	return result
}

func (m *ResultManager) insertNewResult(clerkId int, opId int, result *ExecutionResult) {
	clerkSet := m.resultSet[clerkId]
	if clerkSet == nil {
		clerkSet = make(map[int]*ExecutionResult)
		m.resultSet[clerkId] = clerkSet
	}
	old := clerkSet[opId]
	if old != nil {
		panic("a result already in place")
	}
	clerkSet[opId] = result
}

// call this when executing Get
func (kv *KVServer) readKV(key string, clerkId int, opId int) *ExecutionResult {
	kv.resultManager.mu.Lock()
	defer kv.resultManager.mu.Unlock()
	// check whether this read has been executed
	old := kv.resultManager.getByClerkOpId(clerkId, opId)
	if old != nil {
		return old
	}
	// must perform read
	result := &ExecutionResult{
		executed:  true,
		timeout:   false,
		hasKey:    false,
		vi:        ValueIndex{},
		notLeader: false,
	}
	kv.mapRwmu.RLock()
	vip := kv.kvMap[key]
	if vip != nil {
		result.hasKey = true
		result.vi = *vip
	}
	kv.mapRwmu.RUnlock()
	kv.resultManager.insertNewResult(clerkId, opId, result)
	return result
}
