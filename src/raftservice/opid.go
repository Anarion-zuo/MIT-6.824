package raftservice

import (
	"github.com/sasha-s/go-deadlock"
)

type IdGenerator struct {
	mu     deadlock.Mutex
	curVal int
}

func (g *IdGenerator) make() int {
	g.mu.Lock()
	result := g.curVal
	g.curVal += 1
	g.mu.Unlock()
	return result
}

/*
type ServerOpId struct {
	serverId int
	opId     int
}

// ServerOpIdManager
// A map between (server, opId) and commitIndex
type ServerOpIdManager struct {
	// documenting sent but not yet done commands
	idMap              map[ServerOpId]int
	serverAppliedHeaps []IntMinHeap
}

func makeServerOpIdManager(serverCount int) *ServerOpIdManager {
	ret := &ServerOpIdManager{
		idMap:              make(map[ServerOpId]int),
		serverAppliedHeaps: make([]IntMinHeap, serverCount),
	}
	for i, _ := range ret.serverAppliedHeaps {
		ret.serverAppliedHeaps[i] = make(IntMinHeap, 0)
		heap.Init(&ret.serverAppliedHeaps[i])
	}
	return ret
}

func (m *ServerOpIdManager) isDone(serverId int, opId int) bool {
	if len(m.serverAppliedHeaps[serverId]) == 0 {
		return false
	}
	return opId < m.serverAppliedHeaps[serverId][0]
}

func (m *ServerOpIdManager) isRunning(serverId int, opId int) bool {
	return 0 != m.idMap[ServerOpId{
		serverId: serverId,
		opId:     opId,
	}]
}

func (m *ServerOpIdManager) isSubmitted(serverId int, opId int) bool {
	if m.isDone(serverId, opId) {
		return true
	}
	if m.isRunning(serverId, opId) {
		return true
	}
	return false
}

func (m *ServerOpIdManager) tryAddOp(serverId int, opId int, commitIndex int) {
	if m.isDone(serverId, opId) || m.isRunning(serverId, opId) {
		return
	}
	m.idMap[ServerOpId{
		serverId: serverId,
		opId:     opId,
	}] = commitIndex
	heap.Push(&m.serverAppliedHeaps[serverId], opId)
}

func (m *ServerOpIdManager) opDone(serverId int, opId int, commitIndex int) {
	key := ServerOpId{
		serverId: serverId,
		opId:     opId,
	}
	commit := m.idMap[key]
	if commit != commitIndex {
		panic("real committed index not equal to one obtained at Start()")
	}
	heap.Remove(&m.serverAppliedHeaps[serverId], opId)
	delete(m.idMap, key)
}
*/
