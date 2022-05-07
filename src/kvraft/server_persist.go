package kvraft

//type KvPersister struct {
//	mu   deadlock.Mutex
//	data []byte
//}

//var kvPersisters []*KvPersister = nil
//var kvPersistersMutex deadlock.Mutex
//
//func fetchGlobalKvPersisters(peerCount int, me int) *KvPersister {
//	kvPersistersMutex.Lock()
//	defer kvPersistersMutex.Unlock()
//	if kvPersisters != nil {
//
//	} else {
//		kvPersisters = make([]*KvPersister, peerCount)
//		for i, _ := range kvPersisters {
//			kvPersisters[i] = makeKvPersister()
//		}
//	}
//	return kvPersisters[me]
//}

//func makeKvPersister() *KvPersister {
//	return &KvPersister{
//		data: nil,
//		mu:   deadlock.Mutex{},
//	}
//}

//func (persister *KvPersister) setData(data []byte) {
//	persister.mu.Lock()
//	persister.data = make([]byte, len(data))
//	copy(persister.data, data)
//	persister.mu.Unlock()
//}

//func (kv *KVServer) initKvPersister() {
//	kv.persister = fetchGlobalKvPersisters(kv.rf.PeerCount(), kv.me)
//	kv.indexToSnapshot = make(map[int][]byte)
//	//kv.readSnapshot(kv.persister.data)
//}

func (kv *KVServer) takeSnapshot() []byte {
	w, e := kv.raftServer.TakeSnapshot()
	err := e.Encode(kv.kvMap)
	if err != nil {
		panic(err)
	}
	err = e.Encode(kv.resultManager.resultSet)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (kv *KVServer) readSnapshot(buffer []byte) {
	_, d := kv.raftServer.ReadSnapshot(buffer)
	if len(buffer) <= 0 {
		kv.kvMap = make(map[string]*ValueIndex)
		kv.resultManager = makeResultManager()
		return
	}
	err := d.Decode(&kv.kvMap)
	if err != nil {
		panic(err)
	}
	err = d.Decode(&kv.resultManager.resultSet)
	if err != nil {
		panic(err)
	}
}

//func (kv *KVServer) removeLessIndexSnapshot(index int) {
//	lessIndex := make(map[int]bool)
//	for i, _ := range kv.indexToSnapshot {
//		if i < index {
//			lessIndex[i] = true
//		}
//	}
//	for i, _ := range lessIndex {
//		delete(kv.indexToSnapshot, i)
//	}
//}

func (kv *KVServer) takeSnapshotWhenStateTooLarge(index int) []byte {
	snapshot := kv.takeSnapshot()
	//old := kv.indexToSnapshot[index]
	//if old != nil {
	//	panic("snapshot twice at same index")
	//}
	//kv.indexToSnapshot[index] = snapshot
	//kv.persister.setData(snapshot)
	return snapshot
}

// call Raft.Snapshot
func (kv *KVServer) issueSnapshot(term int, index int) {
	kv.rf.CondInstallSnapshot(term, index, kv.takeSnapshotWhenStateTooLarge(index))
}

// snapshot callback
func (kv *KVServer) executeSnapshot(index int, snapshot []byte) {
	// switch to this snapshot
	kv.readSnapshot(snapshot)
}
