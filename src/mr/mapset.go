package mr

type MapSet struct {
	mapbool map[interface {}]bool
	count int
}

func NewMapSet() *MapSet {
	m := MapSet {}
	m.mapbool = make(map[interface{}]bool)
	m.count = 0
	return &m
}

func (m *MapSet) Insert(data interface {}) {
	m.mapbool[data] = true
	m.count++
}

func (m *MapSet) Has(data interface {}) bool {
	return m.mapbool[data]
}

func (m *MapSet) Remove(data interface {}) {
	m.mapbool[data] = false
	m.count--
}

func (m *MapSet) Size() int {
	return m.count
}
