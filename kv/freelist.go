package kv

type FreeList struct {
	get      func(uint64) []byte
	new      func([]byte) uint64
	set      func(uint64) []byte
	headPage uint64
	headSeq  uint64
	tailPage uint64
	tailSeq  uint64

	maxSeq uint64
}

func (fl *FreeList) PopHead() uint64
func (fl *FreeList) PushTail(ptr uint64)
