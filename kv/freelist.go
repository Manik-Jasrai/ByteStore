package kv

import (
	"github.com/Manik-Jasrai/ByteStore.git/btree"
	"github.com/Manik-Jasrai/ByteStore.git/utils"
)

type FreeList struct {
	get func(uint64) []byte // read a page
	new func([]byte) uint64 // append a new page
	set func(uint64) []byte // update an existing page

	headPage uint64 // Pointer to head Page
	headSeq  uint64 // Seq no of firdt item
	tailPage uint64 // Pointer to tail Page
	tailSeq  uint64 // Seq no of Last item

	maxSeq uint64 // last item available for consumption
}

// Get 1 item from list head, return 0 on failure
func (fl *FreeList) PopHead() uint64 {
	ptr, head := flPop(fl)
	if head != 0 {
		// Push it back
		fl.PushTail(head)
	}
	return ptr
}

// Add 1 item to tail
func (fl *FreeList) PushTail(ptr uint64) {

	LNode(fl.set(fl.tailPage)).setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++
	if seq2idx(fl.tailSeq) == 0 {
		// Add a new tail Page
		next, head := flPop(fl)
		if next == 0 {
			next = fl.new(make([]byte, btree.BTREE_PAGE_SIZE))
		}
		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage = next
		if head != 0 {
			LNode(fl.set(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

// make the newly added item available for consumption
func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

// pop the first item from the head page
func flPop(fl *FreeList) (ptr uint64, head uint64) {

	if fl.headSeq >= fl.maxSeq {
		return 0, 0
	}

	node := LNode(fl.get(fl.headPage))
	ptr = node.getPtr(seq2idx(fl.headSeq))
	fl.headSeq++
	if seq2idx(fl.headSeq) == 0 {
		// we return the previous head page to recycle it
		head, fl.headPage = fl.headPage, node.getNext()
		utils.Assert(fl.headPage != 0, "Empty List")
	}
	return
}
