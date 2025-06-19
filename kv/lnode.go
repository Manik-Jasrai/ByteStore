package kv

import (
	"encoding/binary"

	"github.com/Manik-Jasrai/ByteStore.git/btree"
	"github.com/Manik-Jasrai/ByteStore.git/utils"
)

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (btree.BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

/*
node format
|  8B  |   n*8B   |  ...   |
| next | pointers | unused |
*/
type LNode []byte

// TODO:
// getters & setters
func (node LNode) getNext() uint64 {
	return binary.LittleEndian.Uint64(node[0:])
}
func (node LNode) setNext(next uint64) {
	binary.LittleEndian.PutUint64(node[0:], next)
}
func (node LNode) getPtr(idx int) uint64 {
	utils.Assert(idx >= 0 && idx < FREE_LIST_CAP, "Index Out of Bounds : LNode GetPointer")
	return binary.LittleEndian.Uint64(node[FREE_LIST_HEADER+idx:])
}
func (node LNode) setPtr(idx int, ptr uint64) {
	utils.Assert(idx >= 0 && idx < FREE_LIST_CAP, "Index Out of Bounds : LNode SetPointer")
	binary.LittleEndian.PutUint64(node[FREE_LIST_HEADER+idx:], ptr)
}
