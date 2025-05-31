package btree

import (
	"bytes"
	"encoding/binary"

	"github.com/Manik-Jasrai/ByteStore.git/utils"
)

const (
	BNODE_NODE = uint16(1)
	BNODE_LEAF = uint16(2)
)

type BNode struct {
	data []byte
}

// Header
func (node *BNode) bType() uint16 {
	return binary.LittleEndian.Uint16(node.data[0:2])
}
func (node *BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node *BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype) // uint16 - 2Bytes
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// Pointers
func (node *BNode) getPtr(idx uint16) uint64 {
	utils.Assert(idx < node.nKeys(), "Index Out of Bounds : GetPtr")
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:]) // uint64 - 8Bytes
}
func (node *BNode) setPtr(idx uint16, ptr uint64) {
	utils.Assert(idx < node.nKeys(), "Index Out of Bounds : SetPtr")
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], ptr) // uint64 - 8Bytes
}

// Offset - used to locate KV quickly
func (node *BNode) offsetPos(idx uint16) uint16 {
	utils.Assert(1 <= idx && idx <= node.nKeys(), "Index Out of Bounds : OffsetPos")
	return HEADER + 8*node.nKeys() + 2*(idx-1)
}
func (node *BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[node.offsetPos(idx):])
}
func (node *BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[node.offsetPos(idx):], offset)
}

// KV
func (node *BNode) KVPos(idx uint16) uint16 {
	utils.Assert(idx <= node.nKeys(), "Index Out of Bounds : KVPos")
	return HEADER + 8*node.nKeys() + 2*node.nKeys() + node.getOffset(idx)
}
func (node *BNode) getKey(idx uint16) []byte {
	utils.Assert(idx < node.nKeys(), "Index Out of Bounds : GetKey")
	pos := node.KVPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return (node.data[pos+4:])[:klen]
	// 1. first create a slice from pos + 4 to end
	// 2. then it selects the first klen from it
	// 3. helps preven out of bound panics
}
func (node *BNode) getValue(idx uint16) []byte {
	utils.Assert(idx < node.nKeys(), "Index Out of Bounds : GetValue")
	pos := node.KVPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return (node.data[pos+klen+4:])[:vlen]
}

// Size of Node
func (node *BNode) nBytes() uint16 {
	return node.KVPos(node.nKeys())
}

// Search a key
// or a pos where this key could be put
func (node *BNode) lookUp(key []byte) uint16 {
	found := uint16(0)
	for i := uint16(1); i < node.nKeys(); i++ {
		// since keys are sorted
		// we have to find the largest key which is smaller than new Key
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		// if we have found the largest num possible then we can return
		if cmp >= 0 {
			break
		}
	}
	return found
}

// Insert KV to Leaf Node
// Create a new Node and insert the new key in it
// Inserts at idx and shifts others
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nKeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nKeys()-idx)
}

// Updates at idx
func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nKeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nKeys()-idx-1)
}

// Deletes KV at idx
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nKeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nKeys()-idx-1)
}

func nodeAppendRange(new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	utils.Assert(srcOld+n <= old.nKeys(), "Out of Range Append : nodeAppendRange 1")
	utils.Assert(dstNew+n <= new.nKeys(), "Out of Range Append : nodeAppendRange 2")
	if n == 0 {
		return
	}
	// Pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}
	// Offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin - srcBegin + old.getOffset(srcOld+i)
		new.setOffset(dstNew+i, offset)
	}
	// KVs
	begin := old.KVPos(srcOld)
	end := old.KVPos(srcOld + n)
	copy(new.data[new.KVPos(dstNew):], old.data[begin:end])
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// Pointer
	new.setPtr(idx, ptr)
	// KV
	pos := new.KVPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// Offset for next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

// This Function Helps to update the kids of a node
func NodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(old.bType(), old.nKeys()-1+inc) // we split 1 into inc(new split)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nKeys()-(idx+1))
}

// Replace 2 adjacent links with 1
func NodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(old.bType(), old.nKeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, old.getValue(idx))
	nodeAppendRange(new, old, idx+1, idx+2, old.nKeys()-(idx+2))
}

func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.bType(), left.nKeys()+right.nKeys())
	nodeAppendRange(new, left, 0, 0, left.nKeys())
	nodeAppendRange(new, right, left.nKeys(), 0, right.nKeys())
}
