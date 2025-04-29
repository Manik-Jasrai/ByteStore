package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func createBNode() BNode {
	node := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	node.setHeader(BNODE_LEAF, 2)
	nodeAppendKV(node, 0, 0, []byte("k1"), []byte("hi"))
	nodeAppendKV(node, 1, 0, []byte("k3"), []byte("hello"))
	return node
}

func CreateLeafwithKVs(keys [][]byte, vals [][]byte) BNode {
	node := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	node.setHeader(BNODE_LEAF, 0)
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		val := vals[i]
		newNode := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
		leafInsert(newNode, node, uint16(i), key, val)
		node = newNode
	}
	return node
}
func TestBNode(t *testing.T) {
	node := createBNode()

	// Check Header
	assert.Equal(t, BNODE_LEAF, int(node.bType()))
	assert.Equal(t, uint16(2), node.nKeys())

	// Check Pointers
	assert.Equal(t, uint64(0), node.getPtr(0))
	assert.Equal(t, uint64(0), node.getPtr(1))

	// Check Keys
	assert.Equal(t, []byte("k1"), node.getKey(0))
	assert.Equal(t, []byte("k3"), node.getKey(1))

	// Check Values
	assert.Equal(t, []byte("hi"), node.getValue(0))
	assert.Equal(t, []byte("hello"), node.getValue(1))

	// Check Offsets
	off1 := node.getOffset(0)
	off2 := node.getOffset(1)
	expectedOff1 := uint16(0)
	expectedOff2 := uint16(4 + len("k1") + len("hi")) // offset from beginning of KV1 to start of KV2

	assert.Equal(t, expectedOff1, off1)
	assert.Equal(t, expectedOff2, off2)

	// Check KVPos correctness
	pos1 := node.KVPos(0)
	pos2 := node.KVPos(1)

	assert.Equal(t, HEADER+8*node.nKeys()+2*node.nKeys()+off1, pos1)
	assert.Equal(t, HEADER+8*node.nKeys()+2*node.nKeys()+off2, pos2)

	// Optional: check total size of node
	size := node.nBytes()
	assert.Greater(t, size, uint16(0))
}

func TestLookUp(t *testing.T) {
	node := createBNode()

	// Keys: ["k1", "k3"]

	// Looking for exact key "k1"
	idx := node.lookUp([]byte("k1"))
	assert.Equal(t, uint16(0), idx)
	assert.Equal(t, []byte("k1"), node.getKey(idx))

	// Looking for exact key "k3"
	idx = node.lookUp([]byte("k3"))
	assert.Equal(t, uint16(1), idx)
	assert.Equal(t, []byte("k3"), node.getKey(idx))

	// Looking for key between "k1" and "k3", like "k2"
	idx = node.lookUp([]byte("k2"))
	assert.Equal(t, uint16(0), idx)
	assert.Equal(t, []byte("k1"), node.getKey(idx))

	// Looking for a key less than "k1", like "k0"
	idx = node.lookUp([]byte("k0"))
	assert.Equal(t, uint16(0), idx)

	// Looking for a key greater than all existing keys, like "k9"
	idx = node.lookUp([]byte("k9"))
	assert.Equal(t, uint16(1), idx)
	assert.Equal(t, []byte("k3"), node.getKey(idx))
}

func TestLeafInsert(t *testing.T) {
	old := createBNode()
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	idx := old.lookUp([]byte("k2"))
	assert.Equal(t, uint16(0), idx)
	// Insert "k2" between "k1" and "k3"
	leafInsert(new, old, idx+1, []byte("k2"), []byte("middle"))

	// Now new should have 3 keys: ["k1", "k2", "k3"]
	assert.Equal(t, uint16(3), new.nKeys())
	assert.Equal(t, []byte("k1"), new.getKey(0))
	assert.Equal(t, []byte("k2"), new.getKey(1))
	assert.Equal(t, []byte("k3"), new.getKey(2))

	assert.Equal(t, []byte("hi"), new.getValue(0))
	assert.Equal(t, []byte("middle"), new.getValue(1))
	assert.Equal(t, []byte("hello"), new.getValue(2))
}
func TestLeafUpdate(t *testing.T) {
	old := createBNode()
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	idx := old.lookUp([]byte("k3"))
	assert.Equal(t, uint16(1), idx)

	leafUpdate(new, old, idx, []byte("k3"), []byte("goodbye"))

	assert.Equal(t, uint16(2), new.nKeys())
	assert.Equal(t, []byte("k1"), new.getKey(0))
	assert.Equal(t, []byte("k3"), new.getKey(1))

	assert.Equal(t, []byte("hi"), new.getValue(0))
	assert.Equal(t, []byte("goodbye"), new.getValue(1))
}

func TestLeafDelete(t *testing.T) {
	old := createBNode()

	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	{
		idx := old.lookUp([]byte("k3"))
		assert.Equal(t, uint16(1), idx)

		leafDelete(new, old, idx)

		assert.Equal(t, uint16(1), new.nKeys())

		assert.Equal(t, []byte("k1"), new.getKey(0))
		assert.Equal(t, []byte("hi"), new.getValue(0))
	}
	{
		idx := old.lookUp([]byte("k1"))
		assert.Equal(t, uint16(0), idx)

		leafDelete(new, old, idx)

		assert.Equal(t, uint16(1), new.nKeys())

		assert.Equal(t, []byte("k3"), new.getKey(0))
		assert.Equal(t, []byte("hello"), new.getValue(0))
	}
}

func TestMergeNode(t *testing.T) {

}
