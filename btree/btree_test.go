package btree

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertSingleKey(t *testing.T) {
	c := newC()
	c.add("k1", "hello")

	tree := c.tree
	root := tree.get(tree.root)

	// Root should be a leaf with 2 keys (sentinel + actual key)
	assert.Equal(t, BNODE_LEAF, root.bType())
	assert.Equal(t, uint16(2), root.nKeys())

	// Sentinel key is at index 0 (empty key)
	assert.Equal(t, []byte{}, root.getKey(0))
	assert.Equal(t, []byte{}, root.getValue(0))

	// Actual inserted key-value at index 1
	assert.Equal(t, []byte("k1"), root.getKey(1))
	assert.Equal(t, []byte("hello"), root.getValue(1))
}

func TestInsertDuplicateUpdatesValue(t *testing.T) {
	c := newC()
	c.add("k1", "val1")
	c.add("k1", "val2")

	root := c.tree.get(c.tree.root)
	assert.Equal(t, []byte("val2"), root.getValue(1))
}

func TestInsertMultipleOrdered(t *testing.T) {
	c := newC()
	keys := []string{"k1", "k2", "k3"}
	for _, k := range keys {
		c.add(k, "val"+k[1:])
	}

	root := c.tree.get(c.tree.root)
	assert.Equal(t, uint16(4), root.nKeys()) // 3 + sentinel

	assert.Equal(t, []byte("k1"), root.getKey(1))
	assert.Equal(t, []byte("val1"), root.getValue(1))
	assert.Equal(t, []byte("k2"), root.getKey(2))
	assert.Equal(t, []byte("k3"), root.getKey(3))
}

func TestInsertSplitAndPromote(t *testing.T) {
	c := newC()

	// Insert enough to overflow page (BNode Split threshold is about 3K+)
	for i := 0; i < 25; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := strings.Repeat("v", 150) // Large enough value to trigger page overflow
		err := c.add(k, v)
		assert.NoError(t, err)
	}

	root := c.tree.get(c.tree.root)
	assert.Equal(t, BNODE_NODE, root.bType())
	assert.Greater(t, root.nKeys(), uint16(1)) // Should have >1 child

	// There is a split in middle at 12th node
	// 0-11 on one side and 12-24 on other
	assert.Equal(t, uint16(0), root.lookUp([]byte("k11")))
	assert.Equal(t, uint16(1), root.lookUp([]byte("k12")))
}

func TestDeleteNonexistentKey(t *testing.T) {
	c := newC()
	c.add("k1", "val1")

	ok, err := c.tree.Delete([]byte("does-not-exist"))
	assert.False(t, ok)
	assert.Error(t, err)
}

func TestDeleteExistingKey(t *testing.T) {
	c := newC()
	c.add("k1", "val1")
	c.add("k2", "val2")

	ok, err := c.del("k1")
	assert.True(t, ok)
	assert.NoError(t, err)

	root := c.tree.get(c.tree.root)
	assert.Equal(t, uint16(2), root.nKeys()) // Sentinel + 1 key
	assert.Equal(t, []byte("k2"), root.getKey(1))
}

func TestDeleteLowerLevel(t *testing.T) {
	c := newC()

	// Fill enough to cause split first
	for i := 0; i < 25; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := strings.Repeat("v", 150)
		c.add(k, v)
	}

	ok, err := c.del("k01")
	assert.True(t, ok)
	assert.NoError(t, err)

	root := c.tree.get(c.tree.root)
	node := c.tree.get(root.getPtr(0))

	assert.NotEqual(t, []byte("k01"), node.getKey(1))
}

func TestShouldMerge(t *testing.T) {
	makeLeaf := func(kvCount int, valSize int) BNode {
		n := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		n.setHeader(BNODE_LEAF, uint16(kvCount+1)) // +1 for sentinel
		nodeAppendKV(n, 0, 0, nil, nil)
		for i := 1; i <= kvCount; i++ {
			k := []byte(fmt.Sprintf("k%d", i))
			v := bytes.Repeat([]byte("v"), valSize)
			nodeAppendKV(n, uint16(i), 0, k, v)
		}
		return n
	}

	siblingLeft := makeLeaf(2, 100)   // Small sibling
	siblingRight := makeLeaf(2, 100)  // Small sibling
	updatedSmall := makeLeaf(1, 50)   // Small enough to merge
	updatedLarge := makeLeaf(10, 200) // Too big to merge

	node := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	node.setHeader(BNODE_NODE, 3)
	nodeAppendKV(node, 0, 101, []byte("a"), nil)
	nodeAppendKV(node, 1, 102, []byte("b"), nil)
	nodeAppendKV(node, 2, 103, []byte("c"), nil)

	// Fake storage mapping
	fakeMap := map[uint64]BNode{
		101: siblingLeft,
		102: updatedSmall,
		103: siblingRight,
	}

	tree := &BTree{
		get: func(ptr uint64) BNode {
			return fakeMap[ptr]
		},
		new: func(n BNode) uint64 { return 999 },
		del: func(ptr uint64) {},
	}
	// t.Run("MergeWithLeftSibling", func(t *testing.T) {
	// 	dir, sib := shouldMerge(tree, node, 1, updatedSmall)
	// 	assert.Equal(t, -1, dir)
	// 	assert.Equal(t, siblingLeft.data, sib.data)
	// })

	t.Run("NoMergeDueToLargeUpdated", func(t *testing.T) {
		dir, sib := shouldMerge(tree, node, 1, updatedLarge)
		assert.Equal(t, 0, dir)
		assert.Equal(t, 0, len(sib.data))
	})

	t.Run("NoMergeIfSiblingsTooBig", func(t *testing.T) {
		// Increase sibling size
		bigSibling := makeLeaf(19, 200)
		fmt.Print(bigSibling.nBytes())
		fakeMap[101] = bigSibling
		fakeMap[103] = bigSibling
		dir, _ := shouldMerge(tree, node, 1, updatedSmall)
		assert.Equal(t, 0, dir)
	})

	t.Run("MergeWithRightSibling", func(t *testing.T) {
		// Shift updatedSmall to index 0, now right sibling is valid
		node.setHeader(BNODE_NODE, 2)
		nodeAppendKV(node, 0, 102, []byte("a"), nil)
		nodeAppendKV(node, 1, 103, []byte("b"), nil)

		fakeMap[102] = updatedSmall
		fakeMap[103] = siblingRight

		dir, sib := shouldMerge(tree, node, 0, updatedSmall)
		assert.Equal(t, 1, dir)
		assert.Equal(t, siblingRight.data, sib.data)
	})
}

func TestDeleteTriggersMerge(t *testing.T) {
	c := newC()

	// Fill enough to cause split first
	for i := 0; i < 25; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := strings.Repeat("v", 150)
		c.add(k, v)
	}
	// Test for Splitting
	root := c.tree.get(c.tree.root)
	assert.Equal(t, BNODE_NODE, root.bType())
	assert.Greater(t, root.nKeys(), uint16(1)) // Should have >1 child

	// Delete most keys
	for i := 1; i < 25; i++ {
		k := fmt.Sprintf("k%02d", i)
		ok, err := c.del(k)
		assert.True(t, ok)
		assert.NoError(t, err)
	}

	// Only one key should remain
	newRoot := c.tree.get(c.tree.root)
	assert.Equal(t, BNODE_LEAF, newRoot.bType())
	assert.Equal(t, []byte("k00"), newRoot.getKey(1))
}
