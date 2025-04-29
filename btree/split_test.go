package btree

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeSplit2(t *testing.T) {
	// Case 1: Normal split
	{
		var keys [][]byte
		var vals [][]byte
		for i := 0; i < 10; i++ {
			keys = append(keys, []byte{byte('a' + i)})
			vals = append(vals, bytes.Repeat([]byte{'x'}, 100)) // Each value ~100 bytes
		}
		old := CreateLeafwithKVs(keys, vals)

		left := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		right := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

		NodeSplit2(left, right, old)

		assert.GreaterOrEqual(t, left.nKeys()+right.nKeys(), uint16(4))
		assert.LessOrEqual(t, left.nBytes(), uint16(BTREE_PAGE_SIZE))
		assert.LessOrEqual(t, right.nBytes(), uint16(BTREE_PAGE_SIZE))

		// All keys should still be in order
		var allKeys [][]byte
		for i := uint16(0); i < left.nKeys(); i++ {
			allKeys = append(allKeys, left.getKey(i))
		}
		for i := uint16(0); i < right.nKeys(); i++ {
			allKeys = append(allKeys, right.getKey(i))
		}
		for i := uint16(0); i < uint16(len(allKeys)); i++ {
			assert.Equal(t, []byte{byte('a' + i)}, allKeys[i])
		}
	}

	// Case 2: Minimum valid split (edge case for nleft == 1)
	{
		var keys [][]byte
		var vals [][]byte
		for i := 0; i < 2; i++ {
			keys = append(keys, []byte{byte('k' + i)})
			vals = append(vals, bytes.Repeat([]byte{'x'}, 300)) // Force high byte size
		}
		old := CreateLeafwithKVs(keys, vals)

		left := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		right := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

		NodeSplit2(left, right, old)

		assert.Equal(t, left.nKeys()+right.nKeys(), uint16(2))
		assert.LessOrEqual(t, left.nBytes(), uint16(BTREE_PAGE_SIZE))
		assert.LessOrEqual(t, right.nBytes(), uint16(BTREE_PAGE_SIZE))
	}
}

func TestNodeSplit3(t *testing.T) {
	// Case 1: No split needed (fits in one page)
	{
		var keys [][]byte
		var vals [][]byte
		for i := 0; i < 5; i++ {
			keys = append(keys, []byte{byte('a' + i)})
			vals = append(vals, []byte("val"))
		}
		old := CreateLeafwithKVs(keys, vals)
		count, nodes := NodeSplit3(old)

		assert.Equal(t, uint16(1), count)
		assert.Equal(t, uint16(5), nodes[0].nKeys())
	}

	// Case 2: Split into 2 nodes
	{
		var keys [][]byte
		var vals [][]byte
		for i := 0; i < 50; i++ {
			keys = append(keys, []byte{byte('a' + i)})
			vals = append(vals, bytes.Repeat([]byte{'x'}, 100)) // ~2KB data
		}
		old := CreateLeafwithKVs(keys, vals)

		count, nodes := NodeSplit3(old)

		assert.Equal(t, uint16(2), count)
		assert.LessOrEqual(t, nodes[0].nBytes(), uint16(BTREE_PAGE_SIZE))
		assert.LessOrEqual(t, nodes[1].nBytes(), uint16(BTREE_PAGE_SIZE))

		// Keys should be intact and ordered
		var allKeys [][]byte
		for i := uint16(0); i < count; i++ {
			for j := uint16(0); j < nodes[i].nKeys(); j++ {
				allKeys = append(allKeys, nodes[i].getKey(j))
			}
		}
		for i := uint16(0); i < uint16(len(allKeys)); i++ {
			assert.Equal(t, []byte{byte('a' + i)}, allKeys[i])
		}
	}

	// Case 3: Split into 3 nodes
	{
		var keys [][]byte
		var vals [][]byte
		for i := 0; i < 71; i++ {
			keys = append(keys, []byte{byte('A' + i)})
			vals = append(vals, bytes.Repeat([]byte{'y'}, 100)) // ~5KB data
		}
		old := CreateLeafwithKVs(keys, vals)

		count, nodes := NodeSplit3(old)

		assert.Equal(t, uint16(3), count)
		var allKeys [][]byte
		totalKeys := uint16(0)
		for i := uint16(0); i < count; i++ {
			node := nodes[i]
			totalKeys += node.nKeys()
			for j := uint16(0); j < node.nKeys(); j++ {
				allKeys = append(allKeys, node.getKey(j))
			}
			assert.LessOrEqual(t, node.nBytes(), uint16(BTREE_PAGE_SIZE))
		}
		assert.Equal(t, uint16(71), totalKeys)
		for i := uint16(0); i < totalKeys; i++ {
			assert.Equal(t, []byte{byte('A' + i)}, allKeys[i])
		}
	}
}
