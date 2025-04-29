package btree

import (
	"bytes"
	"errors"

	"github.com/Manik-Jasrai/ByteStore.git/utils"
)

const HEADER = 4
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

type BTree struct {
	root uint64

	get func(uint64) BNode
	new func(BNode) uint64
	del func(uint64)
}

func CheckLimit(key []byte, val []byte) error {
	if len(key) <= BTREE_MAX_KEY_SIZE && len(val) <= BTREE_MAX_VAL_SIZE {
		return nil
	}
	return errors.New("Out of Bound KV")
}

func (tree *BTree) Insert(key []byte, val []byte) error {
	// Check for limit of KV
	if err := CheckLimit(key, val); err != nil {
		return err
	}
	// No tree exists Create a tree
	if tree.root == 0 {
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(root, 0, 0, nil, nil) // Sentinel value
		nodeAppendKV(root, 1, 0, key, val)

		tree.root = tree.new(root)
		return nil
	}
	// Insert KV and we get our updated root
	node := TreeInsert(tree, tree.get(tree.root), key, val)
	// Split the new node coz maybe out of page limit
	nspilt, split := NodeSplit3(node)

	if nspilt > 1 {
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		// TODO
		root.setHeader(BNODE_NODE, nspilt)
		for i, knode := range split[:nspilt] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}

	return nil
}

func (tree *BTree) Delete(key []byte) (bool, error) {
	if tree.root == 0 {
		// return false, error
	}

	updated := TreeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		
	}

}

func TreeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// result node
	// we keep it larger than page size so it result exceeds we will spit in two
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	idx := node.lookUp(key)
	switch node.bType() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			// Update
			// Since updating same position so we put idx
			leafUpdate(new, node, idx, key, val)
		} else {
			// Insert it after idx so we do +1
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// Update Leaf
		kptr := node.getPtr(idx)
		knode := TreeInsert(tree, tree.get(kptr), key, val)
		// Split
		nsplit, split := NodeSplit3(knode)
		// Deallocate previous node
		tree.del(kptr)
		// update N kid links
		NodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
	default:
		panic("Bad Node!")
	}

	return new
}

func TreeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := node.lookUp(key)

	switch node.bType() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // Not Found
		}

		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return NodeDelete(tree, node, idx, key)
	default:
		panic("Bad Node")
	}
}

func NodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.getPtr(idx)

	updated := TreeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{} // Not Found
	}
	tree.del(kptr)

	// should Merge
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	switch {
	case mergeDir == -1:
		nodeMerge(new, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		// nodereplace2kid
	case mergeDir == 1:
		nodeMerge(new, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		// nodereplace2kid
	case mergeDir == 0 && updated.nKeys() == 0:
		utils.Assert(node.nKeys() == 1 && idx == 0, "Bad")
		node.setHeader(node.bType(), 0)
	case mergeDir == 0 && updated.nKeys() > 1:
		NodeReplaceKidN(tree, new, node, idx, updated)
	}

	return new
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nBytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{} // No Merging
	}

	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nBytes() + updated.nBytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nKeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nBytes() + updated.nBytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return 1, sibling
		}
	}
	return 0, BNode{}
}
