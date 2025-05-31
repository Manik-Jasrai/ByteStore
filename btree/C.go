package btree

import (
	"unsafe"

	"github.com/Manik-Jasrai/ByteStore.git/utils"
)

// In Memory Testing
type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}

	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				utils.Assert(ok, "Node Exists")
				return node
			},
			new: func(node BNode) uint64 {
				utils.Assert(node.nBytes() <= BTREE_PAGE_SIZE, "Out of Bounds")
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				utils.Assert(ok, "Node Exists")
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) error {
	if err := c.tree.Insert([]byte(key), []byte(val)); err != nil {
		return err
	}
	c.ref[key] = val
	return nil
}
func (c *C) del(key string) (bool, error) {
	if _, err := c.tree.Delete([]byte(key)); err != nil {
		return false, err
	}
	delete(c.ref, key)
	return true, nil
}
