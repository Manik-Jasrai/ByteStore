package kv

import (
	"encoding/binary"
	"fmt"
	"syscall"

	"github.com/Manik-Jasrai/ByteStore.git/btree"
)

type KV struct {
	Path string

	fd   int
	tree btree.BTree

	mmap struct {
		total  int      // # of pages
		chunks [][]byte // list of pages
	}

	page struct {
		flushed uint64            // database size in number of pages
		nappend uint64            // number of pages to be appended
		updates map[uint64][]byte // pending updates
		temp    [][]byte
	}

	free FreeList

	// failed bool
}

func (db *KV) Open() error {
	// creating a file sync
	fd, err := createFilesync(db.Path)
	if err != nil {
		return err
	}
	// Storing the fd in our struct
	db.fd = fd

	db.page.updates = map[uint64][]byte{}

	// initialize mmap TODO
	fileSize, chunk, err := mmapInit(db)
	if err != nil {
		goto fail
	}
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	// Map the tree functions to implemented
	db.tree.SetGet(db.pageRead)
	db.tree.SetNew(db.pageAlloc)
	db.tree.SetDel(db.pageDel)
	// Free list callbacks
	db.free.get = db.pageRead
	db.free.new = db.pageAppend
	db.free.set = db.pageWrite

	err = readMeta(db, int64(fileSize))
	if err != nil {
		goto fail
	}
	return nil

fail:
	db.Close()
	return fmt.Errorf("KV Open %w : ", err)
}

func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		if err := syscall.Munmap(chunk); err != nil {
			return
		}
	}
	syscall.Close(db.fd)
}

// TODO
func (db *KV) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("empty key")
	}

	val := db.tree.Get(key)
	if val == nil {
		return nil, fmt.Errorf("key not found")
	}

	return val, nil
}
func (db *KV) Del(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("empty key")
	}

	// meta := db.getMeta()

	// Check if key exists
	if db.tree.Get(key) == nil {
		return fmt.Errorf("key not found")
	}

	// Delete from tree
	db.tree.Delete(key)

	return updateFile(db)
}

func (db *KV) Set(key []byte, val []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("empty key")
	}
	// meta := db.getMeta()
	db.tree.Insert(key, val)
	return updateFile(db)
}

// Btree.get, read a page
func (db *KV) pageRead(ptr uint64) []byte {
	if node, ok := db.page.updates[ptr]; ok {
		return node
	}

	return db.pageReadFile(ptr)
}

func (db *KV) pageReadFile(ptr uint64) []byte {
	start := uint64(0)
	// 'start' tells us the starting page number of the chunk
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/btree.BTREE_PAGE_SIZE // No of pages(nodes) in a chunk
		// 'end' tells us the number of the page at the end of the chunk
		if ptr < end {
			// Our page is present in the chunk
			offset := btree.BTREE_PAGE_SIZE * (ptr - start) // position of our page

			return chunk[offset : offset+btree.BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

func (db *KV) pageAppend(node []byte) uint64 {
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node)

	return ptr
}

// Btree.new , allocate a new page
func (db *KV) pageAlloc(node []byte) uint64 {
	// we check the free list first for an empty page
	if ptr := db.free.PopHead(); ptr != 0 {
		db.page.updates[ptr] = node
		return ptr
	}

	return db.pageAppend(node)
}

// Btree.del
func (db *KV) pageDel(ptr uint64) {

	delete(db.page.updates, ptr)
	db.free.PushTail(ptr)

	return
}

// FreeList.set, updates an existing page
func (db *KV) pageWrite(ptr uint64) []byte {
	if node, ok := db.page.updates[ptr]; ok {
		return node
	}
	node := make([]byte, btree.BTREE_PAGE_SIZE)
	copy(node, db.pageReadFile(ptr))

	db.page.updates[ptr] = node
	return node
}

func (db *KV) getMeta() []byte {
	var data [32]byte

	copy(data[0:], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.GetRoot())
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}
func (db *KV) setMeta(data []byte) {
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	db.tree.SetRoot(root)
	db.page.flushed = used
}
