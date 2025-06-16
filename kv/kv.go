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
		flushed uint64
		temp    [][]byte
	}

	free FreeList

	failed bool
}

func (db *KV) Open() error {
	// creating a file sync
	fd, err := createFilesync(db.Path)
	if err != nil {
		return err
	}
	// Storing the fd in our struct
	db.fd = fd

	// initialize mmap

	// Map the tree functions to implemented
	db.tree.SetGet(db.pageRead)
	// db.tree.SetNew(db.pageAlloc)
	db.tree.SetDel(db.free.PushTail)
	// Free list callbacks
	db.free.get = db.pageRead
	db.free.new = db.pageAppend
	// db.free.del = db.pageWrite

	fileSize := db.mmap.total * btree.BTREE_PAGE_SIZE
	err = readMeta(db, int64(fileSize))
	if err != nil {
		goto fail
	}
	return nil

fail:
	db.Close()
	return fmt.Errorf("KV Open %w:", err)
}

func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		if err := syscall.Munmap(chunk); err != nil {
			fmt.Errorf("KV Close %w:", err)
			return
		}
	}

	syscall.Close(db.fd)
}

func (db *KV) Set(key []byte, val []byte) error {
	meta := db.getMeta()
	db.tree.Insert(key, val)
	return updateOrRevert(db, meta)
}

// Btree.get
func (db *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)
	// 'start' tells us the starting page number of the chunk
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/btree.BTREE_PAGE_SIZE // No of pages(nodes) in a chunk
		// 'end' tells us the number of the page at the end of the chunk
		if ptr < end {
			// Our page is present in the chunk
			offset := btree.BTREE_PAGE_SIZE * (ptr - start) // position of our page
			node := btree.BNode{}
			node.SetData(chunk[offset : offset+btree.BTREE_PAGE_SIZE])
			return node
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
