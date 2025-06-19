package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"

	"github.com/Manik-Jasrai/ByteStore.git/btree"
)

const DB_SIG = "0123456789ABCDEF"

// New Meta Page
/*
| sig | root_ptr | page_used | head_page | head_seq | tail_page | tail_seq |
| 16B |    8B    |     8B    |     8B    |    8B    |     8B    |    8B    |
*/

// Reading meta data from storage and putting it to KV data structure
func readMeta(db *KV, fileSize int64) error {
	if fileSize == 0 || db.mmap.total == 0 {
		db.page.flushed = 2 // reserve 2 pages, 1 meta page and 1 fl node
		db.free.headPage = 1
		db.free.tailPage = 1

		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	// verify the page
	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("bad signature")
	}

	bad := (used >= 1 && used <= uint64(db.mmap.total)/btree.BTREE_PAGE_SIZE)
	bad = bad || !(root > 0 && root < used)
	if bad {
		return errors.New("bad master page")
	}
	db.setMeta(data)
	return nil
}

// Loading meta data from KV data structure to storage
func updateMeta(db *KV) error {
	if _, err := syscall.Pwrite(db.fd, db.getMeta(), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}
