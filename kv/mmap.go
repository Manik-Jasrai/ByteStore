package kv

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/Manik-Jasrai/ByteStore.git/btree"
	"github.com/Manik-Jasrai/ByteStore.git/utils"
)

// TODO :
func mmapInit(db *KV) (int, []byte, error) {
	fi, err := os.Stat(db.Path)
	if err != nil {
		return 0, nil, fmt.Errorf("error : %w", err)
	}
	size := fi.Size()
	if size%btree.BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size")
	}

	mmapSize := 64 << 20
	utils.Assert(mmapSize%btree.BTREE_PAGE_SIZE == 0, "MMap size is not a multiple of page size.")
	for mmapSize < int(size) {
		mmapSize *= 2
	}

	chunk, err := syscall.Mmap(db.fd, 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap:%w", err)
	}
	return int(size), chunk, nil

}

func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil
	}

	alloc := max(db.mmap.total, 64<<20)
	for db.mmap.total+alloc < size {
		alloc *= 2
	}

	chunk, err := syscall.Mmap(db.fd, int64(db.mmap.total), alloc, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap %w", err)
	}

	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}
