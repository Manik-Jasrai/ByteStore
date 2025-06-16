package kv

import (
	"fmt"
	"syscall"
)

// TODO :
// func mmapInit(db *KV) (int, []byte, error) {

// }

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
