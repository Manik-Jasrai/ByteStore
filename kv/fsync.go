package kv

import (
	"fmt"
	"os"
	"path"
	"syscall"

	"github.com/Manik-Jasrai/ByteStore.git/btree"
	"golang.org/x/sys/unix"
)

// Opens the directory
// Opens or Creates the file in the same directory
// Fsyncs the file directory
func createFilesync(file string) (int, error) {
	// obtain the directory fd
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)
	// open or create file
	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}

	// fsync the file directory
	if err = syscall.Fsync(dirfd); err != nil {
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("fsync directory: %w", err)
	}
	return fd, nil
}

func writePages(db *KV) error {
	size := (int(db.page.flushed) + len(db.page.temp)) * btree.BTREE_PAGE_SIZE
	if err := extendMmap(db, size); err != nil {
		return err
	}

	offset := int64(db.page.flushed * btree.BTREE_PAGE_SIZE)
	if _, err := unix.Pwritev(db.fd, db.page.temp, offset); err != nil {
		return err
	}

	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

func updateFile(db *KV) error {
	// 1. Write new nodes
	if err := writePages(db); err != nil {
		return fmt.Errorf("writing pages %w: ", err)
	}
	// 2. fsync
	if err := syscall.Fsync(db.fd); err != nil {
		return err
	}

	// 3. loadMeta / updateRoot
	if err := updateMeta(db); err != nil {
		return fmt.Errorf("loading meta %w: ", err)
	}
	// 4. fsync
	return syscall.Fsync(db.fd)
}

// TODO : Second version
func updateOrRevert(db *KV, meta []byte) error {
	// if db.failed {
	// 	db.failed = false
	// }
	// 2 phase update
	// revert to previous root
	if err := updateFile(db); err != nil {
		// db.failed = true
		db.setMeta(meta)
		// discard temporaries
		db.page.temp = db.page.temp[:0]
	}
	return nil
}
