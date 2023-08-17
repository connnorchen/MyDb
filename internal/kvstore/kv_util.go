package kvstore

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/connnorchen/MyDb/internal/b_tree"
	"github.com/connnorchen/MyDb/internal/util"
)

// create the initial mmap that covers the whole file
func mmapInit(fp *os.File) (int, []byte, error) {
    fi, err := fp.Stat()
    if err != nil {
        return 0, nil, fmt.Errorf("stat: %w", err)
    }

    if fi.Size() % b_tree.BTREE_PAGE_SIZE != 0 {
        return 0, nil, errors.New("File size is not a multiple of page size")
    }

    mmapSize := 64 << 20
    util.Assert(mmapSize % b_tree.BTREE_PAGE_SIZE == 0)
    for mmapSize < int(fi.Size()) {
        mmapSize *= 2
    }
    // mmap size can be larger than the file size, the range past the end of
    // the file is not accessible (SIGBUG), but the file can be extended later
    chunk, err := syscall.Mmap(
        int(fp.Fd()), 0, mmapSize,
        syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
    )
    if err != nil {
        return 0, nil, fmt.Errorf("mmap: %w", err)
    }
    
    return int(fi.Size()), chunk, nil
}

// extend the mmap by adding new mappings
func extendMmap(db *KV, npages int) error {
    if db.mmap.total >= npages * b_tree.BTREE_PAGE_SIZE {
        return nil
    }

    // double the address space, the size of the new mapping increases
    // exponetially so that we don't have to call mmap frequently.
    chunk, err := syscall.Mmap(
        int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
        syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
    )
    if err != nil {
        return fmt.Errorf("mmap: %w", err)
    }

    db.mmap.total += db.mmap.total
    db.mmap.chunks = append(db.mmap.chunks, chunk)
    return nil
}

// extend the file to at least `npages`.
func extendFile(db *KV, npages int) error {
    filePages := db.mmap.file / b_tree.BTREE_PAGE_SIZE
    if filePages >= npages {
        return nil
    }

    for filePages < npages {
        // the file size is increased exponetially,
        // so that we don't have to extend the file for every update
        inc := filePages / 8
        if inc < 1 {
            inc = 1
        }
        filePages += inc
    }
    
    fileSize := filePages * b_tree.BTREE_PAGE_SIZE
    err := syscall.Ftruncate(int(db.fp.Fd()), int64(fileSize))
    if err != nil {
        return fmt.Errorf("fallocate: %w", err)
    }

    db.mmap.file = fileSize
    return nil
}

// persist the newly allocated pages after updates
func flushPages(db *KV) error {
    if err := writePages(db); err != nil {
        return err
    }
    return syncPages(db)
}

func writePages(db *KV) error {
    // extend the file & mmap if needed
    npages := int(db.page.flushed) + len(db.page.temp)
    // file extended at a rate of 1.125
    if err := extendFile(db, npages); err != nil {
        return err
    }
    // mmap extended at a rate of 2
    if err := extendMmap(db, npages); err != nil {
        return err
    }

    // copy data to the file
    for i, page := range db.page.temp {
        ptr := db.page.flushed + uint64(i)
        copy(db.pageGet(ptr).Data, page)
    }
    return nil
}

func syncPages(db *KV) error {
    // flush data to the disk, must be done before updating the master page.
    if err := db.fp.Sync(); err != nil {
        return fmt.Errorf("fsync: %w", err)
    }
    db.page.flushed += uint64(len(db.page.temp))
    db.page.temp = db.page.temp[:0]

    // update & flush the master page
    if err := masterStore(db); err != nil {
        return err
    }
    if err := db.fp.Sync(); err != nil {
        return err
    }
    return nil
}

