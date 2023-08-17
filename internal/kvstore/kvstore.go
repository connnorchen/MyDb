package kvstore

import (
	"fmt"
	"os"
	"syscall"

	"github.com/connnorchen/MyDb/internal/b_tree"
	"github.com/connnorchen/MyDb/internal/util"
)

type KV struct {
    Path string
    // internal
    fp *os.File
    tree b_tree.BTree
    mmap struct {
        file   int      // file size, can be larger than the database size
        total  int      // mmap size, can be larger than the file size
        chunks [][]byte // multiple mmaps, can be non-continuous
    }
    page struct {
        flushed uint64   // database size in number of pages
        temp    [][]byte // newly allocated pages
    }
}

// callback function for BTree, dereference a ptr
func (db *KV) pageGet(ptr uint64) b_tree.BNode {
    start := uint64(0)
    for _, chunk := range db.mmap.chunks {
        end := start + uint64(len(chunk)) / b_tree.BTREE_PAGE_SIZE
        if ptr < end {
            offset := b_tree.BTREE_PAGE_SIZE * (ptr - start)
            return b_tree.BNode{Data: chunk[offset:offset + b_tree.BTREE_PAGE_SIZE]}
        }
        start = end
    }
    panic("bad ptr")
}

// callback for BTree, allocate a new page
func (db *KV) pageNew(node b_tree.BNode) uint64 {
    // TODO: reuse deallocated pages
    fmt.Println(len(node.Data))
    util.Assert(len(node.Data) <= b_tree.BTREE_PAGE_SIZE)
    ptr := db.page.flushed + uint64(len(db.page.temp))
    db.page.temp = append(db.page.temp, node.Data)
    return ptr
}

// callback for BTree, deallocate a page
func (db *KV) pageDel(ptr uint64) {
    // TODO
}

func (db *KV) Open() error {
    // open or create the DB file
    fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return fmt.Errorf("OpenFile: %w", err)
    }
    db.fp = fp

    // create the initial mmap
    sz, chunk, err := mmapInit(db.fp)
    if err != nil {
        goto fail
    }
    db.mmap.file = sz
    db.mmap.total = len(chunk)
    db.mmap.chunks = [][]byte{chunk}

    // btree callbacks
    db.tree.Get = db.pageGet
    db.tree.New = db.pageNew
    db.tree.Del = db.pageDel

    // read the master page
    err = masterLoad(db)
    if err != nil {
        goto fail
    }

    // done 
    return nil

fail: 
    db.Close()
    return fmt.Errorf("KV.Open: %w", err)
}

func (db *KV) Get(key []byte) ([]byte, bool) {
    return db.tree.GetKey(key)
}

func (db *KV) Set(key []byte, val []byte) error {
    db.tree.Insert(key, val)
    return flushPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
    deleted := db.tree.DeleteKey(key)
    return deleted, flushPages(db)
}

// cleanups
func (db *KV) Close() {
    for _, chunk := range db.mmap.chunks {
        err := syscall.Munmap(chunk)
        util.Assert(err == nil)
    }
    _ = db.fp.Close()
}

