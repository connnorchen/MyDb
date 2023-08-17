package kvstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/connnorchen/MyDb/internal/b_tree"
)

const DB_SIG = "BuildYourOwnDB05"

// the master page format
// it contains the pointer to the root and other important bits.
// | sig | btree_root | page_used |
// | 16B |     8B     |     8B    |

func masterLoad(db *KV) error {
    if db.mmap.file == 0 {
        // empty file, the master page will be created on the first write.
        db.page.flushed = 1 // reserved for the master page
        return nil
    }
    
    data := db.mmap.chunks[0]
    root := binary.LittleEndian.Uint64(data[16:])
    used := binary.LittleEndian.Uint64(data[24:])
    // verified the page
    if !bytes.Equal([]byte(DB_SIG), data[:16]) {
        return errors.New("Bad signature")
    }
    bad := !(1 <= used && used <= uint64(db.mmap.file / b_tree.BTREE_PAGE_SIZE))
    bad = bad || !(0 <= root && root < used)
    if bad {
        return errors.New("Bad master page")
    }

    db.tree.Root = root
    db.page.flushed = used
    return nil
}

// update the master page. it must be atomic
func masterStore(db *KV) error {
    var data [32]byte
    copy(data[:16], []byte(DB_SIG))
    binary.LittleEndian.PutUint64(data[16:], db.tree.Root)
    binary.LittleEndian.PutUint64(data[24:], db.page.flushed)

    // NOTE: Updating the page via mmap is not atomic.
    // Use the `pwrite()` syscall instead
    _, err := db.fp.WriteAt(data[:], 0)
    if err != nil {
        return fmt.Errorf("write master page: %w", err)
    }
    return nil
}
