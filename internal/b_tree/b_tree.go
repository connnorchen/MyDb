package b_tree

import (
	"encoding/binary"

	"github.com/connnorchen/MyDb/internal/util"
)

// b_tree node basic structure, we use same structures for internal node &
// leaf node.
// | type | nkeys | pointers    | offsets   | key-values
// | 2B   | 2B    |  nkeys * 8B | nkeys * 2B| ...

// | klen | vlen  |  key |  val |
// | 2B   | 2B    | ...  |  ... |
type BNode struct {
    data []byte // can be dumped to disk
}

const (
    BNODE_NODE = 1 // internal node without value
    BNODE_LEAF = 2 // leaf node with value
)

const (
    HEADER = 4 // type + nkeys
    BTREE_PAGE_SIZE = 4096
    BTREE_MAX_KEY_SIZE = 1000
    BTREE_MAX_VALUE_SIZE = 3000
)

type BTree struct {
    // pointer (a nonzero page number)
    root uint64
    // callbacks for managing on-disk pages
    get func(uint64) BNode // dereference a pointer
    new func(BNode) uint64 // allocate a new page
    del func(uint64)       // deallocate a new page

    mockNodeList []BNode   // for testing usage
}

func init() { 
    node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VALUE_SIZE
    util.Assert(node1max <= BTREE_PAGE_SIZE)
}

// decoding BNode
// header 
func (node BNode) btype() uint16 {
    return binary.LittleEndian.Uint16(node.data)    
}

func (node BNode) nkeys() uint16 {
    return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
    binary.LittleEndian.PutUint16(node.data, btype)
    binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
    util.Assert(idx < node.nkeys())
    index := HEADER + idx * 8
    return binary.LittleEndian.Uint64(node.data[index:])
}

func (node BNode) setPtr(idx uint16, ptr uint64) {
    util.Assert(idx < node.nkeys());
    index := HEADER + idx * 8
    binary.LittleEndian.PutUint64(node.data[index:], ptr);
}

// The offset is relative to the position of the first KV pair.
// The offset of the first KV pair is always zero, so it is not stored in the 
// list. 

// important: 
// We store the offset to the end of the last KV pair in the offset list,
// which is used to determine the size of the node.
// |1st node offset| ... |n - 1th node offset| end of node offset|
// there are n offset nums in offset list

func offsetPos(node BNode, idx uint16) uint16 {
    util.Assert(1 <= idx && idx <= node.nkeys())
    nkeys := node.nkeys()
    return HEADER + nkeys * 8 + (idx - 1) * 2
}

func (node BNode) getOffset(idx uint16) uint16 {
    if idx == 0 {
        return 0
    }
    pos := offsetPos(node, idx)
    return binary.LittleEndian.Uint16(node.data[pos:])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
    pos := offsetPos(node, idx)
    binary.LittleEndian.PutUint16(node.data[pos:], offset)
}

// key-values
// kvPos(nkeys) returns the size of data
func (node BNode) kvPos(idx uint16) uint16 {
    util.Assert(idx <= node.nkeys())
    return HEADER + node.nkeys() * 8 + node.nkeys() * 2 + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
    util.Assert(idx < node.nkeys())
    kvPos := node.kvPos(idx)
    keyLength := binary.LittleEndian.Uint16(node.data[kvPos:])
    return node.data[kvPos+4:][:keyLength]
}

func (node BNode) getVal(idx uint16) []byte {
    util.Assert(idx < node.nkeys())
    kvPos := node.kvPos(idx)
    keyLength := binary.LittleEndian.Uint16(node.data[kvPos:])
    valLength := binary.LittleEndian.Uint16(node.data[kvPos + 2:])
    return node.data[kvPos+4+keyLength:][:valLength]
}

func (node BNode) nbytes() uint16 {
    return node.kvPos(node.nkeys())
}

