package b_tree

import (
	"bytes"
	"encoding/binary"

	"github.com/connnorchen/MyDb/internal/util"
)

// returns position in the sorted KV list, the first kid node whose
// range intersects the key. (kid[i] <= key)
func nodeLookLE(node BNode, key []byte) uint16 {
    // the first key is copied from parent, thus it must LE than key
    left := uint16(0)
    right := node.nkeys() - 1
    for left + 1 < right {
        mid := (left + right) / 2
        if bytes.Compare(node.getKey(mid), key) >= 0 {
            right = mid
        } else {
            left = mid
        }
    }
    if bytes.Compare(node.getKey(left), key) >= 0 {
        if bytes.Compare(node.getKey(left), key) == 0 {
            return left
        }
        if left == 0 {
            panic("trying to find a number that is less than all numbers")
        }
        return left - 1
    } else if bytes.Compare(node.getKey(right), key) >= 0 {
        if bytes.Compare(node.getKey(right), key) == 0 {
            return right
        }
        return right - 1
    }
    // meaning left < key and right < key, so right must suffice
    return right
}

func nodeAppendRange(
    new BNode, old BNode, 
    dstNew uint16, srcOld uint16, n uint16,
) {
    util.Assert(srcOld + n <= old.nkeys())
    util.Assert(dstNew + n <= new.nkeys())
    if n == 0 {
        return
    }
    // copy over pointers
    for i := uint16(0); i < n; i++ {
        new.setPtr(i + dstNew, old.getPtr(i + srcOld))
    }
    // copy over offsets
    dstBegin := new.getOffset(dstNew)
    srcBegin := old.getOffset(srcOld)
    
    for i := uint16(1); i <= n; i++ {
        new.setOffset(i + dstNew, 
            dstBegin + old.getOffset(i + srcOld) - srcBegin)
    }

    kvStart := old.kvPos(srcOld)
    kvEnd := old.kvPos(srcOld + n)
    copy(new.data[new.kvPos(dstNew):], old.data[kvStart:kvEnd])
}

func nodeAppendKV(
    new BNode, idx uint16, ptr uint64, key []byte, val []byte,
) {
    // ptrs
    new.setPtr(idx, ptr)

    // KVs
    keyLength := uint16(len(key))
    valLength := uint16(len(val))
    newKvPos := new.kvPos(idx)
    binary.LittleEndian.PutUint16(new.data[newKvPos:], keyLength)
    binary.LittleEndian.PutUint16(new.data[newKvPos+2:], valLength)
    copy(new.data[newKvPos+4:], key)
    copy(new.data[newKvPos+4+keyLength:], val)

    new.setOffset(idx + 1, new.getOffset(idx) + 4 + keyLength + valLength)
}

// splits from idx to end, determine if it could be fit into a page
func splitFromIdxFitInOnePage(node BNode, idx uint16) bool {
    util.Assert(idx < node.nkeys())

    nkeys := node.nkeys() - idx
    kvSize := node.nbytes() - node.kvPos(idx)
    return (HEADER + nkeys * 8 + nkeys * 2 + kvSize) <= BTREE_PAGE_SIZE
}

// split a bigger-than-allowed node into two.
// the second node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
    // binary search on old node to find the biggest kvPos < BTREE_PAGE_SIZE
    l := uint16(0)
    r := old.nkeys() - 1
    for l + 1 < r {
        m := (l + r) / 2
        if splitFromIdxFitInOnePage(old, m) {
            r = m
        } else {
            l = m
        }
    }
    var startIdx uint16
    if splitFromIdxFitInOnePage(old, l) {
        startIdx = l
    } else {
        startIdx = r
    }
    // 0 ... startIdx - 1 are the smallest possible way to store in left
    left.setHeader(old.btype(), startIdx)
    nodeAppendRange(left, old, 0, 0, startIdx)
    // startIdx ... end will be biggest possible way to fit inside of a page
    right.setHeader(old.btype(), old.nkeys() - startIdx)
    nodeAppendRange(right, old, 0, startIdx, old.nkeys() - startIdx)
}

// split a node if it's too big, the results are 1~3 nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
    if old.nbytes() <= BTREE_PAGE_SIZE {
        return 1, [3]BNode{old}
    }

    left := BNode{make([]byte, 2 * BTREE_PAGE_SIZE)}
    right := BNode{make([]byte, BTREE_PAGE_SIZE)}
    nodeSplit2(left, right, old)
    if left.nbytes() <= BTREE_PAGE_SIZE {
        return 2, [3]BNode{left, right}
    }

    // the left side need to be further splitted
    leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
    leftright := BNode{make([]byte, BTREE_PAGE_SIZE)}
    nodeSplit2(leftleft, leftright, left)
    util.Assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
    return 3, [3]BNode{leftleft, leftright, right}
}

// replace one link with multiple links
func nodeReplaceKidN(
    tree *BTree, new BNode, old BNode, idx uint16,
    kids ...BNode,
) {
    inc := uint16(len(kids))
    new.setHeader(BNODE_NODE, old.nkeys() + inc - 1)
    nodeAppendRange(new, old, 0, 0, idx)
    for i, node := range kids {
        nodeAppendKV(new, uint16(i) + idx, tree.new(node), node.getKey(0), nil)
    }
    nodeAppendRange(new, old, idx + inc, idx + 1, old.nkeys() - idx - 1)
}

// discarding idx and idx + 1, place in ptr into idx
func nodeReplace2Kid(
    new BNode, node BNode, idx uint16, ptr uint64, 
    key []byte,
) {
    new.setHeader(BNODE_NODE, node.nkeys() - 1)
    nodeAppendRange(new, node, 0, 0, idx)
    nodeAppendKV(new, idx, ptr, key, nil)
    nodeAppendRange(new, node, idx + 1, idx + 2, node.nkeys() - idx - 2)
}
