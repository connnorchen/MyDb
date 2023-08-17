package b_tree

import (
	"bytes"

	"github.com/connnorchen/MyDb/internal/util"
)

// remove a key from a leaf node
func leafDelete(New BNode, old BNode, idx uint16) {
    New.setHeader(old.btype(), old.nkeys() - 1)
    nodeAppendRange(New, old, 0, 0, idx)
    nodeAppendRange(New, old, idx, idx + 1, old.nkeys() - idx - 1)
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
    idx := nodeLookLE(node, key)
    switch node.btype() {
    case BNODE_LEAF:
        if !bytes.Equal(key, node.getKey(idx)) {
            return BNode{} // key does not exist
        }
        New := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
        leafDelete(New, node, idx)
        return New
    case BNODE_NODE:
        return nodeDelete(tree, node, idx, key)
    default:
        panic("unknown BNode type")
    }
}

// delete a node from internal node
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
    ptr := node.getPtr(idx)
    updated := treeDelete(tree, tree.Get(ptr), key)
    if len(updated.Data) == 0 {
        return BNode{} // key does not exist
    }
    tree.Del(ptr)
    
    New := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
    // check for merging
    mergeDir, sibling := shouldMerge(tree, node, idx, updated)
    switch {
    case mergeDir < 0: // left
        merged := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
        nodeMerge(merged, sibling, updated)
        tree.Del(node.getPtr(idx - 1))
        nodeReplace2Kid(New, node, idx - 1, tree.New(merged), merged.getKey(0))
    case mergeDir > 0: // right
        merged := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
        nodeMerge(merged, updated, sibling)
        tree.Del(node.getPtr(idx + 1))
        nodeReplace2Kid(New, node, idx, tree.New(merged), merged.getKey(0))
    case mergeDir == 0: // no merge needed
        util.Assert(updated.nkeys() > 0)
        nodeReplaceKidN(tree, New, node, idx, updated)
    }
    return New
}

// sizeof(merge(left, right)) <= BTREE_PAGE_SIZE, this condition
// must be checked by the caller
func nodeMerge(merged BNode, left BNode, right BNode) {
    util.Assert(left.btype() == right.btype())
    merged.setHeader(left.btype(), left.nkeys() + right.nkeys())
    nodeAppendRange(merged, left, 0, 0, left.nkeys())
    nodeAppendRange(merged, right, left.nkeys(), 0, right.nkeys())
}

// can be merged as long as these two conditions suffice
// 1. node size is less than 1/4 of a page
// 2. has sibling and merge size is less than a page size
func shouldMerge(
    tree *BTree, node BNode, 
    idx uint16, updated BNode,
) (int, BNode) {
    if updated.nbytes() > BTREE_PAGE_SIZE / 4 {
        return 0, BNode{}
    }
    if idx > 0 {
        leftChildPtr := node.getPtr(idx - 1)
        leftChildNode := tree.Get(leftChildPtr)
        merged := leftChildNode.nbytes() + updated.nbytes() - HEADER
        if merged <= BTREE_PAGE_SIZE {
            return -1, leftChildNode
        }
    }
    if idx < node.nkeys() - 1 {
        rightChildPtr := node.getPtr(idx + 1)
        rightChildNode := tree.Get(rightChildPtr)
        merged := rightChildNode.nbytes() + updated.nbytes() - HEADER
        if merged <= BTREE_PAGE_SIZE {
            return +1, rightChildNode
        }
    }
    return 0, BNode{}
}
