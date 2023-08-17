package b_tree

import "bytes"

// get a key from tree
func treeGet(tree *BTree, node BNode, key[]byte) ([]byte, bool) {
    idx := nodeLookLE(node, key)

    switch node.btype() {
    case BNODE_LEAF:
        if bytes.Equal(key, node.getKey(idx)) {
            return node.getVal(idx), true
        } else {
            return nil, false // not found
        }
    case BNODE_NODE:
        childNode := tree.Get(node.getPtr(idx))
        return treeGet(tree, childNode, key)
    default:
        panic("unrecognized node type")
    }
}
