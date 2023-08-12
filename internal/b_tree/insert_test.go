package b_tree

import (
    "testing"

	"github.com/stretchr/testify/assert"
)

func assertSorted(t *testing.T, node BNode) {
    for i := uint16(0); i < node.nkeys() - 1; i++ {
        f := node.getKey(i)
        b := node.getKey(i + 1)
        assert.Less(t, f, b)
    }
}

func TestLeafInsert(t *testing.T) {
    new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    old := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    old.setHeader(BNODE_LEAF, 10)
    // old: 0, 2, 4, 6, 8, 10, 12, 14, 16, 18
    for i := uint16(0); i < 20; i += 2{
        nodeAppendKV(old, i / 2, 0, []byte{byte(i)}, []byte{byte(i)})
    }
    insertedKey1 := []byte{byte(3)}
    insertedVal1 := []byte{byte(3)}
    // inserted in between 2 and 4
    idx := uint16(2)
    leafInsert(new, old, idx, insertedKey1, insertedVal1)
    assert.Equal(t, new.nkeys(), old.nkeys() + 1)
    assert.Equal(t, new.getKey(idx), insertedKey1)
    assert.Equal(t, new.getVal(idx), insertedVal1)
    assertSorted(t, new)

    insertedKey2 := []byte{byte(20)}
    insertedVal2 := []byte{byte(20)}
    // inserted at the end of the list
    idx = uint16(10)
    leafInsert(new, old, idx, insertedKey2, insertedVal2)
    assert.Equal(t, new.nkeys(), old.nkeys() + 1)
    assert.Equal(t, new.getKey(idx), insertedKey2)
    assertSorted(t, new)
}

func TestLeafUpdate(t *testing.T) {
    new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    old := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    old.setHeader(BNODE_LEAF, 10)
    // old: 0, 2, 4, 6, 8, 10, 12, 14, 16, 18
    for i := uint16(0); i < 20; i += 2{
        nodeAppendKV(old, i / 2, 0, []byte{byte(i)}, []byte{byte(i)})
    }
    updatedKey1 := []byte{byte(3)}
    updatedVal1 := []byte{byte(3)}
    // 2 -> 3
    var idx uint16 = 1
    leafUpdate(new, old, idx, updatedKey1, updatedVal1)
    assert.Equal(t, new.nkeys(), old.nkeys())
    assert.Equal(t, new.getKey(idx), updatedKey1)
    assert.Equal(t, new.getVal(idx), updatedVal1)
    assertSorted(t, new)

    updatedKey2 := []byte{byte(20)}
    updatedVal2 := []byte{byte(20)}
    // 18 -> 20
    idx = 9
    leafUpdate(new, old, idx, updatedKey2, updatedVal2)
    assert.Equal(t, new.nkeys(), old.nkeys())
    assert.Equal(t, new.getKey(idx), updatedKey2)
    assert.Equal(t, new.getVal(idx), updatedVal2)
    assertSorted(t, new)
}

func TestNodeInsertAndTreeInsert(t *testing.T) {
    // root = intermediate node with 0 as the key
    root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    root.setHeader(BNODE_NODE, 1)
    nodeAppendKV(root, 0, 1, []byte{byte(0)}, nil)

    // child0 = default leaf node
    child0 := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    child0.setHeader(BNODE_LEAF, 1)
    nodeAppendKV(child0, 0, 0, []byte{byte(0)}, nil)

    tree := BTree{root: 0}
    nodeList := []BNode{}
    nodeList = append(nodeList, root)
    nodeList = append(nodeList, child0)
    tree.get = func(ptr uint64) BNode {
        return nodeList[ptr]
    }
    tree.new = func(node BNode) uint64 {
        nodeMapLength := len(nodeList)
        nodeList = append(nodeList, node)
        return uint64(nodeMapLength)
    }
    tree.del = func(ptr uint64) {
        nodeList[ptr] = BNode{}
    }
    root = treeInsert(&tree, root, []byte{byte(10)}, []byte{byte(10)})
    assert.Equal(t, root.nkeys(), uint16(1))
    child0 = tree.get(root.getPtr(0))
    // root, deleted_child0, actual_child0
    assert.Equal(t, len(nodeList), 3)
    assert.Equal(t, nodeList[2].nkeys(), uint16(2))
    assert.Equal(t, nodeList[2].getKey(1), []byte{byte(10)})
    assert.Equal(t, nodeList[2].getVal(1), []byte{byte(10)})

    // insert a big node such that it needs to split
    //         root: 0, 15
    // left: 0, 10      right: 15
    key := make([]byte, 1000)
    key[0] = byte(15)
    val := make([]byte, 3063)
    root = treeInsert(&tree, root, key, val)
    assert.Equal(t, root.nkeys(), uint16(2))
    assert.Equal(t, root.getKey(1), key)
    assert.Equal(t, root.getVal(1), []byte{})
    
    left_child := tree.get(root.getPtr(0))
    right_child := tree.get(root.getPtr(1))
    assert.Equal(t, left_child.nkeys(), uint16(2))
    assert.Equal(t, left_child.getKey(0), []byte{byte(0)})
    assert.Equal(t, left_child.getVal(0), []byte{})

    assert.Equal(t, right_child.nkeys(), uint16(1))
    assert.Equal(t, right_child.getKey(0), key)
    assert.Equal(t, right_child.getVal(0), val)

    // insert a small node
    //         root: 0, 15
    // left: 0, 10      right: 15, 17
    root = treeInsert(&tree, root, []byte{byte(17)}, nil)
    assert.Equal(t, root.nkeys(), uint16(2))
    assert.Equal(t, root.getKey(1), key)
    assert.Equal(t, root.getVal(1), []byte{})

    right_child = tree.get(root.getPtr(1))
    assert.Equal(t, right_child.nkeys(), uint16(2))
    assert.Equal(t, right_child.getKey(1), []byte{byte(17)})
    assert.Equal(t, right_child.getVal(1), []byte{})
    
    // insert a super big node, trigger a double split
    //      root: 0, 15, 16, 17
    // left: 0, 10    middle: 15  right1: 16 right2: 17
    key1 := make([]byte, 1000)
    val1 := make([]byte, 3078)
    key1[0] = byte(16)
    root = treeInsert(&tree, root, key1, val1)

    assert.Equal(t, root.nkeys(), uint16(4))
    left_child = tree.get(root.getPtr(0))
    middle_child := tree.get(root.getPtr(1))
    right1_child := tree.get(root.getPtr(2))
    right2_child := tree.get(root.getPtr(3))
    assert.Equal(t, left_child.nkeys(), uint16(2))
    assert.Equal(t, middle_child.nkeys(), uint16(1))
    assert.Equal(t, right1_child.nkeys(), uint16(1))
    assert.Equal(t, right2_child.nkeys(), uint16(1))
    
    assert.Equal(t, middle_child.getKey(0), key)
    assert.Equal(t, right1_child.getKey(0), key1)
    assert.Equal(t, right2_child.getKey(0), []byte{byte(17)})
}
