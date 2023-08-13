package b_tree

import (
    "testing"

     "github.com/stretchr/testify/assert"
)

func SetUpMockBTree(t *testing.T, tree *BTree) {
    // root = intermediate node with 0 as the key
    tree.root = 0
    root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    root.setHeader(BNODE_NODE, 1)
    nodeAppendKV(root, 0, 1, []byte{byte(0)}, nil)

    // child0 = default leaf node
    child0 := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    child0.setHeader(BNODE_LEAF, 1)
    nodeAppendKV(child0, 0, 0, []byte{byte(0)}, nil)

    tree.mockNodeList = append(tree.mockNodeList, root)
    tree.mockNodeList = append(tree.mockNodeList, child0)
    tree.get = func(ptr uint64) BNode {
        return tree.mockNodeList[ptr]
    }
    tree.new = func(node BNode) uint64 {
        nodeMapLength := len(tree.mockNodeList)
        tree.mockNodeList = append(tree.mockNodeList, node)
        return uint64(nodeMapLength)
    }
    tree.del = func(ptr uint64) {
        tree.mockNodeList[ptr] = BNode{}
    }
}

func TestBNodeHeader(t *testing.T) {
    testNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    testNode.setHeader(BNODE_NODE, 10)
    
    assert.Equal(t, testNode.btype(), uint16(BNODE_NODE))
    assert.Equal(t, testNode.nkeys(), uint16(10))
}

func TestBNodePtr(t *testing.T) {
    testNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    testNode.setHeader(BNODE_NODE, 10)
    assert.Equal(t, testNode.getPtr(0), uint64(0))

    r := uint64(999999999)
    testNode.setPtr(0, r)
    assert.Equal(t, testNode.getPtr(0), r)

    r = uint64(5201314)
    testNode.setPtr(1, r)
    assert.Equal(t, testNode.getPtr(1), r)
}

func TestBNodeOffset(t *testing.T) {
    testNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    testNode.setHeader(BNODE_NODE, 10)
    
    assert.Equal(t, testNode.getOffset(0), uint16(0))
    assert.Panics(
        t,
        func() { testNode.setOffset(0, 1) },
        "this code didn't panic",
    )

    testNode.setOffset(1, 100)
    assert.Equal(t, testNode.getOffset(1), uint16(100))
}

func TestGetKV(t *testing.T) {
    testNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    testNode.setHeader(BNODE_NODE, 10)
    for i := 0; i < 10; i++ {
        nodeAppendKV(testNode, uint16(i), 0, []byte{byte(i)}, []byte{byte(i)})
    }
    for i := 0; i < 10; i++ {
        assert.Equal(t, testNode.getKey(uint16(i)), []byte{byte(i)})
        assert.Equal(t, testNode.getVal(uint16(i)), []byte{byte(i)})
    }
}

func TestNBytes(t *testing.T) {
    testNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    testNode.setHeader(BNODE_NODE, 10)
    for i := 0; i < 10; i++ {
        nodeAppendKV(testNode, uint16(i), 0, []byte{byte(i)}, []byte{byte(i)})
    }
    assert.Equal(
        t,
        testNode.nbytes(),
        uint16(HEADER + 8 * 10 + 2 * 10 + 4 * 10 + 2 * 10),
    )
}
