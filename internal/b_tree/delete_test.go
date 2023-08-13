package b_tree

import (
    "testing"

	"github.com/stretchr/testify/assert"
)

func TestLeafDelete(t *testing.T) {
    old := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    old.setHeader(BNODE_LEAF, 3)
    for i := uint16(0); i < 3; i++ {
        nodeAppendKV(old, i, 0, []byte{byte(i)}, []byte{})
    }
    leafDelete(new, old, 0)
    assert.Equal(t, new.nkeys(), uint16(2))
    assert.Equal(t, new.getKey(0), []byte{byte(1)})
    assert.Equal(t, new.getKey(1), []byte{byte(2)})
}

func TestNodeMerge(t *testing.T) {
    left := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    right := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

    left.setHeader(BNODE_LEAF, 3)
    for i := uint16(0); i < 3; i++ {
        nodeAppendKV(left, i, 0, []byte{byte(i)}, []byte{byte(i)})
    }
    right.setHeader(BNODE_LEAF, 3)
    for i := uint16(0); i < 3; i++ {
        nodeAppendKV(right, i, 0, []byte{byte(3 + i)}, []byte{byte(3 + i)})
    }
    nodeMerge(merged, left, right)
    assert.Equal(t, merged.nkeys(), uint16(6))
    for i := uint16(0); i < 6; i++ {
        assert.Equal(t, merged.getKey(i), []byte{byte(i)})
        assert.Equal(t, merged.getVal(i), []byte{byte(i)})
    }
}

func TestShouldMerge(t *testing.T) {
    tree := BTree{}
    SetUpMockBTree(t, &tree)
    //         root: 0
    //  left: 0, 5
    root := tree.get(tree.root)
    key5 := make([]byte, 1000)
    val5 := make([]byte, 3063)
    key5[0] = byte(5)
    root = treeInsert(&tree, root, key5, val5)
    assert.Equal(t, root.nkeys(), uint16(1))

    leftChild := tree.get(root.getPtr(0))
    assert.Equal(t, leftChild.nkeys(), uint16(2))
    assert.Equal(t, leftChild.getKey(1), key5)
    assert.Equal(t, leftChild.getVal(1), val5)
    assert.Equal(t, leftChild.nbytes(), uint16(BTREE_PAGE_SIZE))

    //          root: 0, 7
    // left: 0, 5      right: 7
    key7 := []byte{byte(1)}
    val7 := []byte{byte(1)}
    key7[0] = byte(7)
    root = treeInsert(&tree, root, key7, val7)
    assert.Equal(t, root.nkeys(), uint16(2))
    assert.Equal(t, root.getKey(1), key7)
    rightChild := tree.get(root.getPtr(1))
    assert.Equal(t, rightChild.nkeys(), uint16(1))
    assert.Equal(t, rightChild.getKey(0), key7)
    assert.Equal(t, rightChild.getVal(0), val7)
    
    should, node := shouldMerge(&tree, root, 0, leftChild)
    assert.Equal(t, should, 0)
    assert.Equal(t, node, BNode{})

    should, node = shouldMerge(&tree, root, 1, rightChild)
    assert.Equal(t, should, 0)
    assert.Equal(t, node, BNode{})

    leftChild = tree.get(root.getPtr(0))
    newLeftChild := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    //          root: 0, 7
    // left: 0          right: 7
    leafDelete(newLeftChild, leftChild, 1)
    tree.mockNodeList[root.getPtr(0)] = newLeftChild
    should, node = shouldMerge(&tree, root, 0, newLeftChild)
    assert.Equal(t, should, 1)
    assert.Equal(t, node, rightChild)

    rightChild = tree.get(root.getPtr(1))
    should, node = shouldMerge(&tree, root, 1, rightChild)
    assert.Equal(t, should, -1)
    assert.Equal(t, node, newLeftChild)
}

func TestTreeDeleteAndNodeDelete(t *testing.T) {
    tree := BTree{}
    SetUpMockBTree(t, &tree)
    
    //      root: 0, 7
    // left: 0, 5    right: 7
    root := tree.get(tree.root)
    key5 := make([]byte, 1000)
    val5 := make([]byte, 3063)
    key5[0] = byte(5)
    root = treeInsert(&tree, root, key5, val5)
    key7 := []byte{byte(1)}
    val7 := []byte{byte(1)}
    key7[0] = byte(7)
    root = treeInsert(&tree, root, key7, val7)
    
    //      root: 0
    // left: 0, 7
    root = treeDelete(&tree, root, key5)
    assert.Equal(t, root.nkeys(), uint16(1))
    assert.Equal(t, root.getKey(0), []byte{byte(0)})
    
    childLeft := tree.get(root.getPtr(0))
    assert.Equal(t, childLeft.nkeys(), uint16(2))
    assert.Equal(t, childLeft.getKey(0), []byte{byte(0)})  
    assert.Equal(t, childLeft.getVal(0), []byte{})  
    
    assert.Equal(t, childLeft.getKey(1), key7)  
    assert.Equal(t, childLeft.getVal(1), val7)  

    //      root: 0
    // left: 0
    root = treeDelete(&tree, root, key7)
    assert.Equal(t, root.nkeys(), uint16(1))
    assert.Equal(t, root.getKey(0), []byte{byte(0)})
    
    childLeft = tree.get(root.getPtr(0))
    assert.Equal(t, childLeft.nkeys(), uint16(1))
    assert.Equal(t, childLeft.getKey(0), []byte{byte(0)})  
    assert.Equal(t, childLeft.getVal(0), []byte{})  

    // delete a non-existent key
    result := treeDelete(&tree, root, key7)
    assert.Equal(t, result, BNode{})
}
