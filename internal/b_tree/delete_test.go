package b_tree

import (
    "testing"

	"github.com/stretchr/testify/assert"
)

func TestLeafDelete(t *testing.T) {
    old := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
    new := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
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
    left := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
    right := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
    merged := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}

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
    //         Root: 0
    //  left: 0, 5
    Root := tree.Get(tree.Root)
    key5 := make([]byte, 1000)
    val5 := make([]byte, 3063)
    key5[0] = byte(5)
    Root = treeInsert(&tree, Root, key5, val5)
    assert.Equal(t, Root.nkeys(), uint16(1))

    leftChild := tree.Get(Root.getPtr(0))
    assert.Equal(t, leftChild.nkeys(), uint16(2))
    assert.Equal(t, leftChild.getKey(1), key5)
    assert.Equal(t, leftChild.getVal(1), val5)
    assert.Equal(t, leftChild.nbytes(), uint16(BTREE_PAGE_SIZE))

    //          Root: 0, 7
    // left: 0, 5      right: 7
    key7 := []byte{byte(1)}
    val7 := []byte{byte(1)}
    key7[0] = byte(7)
    Root = treeInsert(&tree, Root, key7, val7)
    assert.Equal(t, Root.nkeys(), uint16(2))
    assert.Equal(t, Root.getKey(1), key7)
    rightChild := tree.Get(Root.getPtr(1))
    assert.Equal(t, rightChild.nkeys(), uint16(1))
    assert.Equal(t, rightChild.getKey(0), key7)
    assert.Equal(t, rightChild.getVal(0), val7)
    
    should, node := shouldMerge(&tree, Root, 0, leftChild)
    assert.Equal(t, should, 0)
    assert.Equal(t, node, BNode{})

    should, node = shouldMerge(&tree, Root, 1, rightChild)
    assert.Equal(t, should, 0)
    assert.Equal(t, node, BNode{})

    leftChild = tree.Get(Root.getPtr(0))
    newLeftChild := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
    //          Root: 0, 7
    // left: 0          right: 7
    leafDelete(newLeftChild, leftChild, 1)
    tree.mockNodeList[Root.getPtr(0)] = newLeftChild
    should, node = shouldMerge(&tree, Root, 0, newLeftChild)
    assert.Equal(t, should, 1)
    assert.Equal(t, node, rightChild)

    rightChild = tree.Get(Root.getPtr(1))
    should, node = shouldMerge(&tree, Root, 1, rightChild)
    assert.Equal(t, should, -1)
    assert.Equal(t, node, newLeftChild)
}

func TestTreeDeleteAndNodeDelete(t *testing.T) {
    tree := BTree{}
    SetUpMockBTree(t, &tree)
    
    //      Root: 0, 7
    // left: 0, 5    right: 7
    Root := tree.Get(tree.Root)
    key5 := make([]byte, 1000)
    val5 := make([]byte, 3063)
    key5[0] = byte(5)
    Root = treeInsert(&tree, Root, key5, val5)
    key7 := []byte{byte(1)}
    val7 := []byte{byte(1)}
    key7[0] = byte(7)
    Root = treeInsert(&tree, Root, key7, val7)
    
    //      Root: 0
    // left: 0, 7
    Root = treeDelete(&tree, Root, key5)
    assert.Equal(t, Root.nkeys(), uint16(1))
    assert.Equal(t, Root.getKey(0), []byte{byte(0)})
    
    childLeft := tree.Get(Root.getPtr(0))
    assert.Equal(t, childLeft.nkeys(), uint16(2))
    assert.Equal(t, childLeft.getKey(0), []byte{byte(0)})  
    assert.Equal(t, childLeft.getVal(0), []byte{})  
    
    assert.Equal(t, childLeft.getKey(1), key7)  
    assert.Equal(t, childLeft.getVal(1), val7)  

    //      Root: 0
    // left: 0
    Root = treeDelete(&tree, Root, key7)
    assert.Equal(t, Root.nkeys(), uint16(1))
    assert.Equal(t, Root.getKey(0), []byte{byte(0)})
    
    childLeft = tree.Get(Root.getPtr(0))
    assert.Equal(t, childLeft.nkeys(), uint16(1))
    assert.Equal(t, childLeft.getKey(0), []byte{byte(0)})  
    assert.Equal(t, childLeft.getVal(0), []byte{})  

    // delete a non-existent key
    result := treeDelete(&tree, Root, key7)
    assert.Equal(t, result, BNode{})
}
