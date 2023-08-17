package b_tree

import (
	"testing"
	"unsafe"

	"github.com/connnorchen/MyDb/internal/util"
	"github.com/stretchr/testify/assert"
)

func SetUpMockBTree(t *testing.T, tree *BTree) {
    // Root = intermediate node with 0 as the key
    tree.Root = 0
    Root := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
    Root.setHeader(BNODE_NODE, 1)
    nodeAppendKV(Root, 0, 1, []byte{byte(0)}, nil)

    // child0 = default leaf node
    child0 := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
    child0.setHeader(BNODE_LEAF, 1)
    nodeAppendKV(child0, 0, 0, []byte{byte(0)}, nil)

    tree.mockNodeList = append(tree.mockNodeList, Root)
    tree.mockNodeList = append(tree.mockNodeList, child0)
    tree.Get = func(ptr uint64) BNode {
        return tree.mockNodeList[ptr]
    }
    tree.New = func(node BNode) uint64 {
        nodeMapLength := len(tree.mockNodeList)
        tree.mockNodeList = append(tree.mockNodeList, node)
        return uint64(nodeMapLength)
    }
    tree.Del = func(ptr uint64) {
        tree.mockNodeList[ptr] = BNode{}
    }
}

func TestBNodeHeader(t *testing.T) {
    testNode := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
    testNode.setHeader(BNODE_NODE, 10)
    
    assert.Equal(t, testNode.btype(), uint16(BNODE_NODE))
    assert.Equal(t, testNode.nkeys(), uint16(10))
}

func TestBNodePtr(t *testing.T) {
    testNode := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
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
    testNode := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
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
    testNode := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
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
    testNode := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
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

type C struct {
    tree  BTree
    pages map[uint64]BNode
}

func newC() *C {
    pages := map[uint64]BNode{}
    return &C{
        tree: BTree{
            Get: func(ptr uint64) BNode {
                node, ok := pages[ptr]
                util.Assert(ok)
                return node
            },
            New: func(node BNode) uint64 {
                util.Assert(node.nbytes() <= BTREE_PAGE_SIZE)
                key := uint64(uintptr(unsafe.Pointer(&node.Data[0])))
                util.Assert(pages[key].Data == nil)
                pages[key] = node
                return key
            },
            Del: func(ptr uint64) {
                _, ok := pages[ptr]
                util.Assert(ok)
                delete(pages, ptr)
            },
        },
        pages: pages,
    }
}

func TestBTreeInsert(t *testing.T) {
    container := newC()
    assert.Equal(t, container.tree.Root, uint64(0))
    
    // edge case checking
    keyTooLong := make([]byte, 1001)
    assert.Panics(t, func() {container.tree.Insert(keyTooLong, nil)})
    valTooLong := make([]byte, 3001)
    assert.Panics(t, func() {container.tree.Insert([]byte{byte(0)}, valTooLong)})
    assert.Panics(t, func() {container.tree.Insert(nil, nil)})
    
    // add a long key, val pair
    //          Root: 0, 5
    
    key5 := make([]byte, 1000)
    key5[0] = byte(5)
    val5 := make([]byte, 200)

    container.tree.Insert(key5, val5)

    assert.NotEqual(t, container.tree.Root, 0)
    Root := container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.btype(), uint16(BNODE_LEAF))
    assert.Equal(t, Root.nkeys(), uint16(2))
    assert.Equal(t, Root.getKey(0), []byte{})
    assert.Equal(t, Root.getVal(0), []byte{})
    assert.Equal(t, Root.getKey(1), key5)
    assert.Equal(t, Root.getVal(1), val5)
    val5 = make([]byte, 3000)
    // update operation
    container.tree.Insert(key5, val5)

    // add a long key, val pair
    // 1: Root: 0, 5, 7
    // 2:          Root: 0, 7
    //     left: 0, 5            right: 7
    key7 := make([]byte, 1000)
    key7[0] = byte(7)
    val7 := make([]byte, 3000)
    container.tree.Insert(key7, val7)
    
    Root = container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.btype(), uint16(BNODE_NODE))
    assert.Equal(t, Root.nkeys(), uint16(2))
    assert.Equal(t, Root.getKey(0), []byte{})
    assert.Equal(t, Root.getVal(0), []byte{})
    assert.Equal(t, Root.getKey(1), key7)
    assert.Equal(t, Root.getVal(1), []byte{})

    leftChild := container.tree.Get(Root.getPtr(0))
    assert.Equal(t, leftChild.btype(), uint16(BNODE_LEAF))
    assert.Equal(t, leftChild.nkeys(), uint16(2))
    assert.Equal(t, leftChild.getKey(0), []byte{})
    assert.Equal(t, leftChild.getVal(0), []byte{})
    assert.Equal(t, leftChild.getKey(1), key5)
    assert.Equal(t, leftChild.getVal(1), val5)

    rightChild := container.tree.Get(Root.getPtr(1))
    assert.Equal(t, rightChild.btype(), uint16(BNODE_LEAF))
    assert.Equal(t, rightChild.nkeys(), uint16(1))
    assert.Equal(t, rightChild.getKey(0), key7)
    assert.Equal(t, rightChild.getVal(0), val7)

    // 2:          Root: 0, 7, 9
    //     left: 0, 5       middle: 7   right: 9
    key9 := make([]byte, 1000)
    key9[0] = byte(9)
    val9 := make([]byte, 3000)
    container.tree.Insert(key9, val9)
    Root = container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.nkeys(), uint16(3))


    // 2:          Root: 0, 7, 9, 11
    //     left: 0, 5       middle: 7  right-middle:9 right: 11
    key11 := make([]byte, 1000)
    key11[0] = byte(11)
    val11 := make([]byte, 3000)
    container.tree.Insert(key11, val11)
    Root = container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.nkeys(), uint16(4))

    // 2:          Root: 0, 7, 9, 11, 13
    //     left: 0, 5       middle: 7  right-middle:9 right-middle2: 11 right: 13
    key13 := make([]byte, 1000)
    key13[0] = byte(13)
    val13 := make([]byte, 3000)
    container.tree.Insert(key13, val13)
    Root = container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.nkeys(), uint16(5))

    key15 := make([]byte, 1000)
    key15[0] = byte(15)
    val15 := make([]byte, 3000)
    
                                // Root: 0, 9
        // 2:          left_internal: 0, 7       right_internal: 9, 11, 13, 15
    //     left: 0, 5       middle: 7  right-middle:9 right-middle2: 11 right: 13, rightright: 15
    container.tree.Insert(key15, val15)
    Root = container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.nkeys(), uint16(2))
    assert.Equal(t, Root.getKey(0), []byte{})
    assert.Equal(t, Root.getKey(1), key9)
   
    leftInternal := container.tree.Get(Root.getPtr(0))
    assert.Equal(t, leftInternal.btype(), uint16(BNODE_NODE))
    assert.Equal(t, leftInternal.nkeys(), uint16(2))
    assert.Equal(t, leftInternal.getKey(0), []byte{})
    assert.Equal(t, leftInternal.getKey(1), key7)
    
    rightInternal := container.tree.Get(Root.getPtr(1))
    assert.Equal(t, rightInternal.btype(), uint16(BNODE_NODE))
    assert.Equal(t, rightInternal.nkeys(), uint16(4))
    assert.Equal(t, rightInternal.getKey(0), key9)
    assert.Equal(t, rightInternal.getKey(1), key11)
    assert.Equal(t, rightInternal.getKey(2), key13)
    assert.Equal(t, rightInternal.getKey(3), key15)
}

func TestBTreeDeleteKey(t *testing.T) {
    container := newC()
    assert.Equal(t, container.tree.Root, uint64(0))
    
    // edge cases 
    keyTooLong := make([]byte, 1001)
    assert.Panics(t, func() {container.tree.DeleteKey(keyTooLong)})
    assert.Panics(t, func() {container.tree.DeleteKey(nil)})
    assert.False(t, container.tree.DeleteKey([]byte{byte(0)}))

    
    key5 := make([]byte, 1000)
    key5[0] = byte(5)
    val5 := make([]byte, 200)

    // Root: 0, 5
    container.tree.Insert(key5, val5)
    // does not exist
    assert.False(t, container.tree.DeleteKey([]byte{byte(100)}))
    // Root: 0, 5
    assert.True(t, container.tree.DeleteKey(key5)) 
    Root := container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.btype(), uint16(BNODE_LEAF))
    assert.Equal(t, Root.nkeys(), uint16(1))
    
    val5 = make([]byte, 3000)
    // update operation
    container.tree.Insert(key5, val5)

    // add a long key, val pair
    // 1: Root: 0, 5, 7
    // 2:          Root: 0, 7
    //     left: 0, 5            right: 7
    key7 := make([]byte, 1000)
    key7[0] = byte(7)
    val7 := make([]byte, 3000)
    container.tree.Insert(key7, val7)
    
    // Root will downlevel
    //     Root: 0, 5
    assert.True(t, container.tree.DeleteKey(key7))
    Root = container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.btype(), uint16(BNODE_LEAF))
    assert.Equal(t, Root.nkeys(), uint16(2))
    
    // add a long key, val pair
    // 1: Root: 0, 5, 7
    // 2:          Root: 0, 7
    //     left: 0, 5            right: 7
    container.tree.Insert(key7, val7)

    // Root: 0, 7, Root will downlevel and 0, 7 will merge
    assert.True(t, container.tree.DeleteKey(key5)) 
    Root = container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.btype(), uint16(BNODE_LEAF))
    assert.Equal(t, Root.nkeys(), uint16(2))
    
    // 2:          Root: 0, 7, 9
    //     left: 0, 5       middle: 7   right: 9
    container.tree.Insert(key5, val5)
    key9 := make([]byte, 1000)
    key9[0] = byte(9)
    val9 := make([]byte, 3000)
    container.tree.Insert(key9, val9)
    Root = container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.nkeys(), uint16(3))

    // 2:          Root: 0, 9
    //     left: 0, 5     right: 9
    assert.True(t, container.tree.DeleteKey(key7)) 
    Root = container.tree.Get(container.tree.Root)
    assert.Equal(t, Root.nkeys(), uint16(2))
}

func TestTreeGetKey(t *testing.T) {
    container := newC()
    // edge cases 
    keyTooLong := make([]byte, 1001)
    assert.Panics(t, func() {container.tree.GetKey(keyTooLong)})
    assert.Panics(t, func() {container.tree.GetKey(nil)})
    val, exist := container.tree.GetKey([]byte{byte(0)})
    assert.Equal(t, val, []byte(nil))
    assert.False(t, exist)

    key := []byte("hello")
    val = []byte("world")
    
    container.tree.Insert(key, val)
    resVal, exist := container.tree.GetKey(key)
    assert.Equal(t, resVal, val)
    assert.True(t, exist)

    notExistKey := []byte("hello1")
    resVal, exist = container.tree.GetKey(notExistKey)
    assert.Equal(t, resVal, []byte(nil))
    assert.False(t, exist)
}
