package b_tree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeAppendKV(t *testing.T) {
    testNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    testNode.setHeader(BNODE_LEAF, 2)
    key := []byte{1, 2, 3}
    val := []byte{1, 2, 3, 4, 5}
    nodeAppendKV(testNode, 0, 0, key, val)
    assert.Equal(t, testNode.getPtr(0), uint64(0))
    assert.Equal(t, testNode.getOffset(0), uint16(0))
    assert.Equal(
        t,
        testNode.getOffset(1),
        uint16(2 + len(key) + 2 + len(val)),
    )
    
    assert.Equal(t, testNode.getKey(0), key)
    assert.Equal(t, testNode.getVal(0), val)

    nodeAppendKV(testNode, 1, uint64(5201314), key, val)
    assert.Equal(t, testNode.getPtr(1), uint64(5201314))
    assert.Equal(
        t,
        testNode.getOffset(2),
        uint16(2 * (2 + len(key) + 2 + len(val))),
    )
    assert.Equal(t, testNode.getKey(1), key)
    assert.Equal(t, testNode.getVal(1), val)
}

func TestNodeAppendRange(t *testing.T) {
    new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    old := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    old.setHeader(BNODE_LEAF, 10)
    new.setHeader(BNODE_LEAF, 10)
    
    for i := uint16(0); i < uint16(10); i++ {
        nodeAppendKV(old, i, 0, []byte{byte(i)}, []byte{byte(i)})
    }
    nodeAppendRange(new, old, 0, 0, 10)
    assert.Equal(t, new.data, old.data)

    new.setHeader(BNODE_LEAF, 11)
    for i := uint16(0); i < uint16(11); i++ {
        nodeAppendKV(new, i, 0, []byte{byte(i)}, []byte{byte(i)})
    }

    nodeAppendRange(new, old, 1, 0, 10)
    for i := uint16(1); i < uint16(11); i++ {
        assert.Equal(t, old.getKey(i - 1), new.getKey(i))
        assert.Equal(t, old.getVal(i - 1), new.getVal(i))
    }
    assert.Equal(t, new.getKey(0), []byte{0})
    assert.Equal(t, new.getKey(0), []byte{0})
}

func TestNodeLookLE(t *testing.T) {
    new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    new.setHeader(BNODE_LEAF, 10)
    // 0, 2, 4, 6, 8, 10, 12, 14, 16, 18
    for i := uint16(0); i < uint16(20); i += 2 {
        nodeAppendKV(new, i / 2, 0, []byte{byte(i)}, []byte{byte(i)})
    }
    
    t1 := nodeLookLE(new, []byte{byte(2)})
    assert.Equal(t, t1, uint16(1))

    t2 := nodeLookLE(new, []byte{byte(3)})
    assert.Equal(t, t2, uint16(1))

    t3 := nodeLookLE(new, []byte{byte(20)})
    assert.Equal(t, t3, uint16(9))

    t4 := nodeLookLE(new, []byte{byte(16)})
    assert.Equal(t, t4, uint16(8))

    t5 := nodeLookLE(new, []byte{byte(1)})
    assert.Equal(t, t5, uint16(0))

    t6 := nodeLookLE(new, []byte{byte(0)})
    assert.Equal(t, t6, uint16(0))

    nodeAppendKV(new, 0, 0, []byte{byte(2)}, []byte{byte(2)})
    // if we are trying to find a number that is less than
    // all number in the array, we will get a negative number.
    // this shouldn't happen any way, so I decide to panic on
    // this.
    assert.Panics(
        t,
        func() {nodeLookLE(new, []byte{byte(0)})},
        "expected panic on minimal number",
    )
}

func TestNodeSplit2(t *testing.T) {
    old := BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}
    // 4000, 200
    key1 := make([]byte, 1000)
    val1 := make([]byte, 3000)
    key2 := make([]byte, 100)
    val2 := make([]byte, 100)
    key2[0] = byte(1)

    old.setHeader(BNODE_LEAF, 2)
    nodeAppendKV(old, 0, 0, key1, val1)
    nodeAppendKV(old, 1, 0, key2, val2)
    left := BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}
    right := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    nodeSplit2(left, right, old)

    assert.Equal(t, left.nkeys(), uint16(1))
    assert.Equal(t, left.getKey(0), key1)
    assert.Equal(t, left.getVal(0), val1)
    assert.Equal(
        t,
        left.nbytes(),
        uint16(HEADER + 8 * 1 + 2 * 1 + 4 + len(key1) + len(val1)),
    )

    assert.Equal(t, right.nkeys(), uint16(1))
    assert.Equal(t, right.getKey(0), key2)
    assert.Equal(t, right.getVal(0), val2)
    assert.Equal(
        t,
        right.nbytes(),
        uint16(HEADER + 8 * 1 + 2 * 1 + 4 + len(key2) + len(val2)),
    )
    assert.Less(t, right.nbytes(), uint16(BTREE_PAGE_SIZE))

    // 2000, 200, 2000
    old = BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}
    key1 = make([]byte, 1000)
    val1 = make([]byte, 1000)
    key2 = make([]byte, 100)
    val2 = make([]byte, 100)
    key3 := make([]byte, 1000)
    val3 := make([]byte, 1000)
    key2[0] = byte(1)
    key3[0] = byte(2)
    left = BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}
    right = BNode{data: make([]byte, BTREE_PAGE_SIZE)}

    old.setHeader(BNODE_LEAF, 3)
    nodeAppendKV(old, 0, 0, key1, val1)
    nodeAppendKV(old, 1, 0, key2, val2)
    nodeAppendKV(old, 2, 0, key3, val3)
    left = BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}
    right = BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    nodeSplit2(left, right, old)

    assert.Equal(t, left.nkeys(), uint16(1))
    assert.Equal(t, left.getKey(0), key1)
    assert.Equal(t, left.getVal(0), val1)
    assert.Equal(
        t,
        left.nbytes(),
        uint16(HEADER + 8 * 1 + 2 * 1 + 4 + len(key1) + len(val1)),
    )

    assert.Equal(t, right.nkeys(), uint16(2))
    assert.Equal(t, right.getKey(0), key2)
    assert.Equal(t, right.getVal(0), val2)
    assert.Equal(t, right.getKey(1), key3)
    assert.Equal(t, right.getVal(1), val3)
    assert.Equal(
        t,
        right.nbytes(),
        uint16(
            HEADER + 8 * 2 + 2 * 2 + 4 * 2 + 
            len(key2) + len(val2) + len(key3) + len(val3),
        ),
    )
    
    // 3000, 2000, 200, 2000
    key1 = make([]byte, 1000)
    val1 = make([]byte, 2000)
    key2 = make([]byte, 1000)
    val2 = make([]byte, 1000)
    key3 = make([]byte, 100)
    val3 = make([]byte, 100)
    key4 := make([]byte, 1000)
    val4 := make([]byte, 1000)
    key2[0] = byte(1)
    key3[0] = byte(2)
    key4[0] = byte(3)
    old.setHeader(BNODE_LEAF, 4)
    nodeAppendKV(old, 0, 0, key1, val1)
    nodeAppendKV(old, 1, 0, key2, val2)
    nodeAppendKV(old, 2, 0, key3, val3)
    nodeAppendKV(old, 3, 0, key4, val4)

    left = BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}
    right = BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    nodeSplit2(left, right, old)
    assert.Equal(t, left.nkeys(), uint16(2))
    assert.Equal(t, left.getKey(0), key1)
    assert.Equal(t, left.getVal(0), val1)
    assert.Equal(t, left.getKey(1), key2)
    assert.Equal(t, left.getVal(1), val2)
    assert.Equal(
        t,
        left.nbytes(),
        uint16(
            HEADER + 2 * 8 + 2 * 2 + 4 * 2 + 
            len(key1) + len(val1) + len(key2) + len(val2),
        ),
    )

    assert.Equal(t, right.nkeys(), uint16(2))
    assert.Equal(t, right.getKey(0), key3)
    assert.Equal(t, right.getVal(0), val3)
    assert.Equal(t, right.getKey(1), key4)
    assert.Equal(t, right.getVal(1), val4)
    assert.Equal(
        t,
        right.nbytes(),
        uint16(
            HEADER + 2 * 8 + 2 * 2 + 4 * 2 + 
            len(key3) + len(val3) + len(key4) + len(key4),
        ),
    )
    assert.Less(t, right.nbytes(), uint16(BTREE_PAGE_SIZE))
}

func TestNodeSplit3(t *testing.T) {
    node := BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}
    node.setHeader(BNODE_LEAF, 1)
    key1 := make([]byte, 1000)
    val1 := make([]byte, 3000)
    nodeAppendKV(node, 0, 0, key1, val1)

    numNode, nodes := nodeSplit3(node)
    assert.Equal(t, numNode, uint16(1))
    assert.Equal(t, len(nodes), 3)
    assert.Equal(t, nodes[0], node)
    assert.Equal(t, nodes[1], BNode{})
    assert.Equal(t, nodes[2], BNode{})
    assert.Less(t, nodes[0].nbytes(), uint16(BTREE_PAGE_SIZE))

    // 2000, 200, 2000
    node = BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}
    key1 = make([]byte, 1000)
    val1 = make([]byte, 1000)
    key2 := make([]byte, 100)
    val2 := make([]byte, 100)
    key3 := make([]byte, 1000)
    val3 := make([]byte, 1000)
    key2[0] = byte(1)
    key3[0] = byte(2)

    node.setHeader(BNODE_LEAF, 3)
    nodeAppendKV(node, 0, 0, key1, val1)
    nodeAppendKV(node, 1, 0, key2, val2)
    nodeAppendKV(node, 2, 0, key3, val3)
    numNode, nodes = nodeSplit3(node)
    assert.Equal(t, numNode, uint16(2))
    left := nodes[0]
    right := nodes[1]

    assert.Equal(t, left.nkeys(), uint16(1))
    assert.Equal(t, left.getKey(0), key1)
    assert.Equal(t, left.getVal(0), val1)
    assert.Equal(
        t,
        left.nbytes(),
        uint16(HEADER + 8 * 1 + 2 * 1 + 4 + len(key1) + len(val1)),
    )
    assert.Less(t, left.nbytes(), uint16(BTREE_PAGE_SIZE))

    assert.Equal(t, right.nkeys(), uint16(2))
    assert.Equal(t, right.getKey(0), key2)
    assert.Equal(t, right.getVal(0), val2)
    assert.Equal(t, right.getKey(1), key3)
    assert.Equal(t, right.getVal(1), val3)
    assert.Equal(
        t,
        right.nbytes(),
        uint16(
            HEADER + 8 * 2 + 2 * 2 + 4 * 2 + 
            len(key2) + len(val2) + len(key3) + len(val3),
        ),
    )
    assert.Less(t, right.nbytes(), uint16(BTREE_PAGE_SIZE))

    // 3000, 2000, 200, 2000
    node = BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}
    key1 = make([]byte, 1000)
    val1 = make([]byte, 2000)
    key2 = make([]byte, 1000)
    val2 = make([]byte, 1000)
    key3 = make([]byte, 100)
    val3 = make([]byte, 100)
    key4 := make([]byte, 1000)
    val4 := make([]byte, 1000)
    key2[0] = byte(1)
    key3[0] = byte(2)
    key4[0] = byte(3)

    node.setHeader(BNODE_LEAF, 4)
    nodeAppendKV(node, 0, 0, key1, val1)
    nodeAppendKV(node, 1, 0, key2, val2)
    nodeAppendKV(node, 2, 0, key3, val3)
    nodeAppendKV(node, 3, 0, key4, val4)
    numNode, nodes = nodeSplit3(node)
    assert.Equal(t, numNode, uint16(3))
    left = nodes[0]
    middle := nodes[1]
    right = nodes[2]
    assert.Equal(t, left.nkeys(), uint16(1))    
    assert.Equal(t, left.getKey(0), key1)
    assert.Equal(t, left.getVal(0), val1)
    assert.Equal(
        t,
        left.nbytes(),
        uint16(
            HEADER + 8 * 1 + 2 * 1 + 4 * 1 + 
            len(key1) + len(val1),
        ),
    )
    assert.Less(t, left.nbytes(), uint16(BTREE_PAGE_SIZE))
    
    assert.Equal(t, middle.nkeys(), uint16(1))
    assert.Equal(t, middle.getKey(0), key2)
    assert.Equal(t, middle.getVal(0), val2)
    assert.Equal(
        t,
        middle.nbytes(),
        uint16(
            HEADER + 8 * 1 + 2 * 1 + 4 * 1 + 
            len(key2) + len(val2),
        ),
    )
    assert.Less(t, middle.nbytes(), uint16(BTREE_PAGE_SIZE))

    assert.Equal(t, right.nkeys(), uint16(2))
    assert.Equal(t, right.getKey(0), key3)
    assert.Equal(t, right.getVal(0), val3)
    assert.Equal(t, right.getKey(1), key4)
    assert.Equal(t, right.getVal(1), val4)
    assert.Equal(
        t,
        right.nbytes(),
        uint16(
            HEADER + 8 * 2 + 2 * 2 + 4 * 2 + 
            len(key3) + len(val3) + len(key4) + len(val4),
        ),
    )
    assert.Less(t, right.nbytes(), uint16(BTREE_PAGE_SIZE))
}

func TestNodeReplaceKidN(t *testing.T) {
    // 0, 2, 4, 6, 8
    old := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    old.setHeader(BNODE_NODE, 5)
    for i := uint16(0); i < 10; i += 2 {
        nodeAppendKV(old, i/2, 0, []byte{byte(i)}, nil)
    }

    new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    new.setHeader(BNODE_NODE, 5)
    tree := BTree{root: 0}
    tree.new = func(node BNode) uint64 {
        return 12311144
    }
    kid1 := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    key1 := []byte{byte(1)}
    val1 := []byte{byte(1)}
    kid1.setHeader(BNODE_LEAF, 1)
    nodeAppendKV(kid1, 0, 0, key1, val1)
    kid2 := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    key2 := []byte{byte(2)}
    val2 := []byte{byte(2)}
    kid2.setHeader(BNODE_LEAF, 1)
    nodeAppendKV(kid2, 0, 0, key2, val2)
    kid3 := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    key3 := []byte{byte(3)}
    val3 := []byte{byte(3)}
    kid3.setHeader(BNODE_LEAF, 1)
    nodeAppendKV(kid3, 0, 0, key3, val3)
    
    // replace 2 with 1: 0, 1, 4, 6, 8
    nodeReplaceKidN(&tree, new, old, 1, kid1)
    assert.Equal(t, new.nkeys(), uint16(5))
    assert.Equal(t, new.getKey(1), key1)
    assert.Equal(t, new.getVal(1), []byte{})
    assert.Equal(t, new.getPtr(1), uint64(12311144))

    // replace 1 with 2, 3: 0, 2, 3, 4, 6, 8
    nodeReplaceKidN(&tree, new, old, 1, kid2, kid3)
    assert.Equal(t, new.nkeys(), uint16(6))
    assert.Equal(t, new.getKey(1), key2)
    assert.Equal(t, new.getVal(1), []byte{})
    assert.Equal(t, new.getPtr(1), uint64(12311144))

    assert.Equal(t, new.getKey(2), key3)
    assert.Equal(t, new.getVal(2), []byte{})
    assert.Equal(t, new.getPtr(2), uint64(12311144))

    // replace 0 with 1, 2, 3: 1, 2, 3, 2, 4, 6, 8
    nodeReplaceKidN(&tree, new, old, 0, kid1, kid2, kid3)
    assert.Equal(t, new.nkeys(), uint16(7))
    assert.Equal(t, new.getKey(0), key1)
    assert.Equal(t, new.getVal(0), []byte{})
    assert.Equal(t, new.getPtr(0), uint64(12311144))

    assert.Equal(t, new.getKey(1), key2)
    assert.Equal(t, new.getVal(1), []byte{})
    assert.Equal(t, new.getPtr(1), uint64(12311144))

    assert.Equal(t, new.getKey(2), key3)
    assert.Equal(t, new.getVal(2), []byte{})
    assert.Equal(t, new.getPtr(2), uint64(12311144))
}

func TestNodeReplace2Kid(t *testing.T) {
    // 0, 2, 4, 6, 8
    old := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    old.setHeader(BNODE_NODE, 5)
    for i := uint16(0); i < 10; i += 2 {
        nodeAppendKV(old, i/2, 0, []byte{byte(i)}, nil)
    }
    new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    // 0, 1, 6, 8
    nodeReplace2Kid(new, old, 1, 123444, []byte{byte(1)})
    assert.Equal(t, new.nkeys(), uint16(4))
    assert.Equal(t, new.getKey(1), []byte{byte(1)})
}
