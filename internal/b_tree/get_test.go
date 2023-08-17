package b_tree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
    tree := BTree{Root: 0}
    SetUpMockBTree(t, &tree)
    root := tree.Get(tree.Root)
    
    root = treeInsert(&tree, root, []byte("hello"), []byte("world"))
    val, exist := treeGet(&tree, root, []byte("hello"))
    assert.True(t, exist)
    assert.Equal(t, val, []byte("world"))

    val, exist = treeGet(&tree, root, []byte("hello1"))
    assert.False(t, exist)
    assert.Equal(t, val, []byte(nil))
}
