package index

import (
	"os"
	"path/filepath"
	"testing"
	"trojan/data"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPTree(path, false)
	tree.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	tree.Put([]byte("bb"), &data.LogRecordPos{Fid: 2, Offset: 10})
	tree.Put([]byte("c"), &data.LogRecordPos{Fid: 3, Offset: 20})
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPTree(path, false)

	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)

	tree.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	pos1 := tree.Get([]byte("aa"))
	assert.NotNil(t, pos1)

	tree.Put([]byte("bb"), &data.LogRecordPos{Fid: 2, Offset: 10})
	pos2 := tree.Get([]byte("bb"))
	assert.NotNil(t, pos2)

}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPTree(path, false)

	res1 := tree.Delete([]byte("not exist"))
	assert.False(t, res1)

	tree.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	res2 := tree.Delete([]byte("aa"))
	assert.True(t, res2)

	res3 := tree.Delete([]byte("aa"))
	assert.NotNil(t, res3)

}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-size")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPTree(path, false)

	tree.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	res1 := tree.Size()
	assert.Equal(t, res1, 1)

	_ = tree.Delete([]byte("aa"))
	res2 := tree.Size()
	assert.Equal(t, res2, 0)

}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-iterator")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPTree(path, false)
	tree.Put([]byte("aa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	tree.Put([]byte("bb"), &data.LogRecordPos{Fid: 2, Offset: 10})
	tree.Put([]byte("cc"), &data.LogRecordPos{Fid: 3, Offset: 20})
	tree.Put([]byte("cd"), &data.LogRecordPos{Fid: 3, Offset: 20})
	tree.Put([]byte("dd"), &data.LogRecordPos{Fid: 3, Offset: 20})

	iter := tree.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
