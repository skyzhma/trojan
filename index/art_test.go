package index

import (
	"testing"
	"trojan/data"

	"github.com/stretchr/testify/assert"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res1 := art.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 123})
	assert.Nil(t, res1)

	res2 := art.Put([]byte("a"), &data.LogRecordPos{Fid: 12, Offset: 1234})
	assert.Equal(t, res2.Fid, uint32(11))
	assert.Equal(t, res2.Offset, int64(123))

	res3 := art.Put([]byte("a"), &data.LogRecordPos{Fid: 123, Offset: 12345})
	assert.Equal(t, res3.Fid, uint32(12))
	assert.Equal(t, res3.Offset, int64(1234))

	res4 := art.Put(nil, nil)
	assert.Nil(t, res4)
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("caas"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("eeda"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("bbue"), &data.LogRecordPos{Fid: 11, Offset: 123})

	val := art.Get([]byte("caas"))
	t.Log(val)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	pos1, res1 := art.Delete([]byte("not exist"))
	assert.Nil(t, pos1)
	assert.False(t, res1)

	art.Put([]byte("caas"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("eeda"), &data.LogRecordPos{Fid: 11, Offset: 123})
	art.Put([]byte("bbue"), &data.LogRecordPos{Fid: 11, Offset: 123})

	pos2, res2 := art.Delete([]byte("caas"))
	assert.True(t, res2)
	assert.Equal(t, pos2.Fid, uint32(11))
	assert.Equal(t, pos2.Offset, int64(123))

}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()

	assert.Equal(t, 0, art.Size())

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("adse"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bade"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
