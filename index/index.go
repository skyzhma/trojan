package index

import (
	"bytes"
	"trojan/data"

	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) (*data.LogRecordPos, bool)
	Size() int
	Iterator(reverse bool) Iterator
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)

func NewIndexer(typ IndexerType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return nil
	default:
		panic("Unsupported Indexer")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

type Iterator interface {
	Rewind()

	Seek(key []byte)

	Next()

	Valid() bool

	Key() []byte

	Value() *data.LogRecordPos

	Close()
}
