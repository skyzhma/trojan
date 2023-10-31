package trojan

import "os"

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrites   bool
	BytesPerSync uint
	IndexType    IndexerType
	MMapAtStart  bool
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type WriteBatchOptions struct {
	MaxWriteBatchNum uint
	SyncWrites       bool
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	BytesPerSync: 0,
	IndexType:    Btree,
	MMapAtStart:  true,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxWriteBatchNum: 10000,
	SyncWrites:       true,
}
