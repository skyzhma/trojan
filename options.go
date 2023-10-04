package trojan

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrites   bool
	IndexType    IndexerType
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)
