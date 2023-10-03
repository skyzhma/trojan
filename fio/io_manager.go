package fio

const DataFilePerm = 0644

type IoManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
}
