package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardIO FileIOType = iota
	MemoryMap
)

type IoManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Size() (int64, error)
}

func NewIoManager(fileName string, ioType FileIOType) (IoManager, error) {

	switch ioType {
	case StandardIO:
		return NewFileIoManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("Unsupported io manager")
	}

	return NewFileIoManager(fileName)
}
