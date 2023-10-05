package fio

import "os"

type FileIo struct {
	fd *os.File
}

func NewFileIoManager(fileName string) (*FileIo, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}

	return &FileIo{fd: fd}, nil
}

func (fio *FileIo) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIo) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIo) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIo) Close() error {
	return fio.fd.Close()
}

func (fio *FileIo) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
