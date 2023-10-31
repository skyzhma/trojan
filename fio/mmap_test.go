package fio

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMMapIOManager(t *testing.T) {

	path := filepath.Join("/tmp", "mmap-a.data")
	defer destroyFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	b := make([]byte, 10)
	n1, err := mmapIO.Read(b, 0)

	assert.Equal(t, n1, 0)
	assert.Equal(t, io.EOF, err)

	fio, err := NewFileIoManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("a"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("b"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("c"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	size, err := mmapIO2.Size()

	assert.Equal(t, size, int64(3))
	assert.Nil(t, err)

	b1 := make([]byte, 1)
	n2, err := mmapIO2.Read(b1, 0)
	assert.Equal(t, n2, 1)
	assert.Nil(t, err)

}
