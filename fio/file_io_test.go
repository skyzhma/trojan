package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIoManager(t *testing.T) {

	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIoManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIo_Write(t *testing.T) {

	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIoManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, n, 0)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, n, 10)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, n, 7)
	assert.Nil(t, err)

}

func TestFileIo_Read(t *testing.T) {

	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIoManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("key-a"))
	assert.Equal(t, n, 5)
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err = fio.Read(b, 0)
	assert.Equal(t, n, 5)
	assert.Nil(t, err)
	assert.Equal(t, []byte("key-a"), b)

	n, err = fio.Write([]byte("key-b"))
	assert.Equal(t, n, 5)
	assert.Nil(t, err)

	n, err = fio.Read(b, 5)
	assert.Equal(t, n, 5)
	assert.Nil(t, err)

}

func TestFileIo_Sync(t *testing.T) {

	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIoManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)

}

func TestFileIo_Close(t *testing.T) {

	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIoManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)

}
