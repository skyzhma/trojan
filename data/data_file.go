package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"trojan/fio"
)

var (
	ErrInvalidCRC = errors.New("Invalid CRC value of log record")
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager fio.IoManager
}

func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StandardIO)
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardIO)
}

func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StandardIO)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	ioManager, err := fio.NewIoManager(fileName, ioType)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {

	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	headerBuf, err := df.readNBytes(headerBytes, offset)

	if err != nil {
		return nil, 0, err
	}

	header, headerSize := DecodeLogRecord(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = keySize + valueSize + headerSize

	logRecord := &LogRecord{Type: header.recordType}

	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {

	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}

	encRecord, _ := EncodeLogRecord(record)

	return df.Write(encRecord)
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}

func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}

	ioManager, err := fio.NewIoManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}

	df.IoManager = ioManager
	return nil

}
