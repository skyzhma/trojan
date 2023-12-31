package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordPos struct {
	Fid    uint32
	Offset int64
	Size   uint32
}

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {

	header := make([]byte, maxLogRecordHeaderSize)
	header[4] = logRecord.Type

	var index = 5
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	return encBytes, int64(size)
}

func EncodeLogRecordPos(pos *LogRecordPos) []byte {

	size := binary.MaxVarintLen32*2 + binary.MaxVarintLen64
	buf := make([]byte, size)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]

}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {

	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n

	offset, n := binary.Varint(buf[index:])
	index += n

	size, _ := binary.Varint(buf[index:])

	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
		Size:   uint32(size),
	}

}

func DecodeLogRecord(buf []byte) (*LogRecordHeader, int64) {

	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {

	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
