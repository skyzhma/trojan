package trojan

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"trojan/data"
	"trojan/fio"
	"trojan/index"
	"trojan/utils"

	"github.com/gofrs/flock"
)

const (
	seqNoKey     = "seq-no"
	fileLockName = "fileLock"
)

type DB struct {
	options         Options
	fileIds         []int
	mu              *sync.RWMutex
	activeFile      *data.DataFile
	olderFiles      map[uint32]*data.DataFile
	index           index.Indexer
	seqNo           uint64
	isMerging       bool
	seqNoFileExists bool
	isInitial       bool
	fileLock        *flock.Flock
	bytesWrite      uint
	reclaimSize     int64
}

type Stat struct {
	KeyNum          uint
	DataFileNum     uint
	ReclaimableSize int64
	DiskSize        int64
}

func Open(options Options) (*DB, error) {

	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool

	// check whether the datapath exists
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsed
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		isInitial = true
	}

	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if options.IndexType != BPlusTree {

		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		// why reset io type when it's bptree index ?
		if db.options.MMapAtStart {
			if err := db.resetIoType(); err != nil {
				return nil, err
			}
		}

	} else {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}

		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}

			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

func (db *DB) Put(key []byte, value []byte) error {

	// If key is empty
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// LogRecord
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecorddWithLock(logRecord)

	if err != nil {
		return err
	}

	if oldValue := db.index.Put(key, pos); oldValue != nil {
		db.reclaimSize += int64(oldValue.Size)
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPosition(logRecordPos)
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted}
	pos, err := db.appendLogRecorddWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)
	// delete the key

	oldValue, isDelete := db.index.Delete(key)
	if !isDelete {
		return ErrIndexUpdateFailed
	}

	if oldValue != nil {
		db.reclaimSize += int64(oldValue.Size)
	}

	return nil
}

func (db *DB) Close() error {

	defer func() {
		err := db.fileLock.Unlock()
		if err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()

	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.index.Close(); err != nil {
		return err
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)

	if err != nil {
		return err
	}

	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}

	encRecord, _ := data.EncodeLogRecord(record)

	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}

	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var numDataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		numDataFiles += 1
	}

	diskSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size, %v", err))
	}

	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     numDataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        diskSize,
	}

}

func (db *DB) ListKeys() [][]byte {

	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}

		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

func (db *DB) appendLogRecorddWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	encRecord, size := data.EncodeLogRecord(logRecord)

	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.olderFiles[db.activeFile.FileId] = db.activeFile

		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}

	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)

	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite > db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: uint32(size)}

	return pos, nil

}

func (db *DB) setActiveDataFile() error {

	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardIO)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataFileDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	sort.Ints(fileIds)

	db.fileIds = fileIds

	for i, fid := range fileIds {
		ioType := fio.StandardIO
		if db.options.MMapAtStart {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}

		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil

}

func (db *DB) loadIndexFromDataFiles() error {

	if len(db.fileIds) == 0 {
		return nil
	}

	hashMerge, nonMergeFileId := false, uint32(0)

	mergeFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)

	if _, err := os.Stat(mergeFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}

		hashMerge = true
		nonMergeFileId = fid

	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {

		var oldValue *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldValue, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldValue = db.index.Put(key, pos)
		}

		if oldValue != nil {
			db.reclaimSize += int64(oldValue.Size)
		}
	}

	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = nonTransactionSeqNo

	for _, fid := range db.fileIds {
		var fileId = uint32(fid)

		if hashMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile

		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}

				return err
			}

			// construct index
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				if logRecord.Type == data.LogRecordTxnFinished {

					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}

					delete(transactionRecords, seqNo)

				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}

			}

			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			offset += size

		}

		if fileId == db.activeFile.FileId {
			db.activeFile.WriteOff = offset
		}

	}

	db.seqNo = currentSeqNo

	return nil

}

func checkOptions(options Options) error {

	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("data file size must be greater than 0")
	}

	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("Invalid data file merge ratio")
	}

	return nil
}

func (db *DB) loadSeqNo() error {

	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(fileName)

	if err != nil {
		return err
	}

	record, _, err := seqNoFile.ReadLogRecord(0)

	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)

	if err != nil {
		return err
	}

	db.seqNo = seqNo
	db.seqNoFileExists = true
	return os.Remove(fileName)
}

func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardIO); err != nil {
			return err
		}
	}

	return nil
}
