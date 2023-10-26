package trojan

import "errors"

var (
	ErrKeyIsEmpty                 = errors.New("key is empty")
	ErrIndexUpdateFailed          = errors.New("failed to update index")
	ErrKeyNotFound                = errors.New("key not found in database")
	ErrDataFileNotFound           = errors.New("data file doesn't exist")
	ErrDataFileDirectoryCorrupted = errors.New("data file directory corrupted")
	ErrExceedMaxBatchNum          = errors.New("Exceed max batch num")
	ErrMergeInProgress            = errors.New("merge is in progress, try again later")
)
