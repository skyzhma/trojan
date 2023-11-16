package redis

import (
	"encoding/binary"
	"errors"
	"time"
	"trojan"
	"trojan/utils"
)

var (
	ErrWrongTypeOperation = errors.New("WrongType Operation against a key holding the wrong kind of value")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

type RedisDataStructure struct {
	db *trojan.DB
}

func NewRedisDataStructure(options trojan.Options) (*RedisDataStructure, error) {
	db, err := trojan.Open(options)
	if err != nil {
		return nil, err
	}

	return &RedisDataStructure{db: db}, nil
}

// ========================== String ====================================== //
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {

	if value == nil {
		return nil
	}

	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}

	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {

	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	if expire > 0 && expire <= time.Now().UnixMicro() {
		return nil, nil
	}

	return encValue[index:], nil

}

// ========================== Hash ====================================== //
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {

	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	encKey := hk.encode()

	var exist = true
	if _, err := rds.db.Get(encKey); err == trojan.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWriteBatch(trojan.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}

	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil

}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {

	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	encKey := hk.encode()

	var exist = true
	if _, err := rds.db.Get(encKey); err == trojan.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(trojan.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil

}

// ========================== Set ====================================== //
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err = rds.db.Get(sk.encode()); err == trojan.ErrKeyNotFound {

		wb := rds.db.NewWriteBatch(trojan.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		wb.Put(sk.encode(), nil)
		if err := wb.Commit(); err != nil {
			return false, err
		}
		ok = true

	}

	return ok, nil

}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {

	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != trojan.ErrKeyNotFound {
		return false, err
	}

	if err == trojan.ErrKeyNotFound {
		return false, nil
	}

	return true, nil

}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {

	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err == trojan.ErrKeyNotFound {
		return false, nil
	}

	wb := rds.db.NewWriteBatch(trojan.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	wb.Delete(sk.encode())
	if err := wb.Commit(); err != nil {
		return false, err
	}

	return true, nil

}

// ========================== List ====================================== //
func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {

	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {

	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {

	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {

	return rds.popInner(key, false)
}

func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {

	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return 0, err
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	wb := rds.db.NewWriteBatch(trojan.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}

	_ = wb.Put(key, meta.encode())
	wb.Put(lk.encode(), element)
	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {

	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}

	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, nil
}

// ========================== ZSet ====================================== //
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {

	meta, err := rds.findMetaData(key, ZSet)
	if err != nil {
		return false, err
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	value, err := rds.db.Get(zk.encodeWithMember())

	if err != nil && err != trojan.ErrKeyNotFound {
		return false, err
	}

	if err == trojan.ErrKeyNotFound {
		exist = false
	}

	if exist {
		if score == utils.BytesToFloat64(value) {
			return false, nil
		}
	}

	wb := rds.db.NewWriteBatch(trojan.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}

	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.BytesToFloat64(value),
		}

		_ = wb.Delete(oldKey.encodeWithScore())
	}

	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil

}

func (rds *RedisDataStructure) ZScore(key, member []byte) (float64, error) {

	meta, err := rds.findMetaData(key, ZSet)
	if err != nil {
		return -1, err
	}

	if meta.size == 0 {
		return -1, nil
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.BytesToFloat64(value), nil
}

func (rds *RedisDataStructure) findMetaData(key []byte, dataType redisDataType) (*metaData, error) {

	metaBuf, err := rds.db.Get(key)
	if err != nil && err != trojan.ErrKeyNotFound {
		return nil, err
	}

	var meta *metaData
	var exist = true

	if err == trojan.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetaData(metaBuf)

		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}

		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}

	}

	if !exist {
		meta = &metaData{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}

		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}

	return meta, nil

}
