package redis

import (
	"os"
	"testing"
	"time"
	"trojan"
	"trojan/utils"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := trojan.DefaultOptions
	dir, _ := os.MkdirTemp("", "trojan-redis-get-test")
	opts.DirPath = dir

	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(10))
	assert.Nil(t, err)

	val, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	_, err = rds.Get(utils.GetTestKey(33))
	assert.Equal(t, err, trojan.ErrKeyNotFound)

}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := trojan.DefaultOptions
	dir, _ := os.MkdirTemp("", "trojan-redis-del-test")
	opts.DirPath = dir

	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, typ, String)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, err, trojan.ErrKeyNotFound)

}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := trojan.DefaultOptions
	dir, _ := os.MkdirTemp("", "trojan-redis-hget-test")
	opts.DirPath = dir

	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	res1, err := rds.HSet(utils.GetTestKey(1), []byte("field-1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, res1)

	v1 := utils.RandomValue(100)
	res2, err := rds.HSet(utils.GetTestKey(1), []byte("field-1"), v1)
	assert.Nil(t, err)
	assert.False(t, res2)

	v2 := utils.RandomValue(100)
	res3, err := rds.HSet(utils.GetTestKey(1), []byte("field-2"), v2)
	assert.Nil(t, err)
	assert.True(t, res3)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field-1"))
	assert.Equal(t, val1, v1)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field-2"))
	assert.Equal(t, val2, v2)

	_, err = rds.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	assert.Equal(t, err, trojan.ErrKeyNotFound)

}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := trojan.DefaultOptions
	dir, _ := os.MkdirTemp("", "trojan-redis-hdel-test")
	opts.DirPath = dir

	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	res1, err := rds.HDel(utils.GetTestKey(1), []byte("field-1"))
	assert.Nil(t, err)
	assert.False(t, res1)

	res2, err := rds.HSet(utils.GetTestKey(1), []byte("field-1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, res2)

	res3, err := rds.HSet(utils.GetTestKey(1), []byte("field-2"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, res3)

	del, err := rds.HDel(utils.GetTestKey(1), []byte("field-2"))
	assert.True(t, del)
	assert.Nil(t, err)

}

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := trojan.DefaultOptions
	dir, _ := os.MkdirTemp("", "trojan-redis-sismember-test")
	opts.DirPath = dir

	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	res1, err := rds.SAdd(utils.GetTestKey(1), []byte("val1"))
	assert.True(t, res1)
	assert.Nil(t, err)

	res2, err := rds.SAdd(utils.GetTestKey(1), []byte("val1"))
	assert.False(t, res2)
	assert.Nil(t, err)

	res3, err := rds.SAdd(utils.GetTestKey(1), []byte("val2"))
	assert.True(t, res3)
	assert.Nil(t, err)

	res4, err := rds.SIsMember(utils.GetTestKey(2), []byte("val1"))
	assert.False(t, res4)
	assert.Nil(t, err)

	res5, err := rds.SIsMember(utils.GetTestKey(1), []byte("val1"))
	assert.True(t, res5)
	assert.Nil(t, err)

	res6, err := rds.SIsMember(utils.GetTestKey(1), []byte("val2"))
	assert.True(t, res6)
	assert.Nil(t, err)

	res7, err := rds.SIsMember(utils.GetTestKey(2), []byte("val-not-exist"))
	assert.False(t, res7)
	assert.Nil(t, err)

}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := trojan.DefaultOptions
	dir, _ := os.MkdirTemp("", "trojan-redis-srem-test")
	opts.DirPath = dir

	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	res1, err := rds.SAdd(utils.GetTestKey(1), []byte("val1"))
	assert.True(t, res1)
	assert.Nil(t, err)

	res2, err := rds.SAdd(utils.GetTestKey(1), []byte("val1"))
	assert.False(t, res2)
	assert.Nil(t, err)

	res3, err := rds.SRem(utils.GetTestKey(1), []byte("val1"))
	assert.True(t, res3)
	assert.Nil(t, err)

	res4, err := rds.SRem(utils.GetTestKey(1), []byte("val-not"))
	assert.False(t, res4)
	assert.Nil(t, err)

}
