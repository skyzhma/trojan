package benchmark

import (
	"math/rand"
	"os"
	"testing"
	"time"
	"trojan"
	"trojan/utils"

	"github.com/stretchr/testify/assert"
)

var db *trojan.DB

func init() {

	options := trojan.DefaultOptions
	dir, _ := os.MkdirTemp("", "trojan-bench-test")
	options.DirPath = dir

	var err error
	db, err = trojan.Open(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {

	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != trojan.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {

	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}

}
