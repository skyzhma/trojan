package main

import (
	"fmt"
	trojan "trojan"
)

func main() {

	opts := trojan.DefaultOptions
	opts.DirPath = "/tmp/trojan-test"
	db, err := trojan.Open(opts)
	if err != nil {
		panic(err)
	}

	// Put
	err = db.Put([]byte("name"), []byte("trojan"))
	if err != nil {
		panic(err)
	}

	// Get
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

}
