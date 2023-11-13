package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	trojan "trojan"
)

var db *trojan.DB

func init() {

	var err error
	options := trojan.DefaultOptions
	dir, _ := os.MkdirTemp("", "trojan-http")
	options.DirPath = dir
	db, err = trojan.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db %v", err))
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put data in db %v", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))

	if err != nil && err != trojan.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value in db %v", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))

	if err != nil && err != trojan.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete key in db %v", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()

	writer.Header().Set("Content-Type", "application/json")
	var result []string

	for _, key := range keys {
		result = append(result, string(key))
	}

	_ = json.NewEncoder(writer).Encode(result)

}

func handleStat(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)

}

func main() {

	http.HandleFunc("/trojan/put", handlePut)

	http.HandleFunc("/trojan/get", handleGet)

	http.HandleFunc("/trojan/delete", handleDelete)

	http.HandleFunc("/trojan/listkeys", handleListKeys)

	http.HandleFunc("/trojan/stat", handleStat)

	http.ListenAndServe("localhost:8080", nil)

}
