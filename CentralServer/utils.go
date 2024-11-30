package main

import (
	"container/list"
	"crypto/sha256"
	"encoding/json"
	"hash"
	"log"
	"net/http"
)

func SplitFileIntoBlocks(file []byte) *list.List {

	maxBlockSize := 128 * MB
	numOfBlocks := len(file) / maxBlockSize
	listOfBlocks := list.New()

	if numOfBlocks == 0 {
		listOfBlocks.PushBack(FileBlock{bytes: file, position: 1})
		return listOfBlocks
	} else {
		for i := range numOfBlocks {
			tmpBlock := file[maxBlockSize*i : (maxBlockSize*i)+maxBlockSize]
			listOfBlocks.PushBack(FileBlock{bytes: tmpBlock, position: i + 1})
		}
		listOfBlocks.PushBack(FileBlock{bytes: file[(maxBlockSize * numOfBlocks):], position: numOfBlocks + 1})
	}

	return listOfBlocks
}

func GenerateFileHash(fileName string) []byte {
	h := sha256.New()

	return generateHash([]byte(fileName), h)
}

func GenerateBlockHash(blockData []byte) []byte {
	h := sha256.New()

	return generateHash(blockData, h)
}

func generateHash(data []byte, h hash.Hash) []byte {
	h.Write(data)

	bs := h.Sum(nil)
	return bs
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	log.Printf("Error %d: %s", code, message)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func logAndRespondError(w http.ResponseWriter, code int, message string, err error) {
	if err != nil {
		log.Printf("[ERROR] %s: %v", message, err)
	} else {
		log.Printf("[ERROR] %s", message)
	}
	respondWithError(w, code, message)
}
