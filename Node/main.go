package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func main() {
	routerHttp := mux.NewRouter()

	routerHttp.HandleFunc("/health", currentHealth).Methods("GET")
	routerHttp.HandleFunc("/receiveFile", receiveFile).Methods("POST")
	routerHttp.HandleFunc("/retrieveFile", retrieveFile).Methods("GET")
	routerHttp.HandleFunc("/checkIfFileExists", checkIfFileExists).Methods("GET")
	routerHttp.HandleFunc("/getCurrentNodeSpace", getCurrentNodeSpace).Methods("GET")

	log.Println("Node started on port: " + os.Args[1])

	err := http.ListenAndServe(fmt.Sprintf("localhost:%s", os.Args[1]), routerHttp)

	if err != nil {
		os.Exit(-1)
	}
}

func currentHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	return
}

func receiveFile(w http.ResponseWriter, r *http.Request) {

	file, header, err := r.FormFile("file")

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	dest, err := os.Create(fmt.Sprintf("/Users/navidnazem/desktop/fdsfiletests%s/%s", os.Args[2], header.Filename))

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, err = io.Copy(dest, file)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func retrieveFile(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("filename")

	if fileName == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	body, err := os.ReadFile(fmt.Sprintf("/Users/navidnazem/desktop/fdsfiletests%s/%s", os.Args[2], fileName))

	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Write(body)
	w.WriteHeader(http.StatusOK)
	return
}

func checkIfFileExists(w http.ResponseWriter, r *http.Request) {

	fileName := r.URL.Query().Get("filename")

	if fileName == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := os.Stat(fmt.Sprintf("/Users/navidnazem/desktop/fdsfiletests%s/%s", os.Args[2], fileName))

	if err != nil && errors.Is(err, os.ErrNotExist) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode("File: " + fileName + " not found on the node.")
		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode("File: " + fileName + " found on the node.")
}

func getCurrentNodeSpace(w http.ResponseWriter, _ *http.Request) {
	var size int64
	err := filepath.Walk("/Users/navidnazem/desktop/fdsfiletests"+os.Args[2], func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]int64{
		"Size": size,
	})
}
