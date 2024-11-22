package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const MB = 1024 * 1024

var availableSpace = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "node_available_space",
		Help: "Available space for each node",
	},
	[]string{"node"}, // Rimuovi "occupied_space" come etichetta
)

func main() {
	routerHttp := mux.NewRouter()

	prometheus.MustRegister(availableSpace)
	routerHttp.HandleFunc("/", func(w http.ResponseWriter, request *http.Request) {
		availableSpace.WithLabelValues(request.Method, request.URL.Path).Inc()
		w.Write([]byte("Hello, Prometheus!"))
	})
	routerHttp.Handle("/metrics", promhttp.Handler())
	routerHttp.HandleFunc("/health", currentHealth).Methods("GET")
	routerHttp.HandleFunc("/receiveFile", receiveFile).Methods("POST")
	routerHttp.HandleFunc("/retrieveFile", retrieveFile).Methods("GET")
	routerHttp.HandleFunc("/checkIfFileExists", checkIfFileExists).Methods("GET")
	routerHttp.HandleFunc("/getCurrentNodeSpace", getCurrentNodeSpace).Methods("GET")

	go func() {
		url := "http://localhost:" + os.Args[1]
		var jsonStr = fmt.Sprintf(`{"Url":"%s"}`, url)

		httpClient := http.Client{Timeout: time.Duration(5) * time.Second}
		req, err := http.NewRequest("POST", "http://localhost:8000/addNode", strings.NewReader(jsonStr))
		if err != nil {
			log.Println("error creating the addNode request")
		}
		req.Header.Set("Content-Type", "application/json; charset=UTF-8")

		_, err = httpClient.Do(req)
		if err != nil {
			log.Println("error while sending the addNode request: " + err.Error())
		}
	}()

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

	size, _ := calculateOccupiedSize()

	occupiedSpace := float64(size / (128 + MB))

	availableSpace.With(prometheus.Labels{"node": fmt.Sprintf("localhost:%s", os.Args[1])}).Set(occupiedSpace)
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
	size, err := calculateOccupiedSize()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]int64{
		"Size": size,
	})
}

func calculateOccupiedSize() (int64, error) {
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
	return size, err
}
