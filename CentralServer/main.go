package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"sort"
	"time"
)

const serverPort = 8000

const maxNodeSize = 128 * MB

const MB = 1024 * 1024

func main() {

	client := &httpClient{client: newHttpClient()}

	routerHttp := mux.NewRouter()

	routerHttp.HandleFunc("/sendFile", client.ReceiveFileAndSend).Methods("POST")
	routerHttp.HandleFunc("/nodesUsage", client.nodesWithUsage).Methods("GET")

	err := http.ListenAndServe(fmt.Sprintf(":%d", serverPort), routerHttp)

	if err != nil {
		os.Exit(-1)
	}
}

func (c *httpClient) ReceiveFileAndSend(w http.ResponseWriter, r *http.Request) {

	file, header, err := r.FormFile("file")

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode("Please, insert a file.")
		return
	}

	body, err := ioutil.ReadAll(file)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	nodes, err := c.calculateNodesUsage()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(err.Error())
		return
	}

	for _, addr := range nodes[:1] {
		if addr.usage > 1 {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode("All the nodes are currently full. Please try again later.")
			return
		}
		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)

		part, err := writer.CreateFormFile("file", header.Filename)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		_, err = part.Write(body)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = writer.Close()

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		_, err = c.client.Post(fmt.Sprintf(addr.address+"/receiveFile"), writer.FormDataContentType(), &buf)

		if err != nil {
			w.WriteHeader(http.StatusRequestTimeout)
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (c *httpClient) calculateNodesUsage() ([]nodeWithUsage, error) {
	addresses := []string{
		"http://localhost:8080",
		"http://localhost:8081",
		"http://localhost:8082",
		"http://localhost:8083",
	}

	var nodes []nodeWithUsage

	for _, addr := range addresses {
		resp, err := c.client.Get(fmt.Sprintf("%s/%s", addr, "/getCurrentNodeSpace"))

		if err != nil {
			continue
		}

		defer resp.Body.Close()

		var nodeResp nodeResponse

		json.NewDecoder(resp.Body).Decode(&nodeResp)

		nodes = append(nodes, nodeWithUsage{
			address: addr,
			usage:   float64(nodeResp.Size / maxNodeSize),
		})
	}

	if len(nodes) == 0 {
		return nodes, errors.New("All the nodes are currently unavailable. Please try again later.")
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].usage < nodes[j].usage
	})

	return nodes, nil
}
func (c *httpClient) nodesWithUsage(w http.ResponseWriter, r *http.Request) {
	nodes, err := c.calculateNodesUsage()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	for _, tmp := range nodes {
		json.NewEncoder(w).Encode(map[string]float64{
			tmp.address: float64(tmp.usage),
		})
	}
}

type nodeWithUsage struct {
	address string
	usage   float64
}

type nodeResponse struct {
	Size float64 `json:"Size"`
}

type httpClient struct {
	client http.Client
}

func newHttpClient() http.Client {
	return http.Client{Timeout: time.Duration(5) * time.Second}
}
