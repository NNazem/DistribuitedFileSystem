package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9"
	"io"
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

	clients := &clients{httpClient: newHttpClient(), redisClient: newRedisClient()}

	routerHttp := mux.NewRouter()

	routerHttp.HandleFunc("/sendFile", clients.ReceiveFileAndSend).Methods("POST")
	routerHttp.HandleFunc("/nodesUsage", clients.nodesWithUsage).Methods("GET")
	routerHttp.HandleFunc("/retrieveFile", clients.RetrieveFile).Methods("GET")

	err := http.ListenAndServe(fmt.Sprintf(":%d", serverPort), routerHttp)

	if err != nil {
		os.Exit(-1)
	}
}

func (c *clients) RetrieveFile(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("fileName")

	h := sha256.New()

	h.Write([]byte(fileName))

	bs := h.Sum(nil)

	node := c.redisClient.Get(context.Background(), fmt.Sprintf("%x", bs))

	res, err := c.httpClient.Get(fmt.Sprintf("%s/%s?filename=%s", node.Val(), "/retrieveFile", fileName))

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if http.StatusOK == res.StatusCode {
		body := res.Body
		defer body.Close()

		content, err := io.ReadAll(body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}

		w.Write(content)
		w.WriteHeader(res.StatusCode)
	}
}

func (c *clients) ReceiveFileAndSend(w http.ResponseWriter, r *http.Request) {

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

	h := sha256.New()

	h.Write([]byte(header.Filename))

	bs := h.Sum(nil)

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

		err = c.redisClient.Set(context.Background(), fmt.Sprintf("%x", bs), addr.address, 0).Err()

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		_, err = c.httpClient.Post(fmt.Sprintf(addr.address+"/receiveFile"), writer.FormDataContentType(), &buf)

		if err != nil {
			w.WriteHeader(http.StatusRequestTimeout)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (c *clients) calculateNodesUsage() ([]nodeWithUsage, error) {
	addresses := []string{
		"http://localhost:8080",
		"http://localhost:8081",
		"http://localhost:8082",
	}

	var nodes []nodeWithUsage

	for _, addr := range addresses {
		resp, err := c.httpClient.Get(fmt.Sprintf("%s/%s", addr, "/getCurrentNodeSpace"))
		defer resp.Body.Close()

		if err != nil {
			continue
		}

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
func (c *clients) nodesWithUsage(w http.ResponseWriter, r *http.Request) {
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

type clients struct {
	httpClient  http.Client
	redisClient *redis.Client
}

func newHttpClient() http.Client {
	return http.Client{Timeout: time.Duration(5) * time.Second}
}

func newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})
}
