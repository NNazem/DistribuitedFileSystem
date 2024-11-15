package main

import (
	"bytes"
	"container/list"
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
	"strconv"
	"time"
)

const serverPort = 8000

const maxNodeSize = 128 * MB

const MB = 1024 * 1024

const KB = 1024

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

	bytes := c.recomposeFile(fileName)

	w.Write(bytes)
	w.WriteHeader(http.StatusOK)
}

func (c *clients) recomposeFile(filename string) []byte {
	bs := c.hashFileName(filename)
	var fileBytes []byte

	numOfBlocks, _ := strconv.Atoi(c.redisClient.Get(context.Background(), fmt.Sprintf("%x", bs)).Val())

	for i := range numOfBlocks {
		fileBlockName := filename + "-block-" + strconv.Itoa(i+1)
		bs := c.hashFileName(fileBlockName)

		node := c.redisClient.Get(context.Background(), fmt.Sprintf("%x", bs)).Val()

		res, _ := c.httpClient.Get(fmt.Sprintf("%s/%s?filename=%s", node, "/retrieveFile", fileBlockName+".bin"))

		body := res.Body
		defer body.Close()

		bodyByte, _ := io.ReadAll(body)

		fileBytes = append(fileBytes, bodyByte...)
	}

	return fileBytes
}

func (c *clients) hashFileName(fileName string) []byte {
	h := sha256.New()

	h.Write([]byte(fileName))

	bs := h.Sum(nil)
	return bs
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

	listOfBlocks := c.divideFileInBlocks(body)

	hashedFileName := c.hashFileName(header.Filename)

	err = c.redisClient.Set(context.Background(), fmt.Sprintf("%x", hashedFileName), listOfBlocks.Len(), 0).Err()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode("Error converting the filename to hash.")
	}

	currentBlock := 1

	for listOfBlocks.Len() > 0 {

		block := listOfBlocks.Front()
		listOfBlocks.Remove(block)
		blockByte, ok := block.Value.([]byte)

		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode("The server couldn't divide the file in blocks. Please try again later.")
			return
		}

		nodes, err := c.calculateNodesUsage()

		bs := c.hashFileName(header.Filename + "-block-" + strconv.Itoa(currentBlock))

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

			fileName := header.Filename + "-block-" + strconv.Itoa(currentBlock) + ".bin"
			part, err := writer.CreateFormFile("file", fileName)
			currentBlock++

			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			_, err = part.Write(blockByte)

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
	}
	w.WriteHeader(http.StatusOK)
}

func (c *clients) calculateNodesUsage() ([]nodeWithUsage, error) {
	addresses := []string{
		"http://localhost:8080",
		"http://localhost:8081",
		"http://localhost:8082",
		"http://localhost:8083",
		"http://localhost:8084",
		"http://localhost:8085",
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

func (c *clients) divideFileInBlocks(file []byte) *list.List {
	numOfBlocks := len(file) / (256 * KB)
	listOfBlocks := list.New()

	if numOfBlocks == 0 {
		listOfBlocks.PushBack(file)
		return listOfBlocks
	} else {
		for i := range numOfBlocks {
			tmpBlock := file[(64*KB)*i : ((64*KB)*i)+64*KB]
			listOfBlocks.PushBack(tmpBlock)
		}
		listOfBlocks.PushBack(file[((64 * KB) * numOfBlocks):])

	}

	return listOfBlocks
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
