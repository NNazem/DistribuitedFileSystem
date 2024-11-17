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
	pgzip "github.com/klauspost/pgzip"
	"github.com/redis/go-redis/v9"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const serverPort = 8000

const maxNodeSize = 128 * MB

const MB = 1024 * 1024

const KB = 1024

type BlockStruct struct {
	bytes    []byte
	position int
}

type nodeWithUsage struct {
	address string
	usage   int
}

type nodeResponse struct {
	Size int `json:"Size"`
}

type clients struct {
	httpClient  http.Client
	redisClient *redis.Client
	nodes       []nodeWithUsage
	mutex       sync.Mutex
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

	recompesedBytes := c.recomposeFile(fileName)

	_, err := w.Write(recompesedBytes)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
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

	reader := bytes.NewReader(fileBytes)

	gz, _ := pgzip.NewReader(reader)
	defer gz.Close()

	decompressedBytes, _ := io.ReadAll(gz)

	return decompressedBytes
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

	var zipBuf bytes.Buffer

	gz := pgzip.NewWriter(&zipBuf)

	err = gz.SetConcurrency(100000, 10)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(err.Error())
		return
	}

	if _, err := gz.Write(body); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := gz.Close(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	compressedData := zipBuf.Bytes()

	listOfBlocks := c.divideFileInBlocks(compressedData)

	hashedFileName := c.hashFileName(header.Filename)

	err = c.redisClient.Set(context.Background(), fmt.Sprintf("%x", hashedFileName), listOfBlocks.Len(), 0).Err()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode("Error converting the filename to hash.")
	}

	wg := sync.WaitGroup{}
	errChan := make(chan error, listOfBlocks.Len())

	nodesRes, err := c.calculateNodesUsage()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode("Error retrieving space from nodes.")
	}

	c.nodes = nodesRes

	for listOfBlocks.Len() > 0 {

		block := listOfBlocks.Front()
		listOfBlocks.Remove(block)
		wg.Add(1)

		go func(block BlockStruct) {
			c.sendBlock(block, &wg, errChan, header)
		}(block.Value.(BlockStruct))
	}

	wg.Wait()

	close(errChan)

	for err := range errChan {
		if err != nil && err.Error() != "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (c *clients) sendBlock(block BlockStruct, wg *sync.WaitGroup, errChan chan error, header *multipart.FileHeader) {
	defer wg.Done()

	c.mutex.Lock()
	selectedNode := c.nodes[0]
	c.nodes[0].usage = selectedNode.usage + len(block.bytes)
	sort.Slice(c.nodes, func(i, j int) bool {
		return c.nodes[i].usage < c.nodes[j].usage
	})
	c.mutex.Unlock()

	bs := c.hashFileName(header.Filename + "-block-" + strconv.Itoa(block.position))

	if selectedNode.usage > 2*maxNodeSize {
		errChan <- errors.New("All the nodes are currently full. Please try again later.")
		return
	}
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	fileName := header.Filename + "-block-" + strconv.Itoa(block.position) + ".bin"
	part, err := writer.CreateFormFile("file", fileName)

	if err != nil {
		errChan <- errors.New("The server couldn't create the form file. Please try again later.")
		return
	}

	_, err = part.Write(block.bytes)

	if err != nil {
		errChan <- errors.New("The server couldn't write the file data to the response. Please try again later.")
		return
	}

	err = writer.Close()

	if err != nil {
		errChan <- errors.New("The server couldn't close the writer. Please try again later")
		return
	}

	err = c.redisClient.Set(context.Background(), fmt.Sprintf("%x", bs), selectedNode.address, 0).Err()

	if err != nil {
		errChan <- errors.New("The server couldn't communicate con redis. Please try again later.")
		return
	}

	_, err = c.httpClient.Post(fmt.Sprintf(selectedNode.address+"/receiveFile"), writer.FormDataContentType(), &buf)

	if err != nil {
		errChan <- errors.New("The server couldn't communicate with the node. Please try again later")
		return
	}

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
			usage:   nodeResp.Size,
		})
	}

	if len(nodes) == 0 {
		return nil, errors.New("All the nodes are currently unavailable. Please try again later.")
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

	maxBlockSize := 128 * MB
	numOfBlocks := len(file) / maxBlockSize
	listOfBlocks := list.New()

	if numOfBlocks == 0 {
		listOfBlocks.PushBack(BlockStruct{bytes: file, position: 1})
		return listOfBlocks
	} else {
		for i := range numOfBlocks {
			tmpBlock := file[maxBlockSize*i : (maxBlockSize*i)+maxBlockSize]
			listOfBlocks.PushBack(BlockStruct{bytes: tmpBlock, position: i + 1})
		}
		listOfBlocks.PushBack(BlockStruct{bytes: file[(maxBlockSize * numOfBlocks):], position: numOfBlocks + 1})
	}

	return listOfBlocks
}

func (c *clients) hashFileName(fileName string) []byte {
	h := sha256.New()

	h.Write([]byte(fileName))

	bs := h.Sum(nil)
	return bs
}
