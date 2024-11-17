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
	"github.com/klauspost/pgzip"
	"github.com/redis/go-redis/v9"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const serverPort = 8000

const maxNodeSize = 128 * MB

const MB = 1024 * 1024

type FileBlock struct {
	bytes    []byte
	position int
}

type NodeUsage struct {
	address string
	usage   int
}

type NodeUsageResponse struct {
	Size int `json:"Size"`
}

type NodeRegistrationRequest struct {
	Url string `json:"Url"`
}

type clients struct {
	httpClient    http.Client
	redisClient   *redis.Client
	NodeAddresses []string
	NodeStats     []NodeUsage
	mutex         sync.Mutex
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

	routerHttp.HandleFunc("/sendFile", clients.UploadAndDistributeFile).Methods("POST")
	routerHttp.HandleFunc("/nodesUsage", clients.GetNodeUsage).Methods("GET")
	routerHttp.HandleFunc("/retrieveFile", clients.DownloadFile).Methods("GET")
	routerHttp.HandleFunc("/addNode", clients.RegisterNode).Methods("POST")

	err := http.ListenAndServe(fmt.Sprintf(":%d", serverPort), routerHttp)

	if err != nil {
		os.Exit(-1)
	}
}

func (c *clients) RegisterNode(w http.ResponseWriter, r *http.Request) {
	var node NodeRegistrationRequest
	err := json.NewDecoder(r.Body).Decode(&node)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		err := json.NewEncoder(w).Encode("Please send a valid request")
		if err != nil {
			return
		}
	}

	u, err := url.ParseRequestURI(node.Url)

	if err != nil || u == nil {
		w.WriteHeader(http.StatusBadRequest)
		err := json.NewEncoder(w).Encode("Please send a valid URL")
		if err != nil {
			return
		}
		return
	}

	res, err := c.httpClient.Get(u.String() + "/health")

	if err != nil || res.StatusCode != 200 {
		w.WriteHeader(http.StatusInternalServerError)
		err := json.NewEncoder(w).Encode("The new node  cannot be verified. Please try again later.")
		if err != nil {
			return
		}
		return
	}

	c.mutex.Lock()
	c.NodeAddresses = append(c.NodeAddresses, u.String())
	nodesWithUsage, err := c.FetchNodeUsageStats()
	if err == nil {
		c.NodeStats = nodesWithUsage
	}
	c.mutex.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (c *clients) DownloadFile(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("fileName")

	recomposedBytes, err := c.ReassembleFile(fileName)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, err = w.Write(recomposedBytes)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *clients) ReassembleFile(filename string) ([]byte, error) {
	bs := c.GenerateFileHash(filename)
	var fileBytes []byte

	numOfBlocks, _ := strconv.Atoi(c.redisClient.Get(context.Background(), fmt.Sprintf("%x", bs)).Val())

	for i := range numOfBlocks {
		fileBlockName := filename + "-block-" + strconv.Itoa(i+1)
		bs := c.GenerateFileHash(fileBlockName)

		node := c.redisClient.Get(context.Background(), fmt.Sprintf("%x", bs)).Val()

		res, _ := c.httpClient.Get(fmt.Sprintf("%s/%s?filename=%s", node, "/retrieveFile", fileBlockName+".bin"))

		body := res.Body
		var err error
		defer func(body io.ReadCloser, err error) {
			errInsideClosure := body.Close()
			if errInsideClosure != nil {
				err = errInsideClosure
			}
		}(body, err)

		bodyByte, _ := io.ReadAll(body)

		fileBytes = append(fileBytes, bodyByte...)
	}

	reader := bytes.NewReader(fileBytes)

	gz, _ := pgzip.NewReader(reader)
	var err error
	defer func(gz *pgzip.Reader) {
		errDefer := gz.Close()
		if err != nil {
			err = errDefer
		}
	}(gz)

	if err != nil {
		return nil, err
	}

	decompressedBytes, _ := io.ReadAll(gz)

	return decompressedBytes, nil
}

func (c *clients) UploadAndDistributeFile(w http.ResponseWriter, r *http.Request) {
	file, header, err := r.FormFile("file")

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		err := json.NewEncoder(w).Encode("Please, insert a file.")
		if err != nil {
			return
		}
		return
	}

	body, err := io.ReadAll(file)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var CompressedBuffer bytes.Buffer

	gz := pgzip.NewWriter(&CompressedBuffer)

	err = gz.SetConcurrency(100000, 10)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		err := json.NewEncoder(w).Encode(err.Error())
		if err != nil {
			return
		}
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

	compressedData := CompressedBuffer.Bytes()

	listOfBlocks := c.SplitFileIntoBlocks(compressedData)

	hashedFileName := c.GenerateFileHash(header.Filename)

	err = c.redisClient.Set(context.Background(), fmt.Sprintf("%x", hashedFileName), listOfBlocks.Len(), 0).Err()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		err := json.NewEncoder(w).Encode("Error converting the filename to hash.")
		if err != nil {
			return
		}
	}

	wg := sync.WaitGroup{}
	ErrorChannel := make(chan error, listOfBlocks.Len())

	nodesRes, err := c.FetchNodeUsageStats()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		err := json.NewEncoder(w).Encode("Error retrieving space from NodeStats.")
		if err != nil {
			return
		}
	}

	c.NodeStats = nodesRes

	for listOfBlocks.Len() > 0 {

		block := listOfBlocks.Front()
		listOfBlocks.Remove(block)
		wg.Add(1)

		go func(block FileBlock) {
			c.DistributeBlock(block, &wg, ErrorChannel, header)
		}(block.Value.(FileBlock))
	}

	wg.Wait()

	close(ErrorChannel)

	for err := range ErrorChannel {
		if err != nil && err.Error() != "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (c *clients) DistributeBlock(block FileBlock, wg *sync.WaitGroup, errChan chan error, header *multipart.FileHeader) {
	defer wg.Done()

	c.mutex.Lock()
	selectedNode := c.NodeStats[0]
	c.NodeStats[0].usage = selectedNode.usage + len(block.bytes)
	sort.Slice(c.NodeStats, func(i, j int) bool {
		return c.NodeStats[i].usage < c.NodeStats[j].usage
	})
	c.mutex.Unlock()

	bs := c.GenerateFileHash(header.Filename + "-block-" + strconv.Itoa(block.position))

	if selectedNode.usage > 2*maxNodeSize {
		errChan <- errors.New("all the NodeStats are currently full. Please try again later")
		return
	}
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	fileName := header.Filename + "-block-" + strconv.Itoa(block.position) + ".bin"
	part, err := writer.CreateFormFile("file", fileName)

	if err != nil {
		errChan <- errors.New("the server couldn't create the form file. Please try again later")
		return
	}

	_, err = part.Write(block.bytes)

	if err != nil {
		errChan <- errors.New("the server couldn't write the file data to the response. Please try again later")
		return
	}

	err = writer.Close()

	if err != nil {
		errChan <- errors.New("the server couldn't close the writer. Please try again later")
		return
	}

	err = c.redisClient.Set(context.Background(), fmt.Sprintf("%x", bs), selectedNode.address, 0).Err()

	if err != nil {
		errChan <- errors.New("the server couldn't communicate con redis. Please try again later")
		return
	}

	_, err = c.httpClient.Post(fmt.Sprintf(selectedNode.address+"/receiveFile"), writer.FormDataContentType(), &buf)

	if err != nil {
		errChan <- errors.New("the server couldn't communicate with the node. Please try again later")
		return
	}

}

func (c *clients) FetchNodeUsageStats() ([]NodeUsage, error) {
	addresses := []string{
		"http://localhost:8080",
		"http://localhost:8081",
		"http://localhost:8082",
		"http://localhost:8083",
		"http://localhost:8084",
		"http://localhost:8085",
	}

	var nodes []NodeUsage

	for _, addr := range addresses {
		resp, err := c.httpClient.Get(fmt.Sprintf("%s/%s", addr, "/getCurrentNodeSpace"))

		if err != nil {
			continue
		}

		defer func(Body io.ReadCloser) {
			errDefer := Body.Close()
			if errDefer != nil {
				err = errDefer
			}
		}(resp.Body)

		if err != nil {
			return nil, err
		}

		var nodeResp NodeUsageResponse

		err = json.NewDecoder(resp.Body).Decode(&nodeResp)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, NodeUsage{
			address: addr,
			usage:   nodeResp.Size,
		})
	}

	if len(nodes) == 0 {
		return nil, errors.New("all the NodeStats are currently unavailable. Please try again later")
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].usage < nodes[j].usage
	})

	return nodes, nil
}
func (c *clients) GetNodeUsage(w http.ResponseWriter, _ *http.Request) {
	nodes, err := c.FetchNodeUsageStats()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		err := json.NewEncoder(w).Encode(err.Error())
		if err != nil {
			return
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	for _, tmp := range nodes {
		err := json.NewEncoder(w).Encode(map[string]float64{
			tmp.address: float64(tmp.usage),
		})
		if err != nil {
			return
		}
	}
}

func (c *clients) SplitFileIntoBlocks(file []byte) *list.List {

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

func (c *clients) GenerateFileHash(fileName string) []byte {
	h := sha256.New()

	h.Write([]byte(fileName))

	bs := h.Sum(nil)
	return bs
}
