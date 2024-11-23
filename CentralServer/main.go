package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/klauspost/pgzip"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
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

var httpRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests received, labeled by method and path.",
	},
	[]string{"method", "path"},
)

type FileBlock struct {
	bytes    []byte
	position int
}

type NodeRegistrationRequest struct {
	Url string `json:"Url"`
}

type clients struct {
	httpClient  http.Client
	redisClient *redis.Client
	mutex       *sync.Mutex
	nodeManager *nodeManager
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
	httpClient := newHttpClient()
	redisClient := newRedisClient()
	mutex := &sync.Mutex{}

	nodeManagerClient := &nodeManager{httpClient: httpClient, mutex: mutex}
	clients := &clients{httpClient: httpClient, redisClient: redisClient, mutex: mutex, nodeManager: nodeManagerClient}

	routerHttp := mux.NewRouter()

	prometheus.MustRegister(httpRequestsTotal)
	routerHttp.HandleFunc("/", func(w http.ResponseWriter, request *http.Request) {
		httpRequestsTotal.WithLabelValues(request.Method, request.URL.Path).Inc()
		w.Write([]byte("Hello, Prometheus!"))
	})
	routerHttp.Handle("/metrics", promhttp.Handler())
	routerHttp.HandleFunc("/sendFile", clients.UploadAndDistributeFile).Methods("POST")
	routerHttp.HandleFunc("/nodesUsage", clients.nodeManager.GetNodeUsage).Methods("GET")
	routerHttp.HandleFunc("/retrieveFile", clients.DownloadFile).Methods("GET")
	routerHttp.HandleFunc("/addNode", clients.nodeManager.VerifyAndRegisterNode).Methods("POST")

	err := http.ListenAndServe(fmt.Sprintf(":%d", serverPort), routerHttp)

	if err != nil {
		os.Exit(-1)
	}
}

func (c *clients) DownloadFile(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path).Inc()
	fileName := r.URL.Query().Get("fileName")

	recomposedBytes, err := c.ReassembleFile(fileName)

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	_, err = w.Write(recomposedBytes)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *clients) ReassembleFile(filename string) ([]byte, error) {
	bs := GenerateFileHash(filename)
	var fileBytes []byte

	numOfBlocks, _ := strconv.Atoi(c.redisClient.Get(context.Background(), fmt.Sprintf("%x", bs)).Val())

	for i := range numOfBlocks {
		fileBlockName := filename + "-block-" + strconv.Itoa(i+1)
		bs := GenerateFileHash(fileBlockName)
		formattedBs := fmt.Sprintf("%x", bs)

		fields := []string{"node_address", "block_hash"}
		values, err := c.redisClient.HMGet(context.Background(), formattedBs, fields...).Result()

		if err != nil {
			return nil, err
		}

		nodeAddress := values[0]
		blockDataOriginalHash := values[1]

		log.Println(values)

		res, _ := c.httpClient.Get(fmt.Sprintf("%s/%s?filename=%s", nodeAddress, "/retrieveFile", fileBlockName+".bin"))

		body := res.Body
		defer func(body io.ReadCloser, err error) {
			errInsideClosure := body.Close()
			if errInsideClosure != nil {
				err = errInsideClosure
			}
		}(body, err)

		bodyByte, _ := io.ReadAll(body)

		bs2 := GenerateBlockHash(bodyByte)
		blockDataHash := fmt.Sprintf("%x", bs2)

		if blockDataHash != blockDataOriginalHash {
			return nil, errors.New("the hash of the block doesn't match")
		}

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
	httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path).Inc()
	file, header, err := r.FormFile("file")

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	body, err := io.ReadAll(file)

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var CompressedBuffer bytes.Buffer

	gz := pgzip.NewWriter(&CompressedBuffer)

	err = gz.SetConcurrency(100000, 10)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if _, err := gz.Write(body); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := gz.Close(); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	compressedData := CompressedBuffer.Bytes()

	listOfBlocks := SplitFileIntoBlocks(compressedData)

	hashedFileName := GenerateFileHash(header.Filename)

	err = c.redisClient.Set(context.Background(), fmt.Sprintf("%x", hashedFileName), listOfBlocks.Len(), 0).Err()

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	wg := sync.WaitGroup{}
	ErrorChannel := make(chan error, listOfBlocks.Len())

	nodesRes, err := c.nodeManager.RetrieveNodeStats()

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	c.nodeManager.NodeStats = nodesRes

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
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (c *clients) DistributeBlock(block FileBlock, wg *sync.WaitGroup, errChan chan error, header *multipart.FileHeader) {
	defer wg.Done()

	c.mutex.Lock()
	selectedNode := c.nodeManager.NodeStats[0]
	c.nodeManager.NodeStats[0].usage = selectedNode.usage + len(block.bytes)
	sort.Slice(c.nodeManager.NodeStats, func(i, j int) bool {
		return c.nodeManager.NodeStats[i].usage < c.nodeManager.NodeStats[j].usage
	})
	c.mutex.Unlock()

	bs := GenerateFileHash(header.Filename + "-block-" + strconv.Itoa(block.position))

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

	data, err := io.ReadAll(&buf)

	if err != nil {
		errChan <- err
	}

	blockDataHash := GenerateBlockHash(block.bytes)

	formattedBs := fmt.Sprintf("%x", bs)

	err = c.redisClient.HSet(context.Background(), formattedBs,
		"node_address", selectedNode.address,
		"block_hash", fmt.Sprintf("%x", blockDataHash),
	).Err()

	if err != nil {
		errChan <- err
		return
	}

	for i := 0; i < len(c.nodeManager.NodeStats); i++ {
		bufReader := bytes.NewReader(data)
		res, err := c.httpClient.Post(fmt.Sprintf(selectedNode.address+"/receiveFile"), writer.FormDataContentType(), bufReader)

		if res != nil {
			defer res.Body.Close()
		}

		if err == nil && res.StatusCode == 200 {
			return
		}

		if err != nil {
			errChan <- err
			return
		}
	}

	errChan <- errors.New("the server couldn't communicate with the nodes. Please try again later")
	return

}
