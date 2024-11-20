package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/klauspost/pgzip"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
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
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	u, err := url.ParseRequestURI(node.Url)

	if err != nil || u == nil {
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	urlString := u.String() + "/health"
	res, err := c.httpClient.Get(urlString)

	if err != nil || res.StatusCode != 200 {
		respondWithError(w, http.StatusInternalServerError, err.Error())
	}

	c.mutex.Lock()
	c.NodeAddresses = append(c.NodeAddresses, u.String())
	nodesWithUsage, err := c.FetchNodeUsageStats()
	if err == nil {
		c.NodeStats = nodesWithUsage
	}
	c.mutex.Unlock()
	w.WriteHeader(http.StatusOK)

	log.Println("Node added")
}

func (c *clients) DownloadFile(w http.ResponseWriter, r *http.Request) {
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

	nodesRes, err := c.FetchNodeUsageStats()

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
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
			respondWithError(w, http.StatusInternalServerError, err.Error())
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

	for i := 0; i < len(c.NodeStats); i++ {
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

func (c *clients) FetchNodeUsageStats() ([]NodeUsage, error) {
	var nodes []NodeUsage

	for _, addr := range c.NodeAddresses {
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
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	for _, tmp := range nodes {
		err := json.NewEncoder(w).Encode(map[string]float64{
			tmp.address: float64(tmp.usage),
		})
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
}
