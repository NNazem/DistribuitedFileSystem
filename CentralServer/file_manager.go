package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/klauspost/pgzip"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"sort"
	"strconv"
	"sync"
)

type fileManager struct {
	redisClient *redis.Client
	httpClient  *http.Client
	nodeManager *nodeManager
	mutex       *sync.Mutex
}

func (f *fileManager) DownloadFile(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path).Inc()
	fileName := r.URL.Query().Get("fileName")

	recomposedBytes, err := f.ReconstructFileFromBlocks(fileName)

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

func (f *fileManager) ReconstructFileFromBlocks(filename string) ([]byte, error) {
	bs := GenerateFileHash(filename)
	var fileBytes []byte

	numOfBlocks, _ := strconv.Atoi(f.redisClient.Get(context.Background(), fmt.Sprintf("%x", bs)).Val())

	for i := range numOfBlocks {
		fileBlockName := filename + "-block-" + strconv.Itoa(i+1)
		bs := GenerateFileHash(fileBlockName)
		formattedBs := fmt.Sprintf("%x", bs)

		fields := []string{"node_address", "block_hash"}
		values, err := f.redisClient.HMGet(context.Background(), formattedBs, fields...).Result()

		if err != nil {
			return nil, err
		}

		nodeAddress := values[0]
		blockDataOriginalHash := values[1]

		log.Println(values)

		res, _ := f.httpClient.Get(fmt.Sprintf("%s/%s?filename=%s", nodeAddress, "/retrieveFile", fileBlockName+".bin"))

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

func (f *fileManager) UploadFileAndDistributeBlocks(w http.ResponseWriter, r *http.Request) {
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

	err = f.redisClient.Set(context.Background(), fmt.Sprintf("%x", hashedFileName), listOfBlocks.Len(), 0).Err()

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	wg := sync.WaitGroup{}
	ErrorChannel := make(chan error, listOfBlocks.Len())

	nodesRes, err := f.nodeManager.RetrieveNodeStats()

	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	f.nodeManager.NodeStats = nodesRes

	for listOfBlocks.Len() > 0 {

		block := listOfBlocks.Front()
		listOfBlocks.Remove(block)
		wg.Add(1)

		go func(block FileBlock) {
			f.SendBlockToNode(block, &wg, ErrorChannel, header)
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

func (f *fileManager) SendBlockToNode(block FileBlock, wg *sync.WaitGroup, errChan chan error, header *multipart.FileHeader) {
	defer wg.Done()

	selectedNode := f.SelectAndUpdateNode(block)

	bs := GenerateFileHash(header.Filename + "-block-" + strconv.Itoa(block.position))

	if selectedNode.usage > 2*maxNodeSize {
		errChan <- errors.New("all the NodeStats are currently full. Please try again later")
		return
	}

	writer, err, errChan, data, blockDataHash, formattedBs, done := f.PrepareBlockForTransmission(block, header, errChan, bs)

	if !done {
		return
	}

	errChan, done2 := f.TransmitBlock(err, formattedBs, selectedNode, blockDataHash, errChan, data, writer)
	if done2 {
		return
	}

	errChan <- errors.New("the server couldn't communicate with the nodes. Please try again later")
	return

}

func (f *fileManager) PrepareBlockForTransmission(block FileBlock, header *multipart.FileHeader, errChan chan error, bs []byte) (*multipart.Writer, error, chan error, []byte, []byte, string, bool) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	fileName := header.Filename + "-block-" + strconv.Itoa(block.position) + ".bin"
	part, err := writer.CreateFormFile("file", fileName)

	if err != nil {
		errChan <- errors.New("the server couldn't create the form file. Please try again later")
		return nil, nil, nil, nil, nil, "", false
	}

	_, err = part.Write(block.bytes)

	if err != nil {
		errChan <- errors.New("the server couldn't write the file data to the response. Please try again later")
		return nil, nil, nil, nil, nil, "", false
	}

	err = writer.Close()

	if err != nil {
		errChan <- errors.New("the server couldn't close the writer. Please try again later")
		return nil, nil, nil, nil, nil, "", false
	}

	data, err := io.ReadAll(&buf)

	if err != nil {
		errChan <- err
	}

	blockDataHash := GenerateBlockHash(block.bytes)

	formattedBs := fmt.Sprintf("%x", bs)
	return writer, err, errChan, data, blockDataHash, formattedBs, true
}

func (f *fileManager) TransmitBlock(err error, formattedBs string, selectedNode NodeUsage, blockDataHash []byte, errChan chan error, data []byte, writer *multipart.Writer) (chan error, bool) {
	err = f.redisClient.HSet(context.Background(), formattedBs,
		"node_address", selectedNode.address,
		"block_hash", fmt.Sprintf("%x", blockDataHash),
	).Err()

	if err != nil {
		errChan <- err
		return nil, true
	}

	for i := 0; i < len(f.nodeManager.NodeStats); i++ {
		bufReader := bytes.NewReader(data)
		res, err := f.httpClient.Post(fmt.Sprintf(selectedNode.address+"/receiveFile"), writer.FormDataContentType(), bufReader)

		if res != nil {
			defer res.Body.Close()
		}

		if err == nil && res.StatusCode == 200 {
			return nil, true
		}

		if err != nil {
			errChan <- err
			return nil, true
		}
	}
	return errChan, false
}

func (f *fileManager) SelectAndUpdateNode(block FileBlock) NodeUsage {
	f.mutex.Lock()
	selectedNode := f.nodeManager.NodeStats[0]
	f.nodeManager.NodeStats[0].usage = selectedNode.usage + len(block.bytes)
	sort.Slice(f.nodeManager.NodeStats, func(i, j int) bool {
		return f.nodeManager.NodeStats[i].usage < f.nodeManager.NodeStats[j].usage
	})
	f.mutex.Unlock()
	return selectedNode
}
