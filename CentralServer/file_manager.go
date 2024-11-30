package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/klauspost/pgzip"
	"go.uber.org/zap"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"sync"
)

type fileManager struct {
	redisManager *RedisManager
	httpClient   *http.Client
	nodeManager  *nodeManager
	mutex        *sync.Mutex
}

func (f *fileManager) DownloadFile(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path).Inc()
	fileName := r.URL.Query().Get("fileName")

	logger.Info("Received request to download file",
		zap.String("fileName", fileName),
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
	)

	recomposedBytes, err := f.ReconstructFileFromBlocks(fileName)
	if err != nil {
		logger.Error("Failed to reconstruct file",
			zap.String("fileName", fileName),
			zap.Error(err),
		)
		respondWithError(w, http.StatusInternalServerError, "Failed to download file")
		return
	}

	_, err = w.Write(recomposedBytes)
	if err != nil {
		logger.Error("Failed to write response",
			zap.String("fileName", fileName),
			zap.Error(err),
		)
		respondWithError(w, http.StatusInternalServerError, "Failed to write response")
		return
	}

	logger.Info("Successfully served file",
		zap.String("fileName", fileName),
		zap.Int("responseSize", len(recomposedBytes)),
	)
	w.WriteHeader(http.StatusOK)
}

func (f *fileManager) ReconstructFileFromBlocks(filename string) ([]byte, error) {
	fileHashedName := GenerateFileHash(filename)
	var fileBytes []byte

	logger.Info("Starting file reconstruction",
		zap.String("fileName", filename),
		zap.String("hashedFileName", fmt.Sprintf("%x", fileHashedName)),
	)

	numOfBlocks, _ := f.redisManager.GetNumberOfBlocksOfAFile(fileHashedName)
	logger.Debug("Retrieved number of blocks",
		zap.String("fileName", filename),
		zap.Int("numOfBlocks", numOfBlocks),
	)

	for i := 0; i < numOfBlocks; i++ {
		fileBlockName := filename + "-block-" + strconv.Itoa(i+1)
		blockHash := GenerateFileHash(fileBlockName)
		formattedBs := fmt.Sprintf("%x", blockHash)

		fields := []string{"node_address", "block_hash"}
		values, err := f.redisManager.redisClient.HMGet(context.Background(), formattedBs, fields...).Result()
		if err != nil {
			logger.Error("Failed to retrieve block metadata from Redis",
				zap.String("blockName", fileBlockName),
				zap.String("blockHash", formattedBs),
				zap.Error(err),
			)
			return nil, err
		}

		nodeAddress := values[0]
		blockDataOriginalHash := values[1]

		logger.Debug("Block metadata retrieved",
			zap.String("blockName", fileBlockName),
			zap.Any("nodeAddress", nodeAddress),
			zap.Any("originalBlockHash", blockDataOriginalHash),
		)

		res, err := f.httpClient.Get(fmt.Sprintf("%s/%s?filename=%s", nodeAddress, "/retrieveFile", fileBlockName+".bin"))
		if err != nil || res.StatusCode != 200 {
			logger.Error("Failed to retrieve block from node",
				zap.String("blockName", fileBlockName),
				zap.Any("nodeAddress", nodeAddress),
				zap.Error(err),
			)
			return nil, errors.New("failed to retrieve block from node")
		}

		body := res.Body
		defer func(body io.ReadCloser) {
			err := body.Close()
			if err != nil {
				logger.Warn("Failed to close response body", zap.Error(err))
			}
		}(body)

		bodyByte, err := io.ReadAll(body)
		if err != nil {
			logger.Error("Failed to read block data",
				zap.String("blockName", fileBlockName),
				zap.Any("nodeAddress", nodeAddress),
				zap.Error(err),
			)
			return nil, err
		}

		blockDataHash := fmt.Sprintf("%x", GenerateBlockHash(bodyByte))
		if blockDataHash != blockDataOriginalHash {
			logger.Error("Block hash mismatch",
				zap.String("blockName", fileBlockName),
				zap.Any("expectedHash", blockDataOriginalHash),
				zap.String("actualHash", blockDataHash),
			)
			return nil, errors.New("block hash mismatch")
		}

		fileBytes = append(fileBytes, bodyByte...)
		logger.Debug("Block successfully appended",
			zap.String("blockName", fileBlockName),
			zap.Int("currentFileSize", len(fileBytes)),
		)
	}

	reader := bytes.NewReader(fileBytes)
	gz, err := pgzip.NewReader(reader)
	if err != nil {
		logger.Error("Failed to create gzip reader", zap.Error(err))
		return nil, err
	}
	defer func(gz *pgzip.Reader) {
		err := gz.Close()
		if err != nil {
			logger.Warn("Failed to close gzip reader", zap.Error(err))
		}
	}(gz)

	decompressedBytes, err := io.ReadAll(gz)
	if err != nil {
		logger.Error("Failed to decompress file", zap.Error(err))
		return nil, err
	}

	logger.Info("File reconstruction completed",
		zap.String("fileName", filename),
		zap.Int("finalFileSize", len(decompressedBytes)),
	)

	return decompressedBytes, nil
}

func (f *fileManager) UploadFileAndDistributeBlocks(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path).Inc()

	logger.Info("Starting file upload and distribution")

	file, header, err := r.FormFile("file")
	if err != nil {
		logger.Error("Failed to parse form file", zap.Error(err))
		respondWithError(w, http.StatusInternalServerError, "Failed to parse uploaded file")
		return
	}
	logger.Info("File received", zap.String("fileName", header.Filename))

	body, err := io.ReadAll(file)
	if err != nil {
		logger.Error("Failed to read uploaded file content", zap.Error(err))
		respondWithError(w, http.StatusInternalServerError, "Failed to read file content")
		return
	}

	var CompressedBuffer bytes.Buffer
	gz := pgzip.NewWriter(&CompressedBuffer)

	err = gz.SetConcurrency(100000, 10)
	if err != nil {
		logger.Error("Failed to set gzip concurrency", zap.Error(err))
		respondWithError(w, http.StatusInternalServerError, "Failed to set gzip concurrency")
		return
	}

	if _, err := gz.Write(body); err != nil {
		logger.Error("Failed to compress file", zap.Error(err))
		respondWithError(w, http.StatusInternalServerError, "Failed to compress file")
		return
	}

	if err := gz.Close(); err != nil {
		logger.Error("Failed to close gzip writer", zap.Error(err))
		respondWithError(w, http.StatusInternalServerError, "Failed to close compression stream")
		return
	}

	compressedData := CompressedBuffer.Bytes()
	logger.Info("File compression completed",
		zap.String("fileName", header.Filename),
		zap.Int("compressedSize", len(compressedData)),
	)

	listOfBlocks := SplitFileIntoBlocks(compressedData)
	hashedFileName := GenerateFileHash(header.Filename)
	logger.Info("File split into blocks",
		zap.String("fileName", header.Filename),
		zap.Int("numberOfBlocks", listOfBlocks.Len()),
	)

	err = f.redisManager.SendBlockHashWithNumberOfBlocks(hashedFileName, listOfBlocks.Len())
	if err != nil {
		logger.Error("Failed to store file metadata in Redis", zap.Error(err))
		respondWithError(w, http.StatusInternalServerError, "Failed to store file metadata")
		return
	}

	wg := sync.WaitGroup{}
	ErrorChannel := make(chan error, listOfBlocks.Len())

	nodesRes, err := f.nodeManager.RetrieveNodeStats()
	if err != nil {
		logger.Error("Failed to retrieve node statistics", zap.Error(err))
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve node statistics")
		return
	}

	f.nodeManager.NodeStats = nodesRes
	logger.Info("Node statistics retrieved", zap.Int("nodeCount", len(nodesRes)))

	for listOfBlocks.Len() > 0 {
		block := listOfBlocks.Front()
		listOfBlocks.Remove(block)
		wg.Add(1)

		go func(block FileBlock) {
			logger.Debug("Sending block to node",
				zap.Int("blockPosition", block.position),
			)
			f.SendBlockToNode(block, &wg, ErrorChannel, header)
		}(block.Value.(FileBlock))
	}

	wg.Wait()
	close(ErrorChannel)

	for err := range ErrorChannel {
		if err != nil && err.Error() != "" {
			logger.Error("Error during block distribution", zap.Error(err))
			respondWithError(w, http.StatusInternalServerError, "Error during block distribution")
			return
		}
	}

	logger.Info("File upload and distribution completed successfully", zap.String("fileName", header.Filename))
	w.WriteHeader(http.StatusOK)
}

func (f *fileManager) SendBlockToNode(block FileBlock, wg *sync.WaitGroup, errChan chan error, header *multipart.FileHeader) {
	defer wg.Done()

	logger.Info("Starting transmission for block",
		zap.Int("blockPosition", block.position),
		zap.String("fileName", header.Filename),
	)

	selectedNode := f.nodeManager.SelectAndUpdateNode(block)

	bs := GenerateFileHash(header.Filename + "-block-" + strconv.Itoa(block.position))

	if selectedNode.usage > 2*maxNodeSize {
		logger.Error("All nodes are full",
			zap.Int("blockPosition", block.position),
			zap.String("fileName", header.Filename),
		)
		errChan <- errors.New("all nodes are full")
		return
	}

	logger.Info("Preparing block for transmission",
		zap.Int("blockPosition", block.position),
		zap.String("fileName", header.Filename),
	)

	writer, data, blockDataHash, formattedBs, err := f.PrepareBlockForTransmission(block, header, bs)
	if err != nil {
		logger.Error("Failed to prepare block for transmission",
			zap.Int("blockPosition", block.position),
			zap.String("fileName", header.Filename),
			zap.Error(err),
		)
		errChan <- err
		return
	}

	for {
		logger.Info("Attempting to send block to node",
			zap.Int("blockPosition", block.position),
			zap.String("fileName", header.Filename),
			zap.String("nodeAddress", selectedNode.address),
		)

		err = f.TransmitBlock(formattedBs, selectedNode, blockDataHash, data, writer)

		if err == nil {
			logger.Info("Successfully transmitted block",
				zap.Int("blockPosition", block.position),
				zap.String("fileName", header.Filename),
				zap.String("nodeAddress", selectedNode.address),
			)
			return
		}

		logger.Error("Failed to transmit block",
			zap.Int("blockPosition", block.position),
			zap.String("fileName", header.Filename),
			zap.String("nodeAddress", selectedNode.address),
			zap.Error(err),
		)

		logger.Info("Removing node after failed transmission",
			zap.String("nodeAddress", selectedNode.address),
		)
		f.nodeManager.DeleteNode(selectedNode)

		if len(f.nodeManager.NodeStats) == 0 {
			logger.Error("No available nodes for block",
				zap.Int("blockPosition", block.position),
				zap.String("fileName", header.Filename),
			)
			errChan <- errors.New("no available nodes")
			return
		}

		selectedNode = f.nodeManager.SelectAndUpdateNode(block)
		logger.Info("Retrying transmission with new node",
			zap.Int("blockPosition", block.position),
			zap.String("fileName", header.Filename),
			zap.String("newNodeAddress", selectedNode.address),
		)
	}
}

func (f *fileManager) PrepareBlockForTransmission(block FileBlock, header *multipart.FileHeader, bs []byte) (*multipart.Writer, []byte, []byte, string, error) {
	context := fmt.Sprintf("block %d of file %s", block.position, header.Filename)

	logger.Info("Starting preparation for transmission",
		zap.Int("blockPosition", block.position),
		zap.String("fileName", header.Filename),
	)

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	fileName := header.Filename + "-block-" + strconv.Itoa(block.position) + ".bin"
	part, err := writer.CreateFormFile("file", fileName)
	if err != nil {
		logger.Error("Failed to create form file",
			zap.String("context", context),
			zap.Error(err),
		)
		return nil, nil, nil, "", fmt.Errorf("failed to create form file for %s: %w", context, err)
	}

	if _, err := part.Write(block.bytes); err != nil {
		logger.Error("Failed to write file data",
			zap.String("context", context),
			zap.Error(err),
		)
		return nil, nil, nil, "", fmt.Errorf("failed to write file data for %s: %w", context, err)
	}

	if err := writer.Close(); err != nil {
		logger.Error("Failed to close writer",
			zap.String("context", context),
			zap.Error(err),
		)
		return nil, nil, nil, "", fmt.Errorf("failed to close writer for %s: %w", context, err)
	}

	data, err := io.ReadAll(&buf)
	if err != nil {
		logger.Error("Failed to read data",
			zap.String("context", context),
			zap.Error(err),
		)
		return nil, nil, nil, "", fmt.Errorf("failed to read data for %s: %w", context, err)
	}

	blockDataHash := GenerateBlockHash(block.bytes)
	formattedBs := fmt.Sprintf("%x", bs)

	logger.Info("Successfully prepared block for transmission",
		zap.Int("blockPosition", block.position),
		zap.String("fileName", header.Filename),
		zap.String("blockHash", formattedBs),
	)

	return writer, data, blockDataHash, formattedBs, nil
}

func (f *fileManager) TransmitBlock(formattedBs string, selectedNode Node, blockDataHash []byte, data []byte, writer *multipart.Writer) error {
	logger.Info("Starting block transmission to Redis",
		zap.String("blockHash", formattedBs),
		zap.String("nodeAddress", selectedNode.address),
	)

	// Step 1: Memorizzazione su Redis
	err := f.redisManager.redisClient.HSet(context.Background(), formattedBs,
		"node_address", selectedNode.address,
		"block_hash", fmt.Sprintf("%x", blockDataHash),
	).Err()

	if err != nil {
		logger.Error("Failed to store blockHash in Redis",
			zap.String("blockHash", formattedBs),
			zap.Error(err),
		)
		return fmt.Errorf("failed to store blockHash in Redis: %w", err)
	}

	logger.Info("Successfully stored blockHash in Redis",
		zap.String("blockHash", formattedBs),
	)

	// Step 2: Trasmissione del blocco al nodo
	bufReader := bytes.NewReader(data)
	nodeURL := fmt.Sprintf("%s/receiveFile", selectedNode.address)

	logger.Info("Transmitting block to node",
		zap.String("nodeAddress", selectedNode.address),
		zap.String("nodeURL", nodeURL),
	)

	res, err := f.httpClient.Post(nodeURL, writer.FormDataContentType(), bufReader)
	if res != nil {
		defer res.Body.Close()
	}

	if err != nil {
		logger.Error("Failed to transmit block to node",
			zap.String("nodeAddress", selectedNode.address),
			zap.Error(err),
		)
		return fmt.Errorf("failed to transmit block to node %s: %w", selectedNode.address, err)
	}

	if res.StatusCode != http.StatusOK {
		logger.Warn("Node responded with non-OK status",
			zap.String("nodeAddress", selectedNode.address),
			zap.Int("statusCode", res.StatusCode),
		)
		return fmt.Errorf("unexpected response from node %s: status %d", selectedNode.address, res.StatusCode)
	}

	logger.Info("Successfully transmitted block to node",
		zap.String("nodeAddress", selectedNode.address),
		zap.String("blockHash", formattedBs),
	)

	return nil
}
