package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"log"
	"net/http"
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
	fileManager *fileManager
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

	nodeManagerClient := &nodeManager{httpClient: &httpClient, mutex: mutex}
	fileManagerClient := &fileManager{nodeManager: nodeManagerClient, redisClient: redisClient, httpClient: &httpClient, mutex: mutex}
	clients := &clients{httpClient: httpClient, redisClient: redisClient, mutex: mutex, nodeManager: nodeManagerClient, fileManager: fileManagerClient}

	routerHttp := clients.SetupRouter()

	prometheus.MustRegister(httpRequestsTotal)

	err := http.ListenAndServe(fmt.Sprintf(":%d", serverPort), routerHttp)

	if err != nil {
		log.Fatalf(err.Error())
	}
}

func (c *clients) SetupRouter() *mux.Router {
	routerHttp := mux.NewRouter()

	routerHttp.HandleFunc("/", func(w http.ResponseWriter, request *http.Request) {
		httpRequestsTotal.WithLabelValues(request.Method, request.URL.Path).Inc()
		w.Write([]byte("Hello, Prometheus!"))
	})
	routerHttp.Handle("/metrics", promhttp.Handler())
	routerHttp.HandleFunc("/sendFile", c.fileManager.UploadFileAndDistributeBlocks).Methods("POST")
	routerHttp.HandleFunc("/nodesUsage", c.nodeManager.GetNodeUsage).Methods("GET")
	routerHttp.HandleFunc("/retrieveFile", c.fileManager.DownloadFile).Methods("GET")
	routerHttp.HandleFunc("/addNode", c.nodeManager.VerifyAndRegisterNode).Methods("POST")

	return routerHttp
}
