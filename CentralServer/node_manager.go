package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"sync"
)

type NodeManager interface {
	VerifyAndRegisterNode(w http.ResponseWriter, r *http.Request)
	RetrieveNodeStats() ([]NodeUsage, error)
}

type NodeUsage struct {
	address string
	usage   int
}

type NodeUsageResponse struct {
	Size int `json:"Size"`
}

type nodeManager struct {
	NodeAddresses []string
	NodeStats     []NodeUsage
	httpClient    http.Client
	mutex         *sync.Mutex
}

func (n *nodeManager) VerifyAndRegisterNode(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path).Inc()
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
	res, err := n.httpClient.Get(urlString)

	if err != nil || res.StatusCode != 200 {
		respondWithError(w, http.StatusInternalServerError, err.Error())
	}

	n.registerNode(u.String())
	w.WriteHeader(http.StatusOK)

	log.Println("Node added")
}

func (n *nodeManager) registerNode(node string) {
	n.mutex.Lock()
	n.NodeAddresses = append(n.NodeAddresses, node)
	nodesWithUsage, err := n.RetrieveNodeStats()
	if err == nil {
		n.NodeStats = nodesWithUsage
	}
	n.mutex.Unlock()
}

func (n *nodeManager) RetrieveNodeStats() ([]NodeUsage, error) {
	var nodes []NodeUsage

	for _, addr := range n.NodeAddresses {
		resp, err := n.httpClient.Get(fmt.Sprintf("%s/%s", addr, "/getCurrentNodeSpace"))

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
		return nil, errors.New("all the Nodes are currently unavailable. Please try again later")
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].usage < nodes[j].usage
	})

	return nodes, nil
}

func (n *nodeManager) GetNodeUsage(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path).Inc()
	nodes, err := n.RetrieveNodeStats()

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
