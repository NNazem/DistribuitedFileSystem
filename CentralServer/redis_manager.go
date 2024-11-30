package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
)

type RedisManager struct {
	redisClient *redis.Client
}

func (r *RedisManager) SendBlockHashWithNumberOfBlocks(blockHashedName []byte, blockLength int) error {
	return r.redisClient.Set(context.Background(), fmt.Sprintf("%x", blockHashedName), blockLength, 0).Err()
}

func (r *RedisManager) GetNumberOfBlocksOfAFile(fileHashedName []byte) (int, error) {
	return strconv.Atoi(r.redisClient.Get(context.Background(), fmt.Sprintf("%x", fileHashedName)).Val())
}
