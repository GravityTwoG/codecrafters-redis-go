package main

import (
	"fmt"

	redis "github.com/codecrafters-io/redis-starter-go/pkg/redis"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	redisServer := redis.NewRedisServer("localhost", "6379")
	redisServer.Start()
}
