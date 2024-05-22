package main

import (
	"flag"
	"fmt"
	"strings"

	redis "github.com/codecrafters-io/redis-starter-go/pkg/redis"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	var port string
	var replicaOf string
	flag.StringVar(&port, "port", "6379", "port to listen on")
	flag.StringVar(&replicaOf, "replicaof", "", "replica of host port")
	flag.Parse()

	replicaOf = strings.Replace(replicaOf, " ", ":", -1)

	redisServer := redis.NewRedisServer("localhost", port, replicaOf)
	redisServer.Start()
}
