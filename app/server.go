package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	redis "github.com/codecrafters-io/redis-starter-go/pkg/redis"
)

func main() {
	config := redis.RedisConfig{
		Host:       "localhost",
		Port:       "6379",
		ReplicaOf:  "",
		Dir:        "",
		DBFilename: "",
	}
	flag.StringVar(&config.Port, "port", "6379", "port to listen on")
	flag.StringVar(&config.ReplicaOf, "replicaof", "", "replica of host port")
	flag.StringVar(&config.Dir, "dir", "", "directory to store RDB file")
	flag.StringVar(&config.DBFilename, "dbfilename", "", "filename of RDB file")
	flag.Parse()

	config.ReplicaOf = strings.Replace(config.ReplicaOf, " ", ":", -1)

	redisServer := redis.NewRedisServer(&config)

	// graceful shutdown
	go func() {
		exit := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
		signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

		<-exit
		fmt.Println("Shutting down...")
		redisServer.Stop()
	}()

	redisServer.Start()
}
