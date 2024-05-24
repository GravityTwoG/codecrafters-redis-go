package main

import (
	"flag"
	"strings"

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

	redisstore := redis.NewRedisServer(&config)
	redisstore.Start()
}
