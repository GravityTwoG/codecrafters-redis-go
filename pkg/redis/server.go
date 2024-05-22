package redis

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	redisstore "github.com/codecrafters-io/redis-starter-go/pkg/redis/store"
)

const SIMPLE_STRING_SPECIFIER = '+'
const ERROR_SPECIFIER = '-'
const INTEGER_SPECIFIER = ':'
const BULK_STRING_SPECIFIER = '$'
const ARRAY_SPECIFIER = '*'

type RedisCommand struct {
	Name       string
	Parameters []string
}

type redisServer struct {
	host string
	port string

	replicaOf string
	role      string

	store *redisstore.RedisStore
}

func NewRedisServer(host string, port string, replicaOf string) *redisServer {

	var role = "master"
	if replicaOf != "" {
		role = "slave"
	}

	return &redisServer{
		host: host,
		port: port,

		replicaOf: replicaOf,
		role:      role,

		store: redisstore.NewRedisStore(),
	}
}

func (r *redisServer) Start() {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", r.host, r.port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	wg := &sync.WaitGroup{}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			break
		}
		wg.Add(1)
		go func() {
			r.handleConnection(conn)
			defer conn.Close()
			defer wg.Done()
		}()
	}

	wg.Wait()
}

func (r *redisServer) handleConnection(conn net.Conn) {

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		r.handleCommand(reader, writer)
		writer.Flush()
	}
}

func (r *redisServer) handleCommand(reader *bufio.Reader, writer *bufio.Writer) {
	command := parseCommand(reader)
	if command == nil {
		return
	}

	if command.Name == "PING" {
		writeSimpleString(writer, "PONG")
		return
	}

	if command.Name == "ECHO" {
		writeBulkString(writer, command.Parameters[0])
		return
	}

	if command.Name == "SET" {
		r.handleSET(writer, command)
		return
	}

	if command.Name == "GET" {
		r.handleGET(writer, command)
		return
	}

	if command.Name == "INFO" {
		r.handleINFO(writer, command)
		return
	}

	writeError(writer, "ERROR")
}

func (r *redisServer) handleSET(writer *bufio.Writer, command *RedisCommand) {

	// SET foo bar
	if len(command.Parameters) == 2 {
		key := command.Parameters[0]
		value := command.Parameters[1]
		r.store.Set(key, value)
		writeSimpleString(writer, "OK")
		fmt.Printf("key: %s, value: %s\n", key, value)

		// SET foo bar PX 10000
	} else if len(command.Parameters) == 4 &&
		strings.ToUpper(command.Parameters[2]) == "PX" {

		key := command.Parameters[0]
		value := command.Parameters[1]
		durationMs, err := strconv.Atoi(command.Parameters[3])
		if err != nil {
			writeError(writer, "ERROR")
			return
		}

		duration := time.Duration(durationMs) * time.Millisecond
		r.store.SetWithTTL(key, value, duration)
		writeSimpleString(writer, "OK")
		fmt.Printf("key: %s, value: %s, duration: %s\n", key, value, duration)
	}
}

func (r *redisServer) handleGET(writer *bufio.Writer, command *RedisCommand) {
	if len(command.Parameters) == 1 {
		key := command.Parameters[0]
		value, ok, err := r.store.Get(key)
		if !ok {
			writeError(writer, "ERROR: Key not found")
			fmt.Printf("key: %s not found\n", key)
		} else if err != nil {
			if err.Error() == "EXPIRED" {
				writeNullBulkString(writer)
				fmt.Printf("key: %s expired\n", key)
			} else {
				writeError(writer, err.Error())
			}
		} else {
			writeBulkString(writer, value)
			fmt.Printf("key: %s, value: %s\n", key, value)
		}
	}
}

func (r *redisServer) handleINFO(writer *bufio.Writer, command *RedisCommand) {
	if len(command.Parameters) == 1 {
		key := command.Parameters[0]
		if strings.ToUpper(key) == "REPLICATION" {
			writeBulkString(writer, fmt.Sprintf(`# Replication\r\nrole:%s\r\nconnected_slaves:0\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:`, r.role))
			return
		}
	}

	writeError(writer, "ERROR")
}
