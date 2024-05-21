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

type RedisValue struct {
	Value     string
	ExpiresAt *time.Time
}

type redisServer struct {
	host string
	port string

	mutex *sync.Mutex
	store map[string]RedisValue
}

func NewRedisServer(host string, port string) *redisServer {

	return &redisServer{
		host: host,
		port: port,

		mutex: &sync.Mutex{},
		store: make(map[string]RedisValue),
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
	} else if command.Name == "ECHO" {
		writeBulkString(writer, command.Parameters[0])
	} else if command.Name == "SET" {
		r.handleSET(writer, command)
	} else if command.Name == "GET" {
		r.handleGET(writer, command)
	} else {
		writeError(writer, "ERROR")
	}
}

func (r *redisServer) handleSET(writer *bufio.Writer, command *RedisCommand) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// SET foo bar
	if len(command.Parameters) == 2 {
		key := command.Parameters[0]
		value := command.Parameters[1]
		r.store[key] = RedisValue{
			Value:     value,
			ExpiresAt: nil,
		}

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

		expiresAt := time.Now().Add(time.Duration(durationMs) * time.Millisecond)
		r.store[key] = RedisValue{
			Value:     value,
			ExpiresAt: &expiresAt,
		}

		writeSimpleString(writer, "OK")
		fmt.Printf("key: %s, value: %s, expiresAt: %s\n", key, value, expiresAt)
	}
}

func (r *redisServer) handleGET(writer *bufio.Writer, command *RedisCommand) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(command.Parameters) == 1 {
		key := command.Parameters[0]
		value, ok := r.store[key]
		if !ok {
			writeError(writer, "ERROR: Key not found")
			fmt.Printf("key: %s not found\n", key)
		} else if value.ExpiresAt != nil && value.ExpiresAt.Before(time.Now()) {
			writeNullBulkString(writer)
			fmt.Printf("key: %s expired\n", key)
		} else {
			writeBulkString(writer, value.Value)
			fmt.Printf("key: %s, value: %s\n", key, value.Value)
		}
	}
}
