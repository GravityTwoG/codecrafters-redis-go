package redis

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
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

	store map[string]string
}

func NewRedisServer(host string, port string) *redisServer {

	return &redisServer{
		host: host,
		port: port,

		store: make(map[string]string),
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
	} else if command.Name == "SET" && len(command.Parameters) == 2 {
		r.store[command.Parameters[0]] = command.Parameters[1]
		writeSimpleString(writer, "OK")
	} else if command.Name == "GET" && len(command.Parameters) == 1 {
		value, ok := r.store[command.Parameters[0]]
		if !ok {
			writeError(writer, "ERROR: Key not found")
		} else {
			writeBulkString(writer, value)
		}
	} else {
		writeError(writer, "ERROR")
	}
}
