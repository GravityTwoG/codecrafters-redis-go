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

	"github.com/codecrafters-io/redis-starter-go/pkg/redis/rand"
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

	role string

	slavePorts      []string
	connectedSlaves int

	replicaOf         string
	replicationId     string
	replicationOffset int

	store *redisstore.RedisStore
}

func NewRedisServer(host string, port string, replicaOf string) *redisServer {

	var role = "master"
	var replicationId = rand.RandString(40)
	if replicaOf != "" {
		role = "slave"
		replicationId = ""
	}

	return &redisServer{
		host: host,
		port: port,

		role: role,

		slavePorts:      make([]string, 0),
		connectedSlaves: 0,

		replicaOf:         replicaOf,
		replicationId:     replicationId,
		replicationOffset: 0,

		store: redisstore.NewRedisStore(),
	}
}

func (r *redisServer) Start() {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", r.host, r.port))
	if err != nil {
		fmt.Printf("Failed to bind to port %s: %s\n", r.port, err.Error())
		os.Exit(1)
	}
	defer l.Close()

	if r.role == "slave" {
		r.handleReplication()
	}

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

	if command.Name == "REPLCONF" {
		r.handleREPLCONF(writer, command)
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
			response := "# Replication\r\n"
			response += fmt.Sprintf("role:%s\r\n", r.role)
			response += fmt.Sprintf("connected_slaves:%d\r\n", r.connectedSlaves)
			response += fmt.Sprintf("master_replid:%s\r\n", r.replicationId)
			response += fmt.Sprintf("master_repl_offset:%d\r\n", r.replicationOffset)
			response += fmt.Sprintf("second_repl_offset:-1\r\n")
			response += fmt.Sprintf("repl_backlog_active:0\r\n")
			response += fmt.Sprintf("repl_backlog_size:1048576\r\n")
			response += fmt.Sprintf("repl_backlog_first_byte_offset:0\r\n")
			response += fmt.Sprintf("repl_backlog_histlen:0")
			writeBulkString(writer, response)
			return
		}
	}

	writeError(writer, "ERROR")
}

func (r *redisServer) handleREPLCONF(writer *bufio.Writer, command *RedisCommand) {
	if len(command.Parameters) != 2 {
		writeError(writer, "ERROR")
		return
	}

	if strings.ToUpper(command.Parameters[0]) == "listening-port" {
		slavePort := command.Parameters[1]
		r.slavePorts = append(r.slavePorts, slavePort)
		writeSimpleString(writer, "OK")
		return
	}

	if strings.ToUpper(command.Parameters[0]) == "CAPA" &&
		strings.ToUpper(command.Parameters[1]) == "PSYNC2" {

		writeArrayLength(writer, 0)
		return
	}

	writeError(writer, "ERROR")
}
