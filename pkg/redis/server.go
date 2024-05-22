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
	"github.com/codecrafters-io/redis-starter-go/pkg/utils"
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

type Slave struct {
	conn    net.Conn
	pending bool
}

type redisServer struct {
	host string
	port string

	role string

	slavePorts      []string
	connectedSlaves []Slave

	replicaOf              string
	replicationId          string
	replicationOffset      int
	slaveReplicationOffset int

	store *redisstore.RedisStore
}

func NewRedisServer(host string, port string, replicaOf string) *redisServer {

	var role = "master"
	var replicationId = utils.RandString(40)
	if replicaOf != "" {
		role = "slave"
		replicationId = ""
	}

	return &redisServer{
		host: host,
		port: port,

		role: role,

		slavePorts:      make([]string, 0),
		connectedSlaves: make([]Slave, 0),

		replicaOf:              replicaOf,
		replicationId:          replicationId,
		replicationOffset:      0,
		slaveReplicationOffset: 0,

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

	wg := &sync.WaitGroup{}
	if r.role == "slave" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.setupReplication()
		}()
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.handleConnection(conn)
		}()
	}

	wg.Wait()
}

func (r *redisServer) handleConnection(conn net.Conn) {
	slaveConnection := false
	defer func() {
		if !slaveConnection {
			conn.Close()
		}
	}()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		command := parseCommand(reader)
		if command == nil {
			return
		}

		if command.Name == "PSYNC" {
			slaveConnection = true
		}

		r.handleCommand(conn, reader, writer, command)
		writer.Flush()
	}
}

func (r *redisServer) handleCommand(conn net.Conn, reader *bufio.Reader, writer *bufio.Writer, command *RedisCommand) {
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

	if command.Name == "PSYNC" {
		r.handlePSYNC(writer, command)
		r.handleSlave(conn, reader, writer)
		return
	}

	if command.Name == "WAIT" {
		r.handleWAIT(writer, command)
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

	r.sendSETtoSlaves(command)
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
			response += fmt.Sprintf("connected_slaves:%d\r\n", len(r.connectedSlaves))
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
