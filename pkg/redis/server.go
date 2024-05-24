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

	protocol "github.com/codecrafters-io/redis-starter-go/pkg/redis/protocol"
	redisstore "github.com/codecrafters-io/redis-starter-go/pkg/redis/store"
	"github.com/codecrafters-io/redis-starter-go/pkg/utils"
)

type Replica struct {
	conn    net.Conn
	pending bool
	mutex   *sync.Mutex
}

type redisServer struct {
	host string
	port string

	dir        string
	dbfilename string

	role string

	slavePorts        []string
	connectedReplicas []Replica
	mutex             *sync.Mutex

	replicaOf              string
	replicationId          string
	replicationOffset      int
	slaveReplicationOffset int

	store *redisstore.RedisStore
}

func NewRedisServer(config *RedisConfig) *redisServer {

	var role = "master"
	var replicationId = utils.RandString(40)
	if config.ReplicaOf != "" {
		role = "slave"
		replicationId = ""
	}

	return &redisServer{
		host: config.Host,
		port: config.Port,

		dir:        config.Dir,
		dbfilename: config.DBFilename,

		role: role,

		slavePorts:        make([]string, 0),
		connectedReplicas: make([]Replica, 0),
		mutex:             &sync.Mutex{},

		replicaOf:              config.ReplicaOf,
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
			r.slaveSetupReplication()
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
		command := protocol.ParseCommand(reader)
		if command == nil {
			return
		}

		if command.Name == protocol.PSYNC {
			slaveConnection = true
			r.masterHandlePSYNC(writer, command)
			r.masterHandleSlave(conn, reader)
			return
		}

		r.handleCommand(writer, command)
		writer.Flush()
	}
}

func (r *redisServer) handleCommand(writer *bufio.Writer, command *protocol.RedisCommand) {
	switch command.Name {
	case protocol.PING:
		protocol.WriteSimpleString(writer, "PONG")

	case protocol.ECHO:
		protocol.WriteBulkString(writer, command.Parameters[0])

	case protocol.SET:
		r.handleSET(writer, command)

	case protocol.GET:
		r.handleGET(writer, command)

	case protocol.INFO:
		r.handleINFO(writer, command)

	case protocol.REPLCONF:
		r.masterHandleREPLCONF(writer, command)

	case protocol.WAIT:
		r.masterHandleWAIT(writer, command)

	case protocol.CONFIG:
		r.handleCONFIG(writer, command)

	default:
		protocol.WriteError(writer, "ERROR: Unknown command")
	}
}

func (r *redisServer) handleSET(writer *bufio.Writer, command *protocol.RedisCommand) {
	go r.masterSendSET(command)

	// SET foo bar
	if len(command.Parameters) == 2 {
		key := command.Parameters[0]
		value := command.Parameters[1]
		r.store.Set(key, value)
		protocol.WriteSimpleString(writer, "OK")
		fmt.Printf("key: %s, value: %s\n", key, value)
		return
	}

	// SET foo bar PX 10000
	if len(command.Parameters) == 4 &&
		strings.ToUpper(command.Parameters[2]) == "PX" {

		key := command.Parameters[0]
		value := command.Parameters[1]
		durationMs, err := strconv.Atoi(command.Parameters[3])
		if err != nil {
			protocol.WriteError(writer, "ERROR: Invalid duration")
			return
		}

		duration := time.Duration(durationMs) * time.Millisecond
		r.store.SetWithTTL(key, value, duration)
		protocol.WriteSimpleString(writer, "OK")
		fmt.Printf("key: %s, value: %s, duration: %s\n", key, value, duration)
		return
	}

	protocol.WriteError(writer, "ERROR: SET: Wrong number of arguments")
}

func (r *redisServer) handleGET(writer *bufio.Writer, command *protocol.RedisCommand) {
	if len(command.Parameters) != 1 {
		protocol.WriteError(writer, "ERROR: Wrong number of arguments")
		fmt.Println("ERROR: GET: Wrong number of arguments")
		return
	}

	key := command.Parameters[0]
	value, ok, err := r.store.Get(key)
	if err != nil {
		if err.Error() == "EXPIRED" {
			protocol.WriteNullBulkString(writer)
		} else {
			protocol.WriteError(writer, err.Error())
		}
		return
	}

	if !ok {
		protocol.WriteError(writer, "ERROR: Key not found")
		return
	}

	protocol.WriteBulkString(writer, value)
}

func (r *redisServer) handleINFO(writer *bufio.Writer, command *protocol.RedisCommand) {
	if len(command.Parameters) != 1 {
		protocol.WriteError(writer, "ERROR: INFO. Invalid number of parameters")
		return
	}

	key := command.Parameters[0]
	if strings.ToUpper(key) != "REPLICATION" {
		protocol.WriteError(writer, "ERROR: INFO. Invalid parameters")
		return
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	response := "# Replication\r\n"
	response += fmt.Sprintf("role:%s\r\n", r.role)
	response += fmt.Sprintf("connected_slaves:%d\r\n", len(r.connectedReplicas))
	response += fmt.Sprintf("master_replid:%s\r\n", r.replicationId)
	response += fmt.Sprintf("master_repl_offset:%d\r\n", r.replicationOffset)
	response += fmt.Sprintf("second_repl_offset:%d\r\n", -1)
	response += fmt.Sprintf("repl_backlog_active:%d\r\n", 0)
	response += fmt.Sprintf("repl_backlog_size:%d\r\n", 1048576)
	response += fmt.Sprintf("repl_backlog_first_byte_offset:%d\r\n", 0)
	response += fmt.Sprintf("repl_backlog_histlen:%d", 0)

	protocol.WriteBulkString(writer, response)
}

func (r *redisServer) handleCONFIG(
	writer *bufio.Writer, command *protocol.RedisCommand,
) {
	if len(command.Parameters) != 2 {
		protocol.WriteError(writer, "ERROR: CONFIG. Invalid number of parameters")
		return
	}

	if strings.ToUpper(command.Parameters[0]) != "GET" {
		protocol.WriteError(
			writer,
			fmt.Sprintf("ERROR: CONFIG. Invalid parameter %s", command.Parameters[0]),
		)
		return
	}

	key := command.Parameters[1]

	if key == "dir" {
		protocol.WriteBulkStringArray(writer, []string{"dir", r.dir})
		return
	}

	if key == "dbfilename" {
		protocol.WriteBulkStringArray(writer, []string{"dbfilename", r.dbfilename})
		return
	}

	protocol.WriteError(writer, "ERROR: CONFIG. Invalid key")
}
