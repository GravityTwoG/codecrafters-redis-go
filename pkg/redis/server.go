package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	kv_store "github.com/codecrafters-io/redis-starter-go/pkg/redis/kv-store"
	protocol "github.com/codecrafters-io/redis-starter-go/pkg/redis/protocol"
	streams_store "github.com/codecrafters-io/redis-starter-go/pkg/redis/streams-store"

	master "github.com/codecrafters-io/redis-starter-go/pkg/redis/master"
	slave "github.com/codecrafters-io/redis-starter-go/pkg/redis/slave"
)

type HandlerFunc func(
	ctx context.Context,
	writer *bufio.Writer,
	command *protocol.RedisCommand,
)

type redisServer struct {
	host string
	port string

	dir        string
	dbfilename string

	ctx    context.Context
	cancel func()
	wg     *sync.WaitGroup
	role   string

	master *master.Master
	slave  *slave.Slave

	kvStore      *kv_store.KVStore
	streamsStore *streams_store.StreamsStore

	handlers map[string]HandlerFunc
}

func NewRedisServer(config *RedisConfig) *redisServer {
	wg := &sync.WaitGroup{}
	kvStore := kv_store.NewKVStore(config.Dir, config.DBFilename)
	streamsStore := streams_store.NewStreamsStore(wg)

	role := "slave"
	var m *master.Master = nil
	var s *slave.Slave = nil
	if config.ReplicaOf == "" {
		role = "master"
		m = master.NewMaster()
	} else {
		s = slave.NewSlave(kvStore, config.Port, config.ReplicaOf)
	}

	ctx, cancel := context.WithCancel(context.Background())

	server := redisServer{
		host: config.Host,
		port: config.Port,

		dir:        config.Dir,
		dbfilename: config.DBFilename,

		ctx:    ctx,
		cancel: cancel,
		wg:     wg,
		role:   role,

		master: m,
		slave:  s,

		kvStore:      kvStore,
		streamsStore: streamsStore,

		handlers: make(map[string]HandlerFunc),
	}

	server.registerCommands()

	return &server
}

func (r *redisServer) registerCommands() {
	r.addHandler(protocol.PING, r.handlePING)
	r.addHandler(protocol.ECHO, r.handleECHO)

	r.addHandler(protocol.SET, r.handleSET)
	r.addHandler(protocol.GET, r.handleGET)
	r.addHandler(protocol.DEL, r.handleDEL)
	r.addHandler(protocol.KEYS, r.handleKEYS)

	r.addHandler(protocol.XADD, r.handleXADD)
	r.addHandler(protocol.XRANGE, r.handleXRANGE)
	r.addHandler(protocol.XREAD, r.handleXREAD)

	r.addHandler(protocol.INFO, r.handleINFO)

	r.addHandler(protocol.REPLCONF, func(
		ctx context.Context,
		writer *bufio.Writer,
		command *protocol.RedisCommand,
	) {
		if r.master == nil {
			protocol.WriteError(writer, "ERROR: REPLCONF is not supported in slave mode")
			return
		}
		r.master.HandleREPLCONF(ctx, writer, command)
	})

	r.addHandler(protocol.WAIT, func(
		ctx context.Context,
		writer *bufio.Writer,
		command *protocol.RedisCommand,
	) {
		if r.master == nil {
			protocol.WriteError(writer, "ERROR: WAIT is not supported in slave mode")
			return
		}
		r.master.HandleWAIT(ctx, writer, command)
	})

	r.addHandler(protocol.CONFIG, r.handleCONFIG)
	r.addHandler(protocol.TYPE, r.handleTYPE)
}

func (r *redisServer) Start() {
	if r.role == "slave" {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.slave.SetupReplication()
		}()
	}

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", r.host, r.port))
	if err != nil {
		fmt.Printf("Failed to bind to port %s: %s\n", r.port, err.Error())
		os.Exit(1)
	}
	defer l.Close()

	isRunning := true
	for isRunning {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		r.wg.Add(1)
		ctx, cancel := context.WithCancel(r.ctx)
		go func(ctx context.Context) {
			defer r.wg.Done()
			defer cancel()
			r.handleConnection(ctx, conn)
		}(ctx)

		select {
		case <-r.ctx.Done():
			isRunning = false
		default:
		}
	}

	r.wg.Wait()
}

func (r *redisServer) Stop() {
	r.cancel()
	if r.slave != nil {
		r.slave.Stop()
	}
	r.wg.Wait()
}

func (r *redisServer) addHandler(name string, handler HandlerFunc) {
	r.handlers[name] = handler
}

func (r *redisServer) handleConnection(ctx context.Context, conn net.Conn) {
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

		if command.Name == protocol.PSYNC && r.master != nil {
			err := r.master.HandlePSYNC(writer, command)
			if err != nil {
				fmt.Println("Error handling PSYNC: ", err.Error())
				return
			}
			slaveConnection = true
			r.master.HandleSlave(conn, reader)
			return
		}

		if handler, ok := r.handlers[command.Name]; ok {
			handler(ctx, writer, command)
		} else {
			protocol.WriteError(writer, fmt.Sprintf("ERROR: Unknown command: %s", command.Name))
		}

		writer.Flush()
	}
}

func (r *redisServer) handlePING(
	ctx context.Context,
	writer *bufio.Writer,
	command *protocol.RedisCommand,
) {
	protocol.WriteSimpleString(writer, "PONG")
}

func (r *redisServer) handleECHO(
	ctx context.Context, writer *bufio.Writer, command *protocol.RedisCommand) {
	if len(command.Parameters) == 0 {
		protocol.WriteError(writer, "ERROR: ECHO requires at least 1 argument")
		return
	}

	protocol.WriteBulkString(writer, command.Parameters[0])
}

// key value

func (r *redisServer) handleSET(
	ctx context.Context,
	writer *bufio.Writer,
	command *protocol.RedisCommand,
) {
	if r.master != nil {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.master.SendSET(command)
		}()
	}

	// SET foo bar
	if len(command.Parameters) == 2 {
		key := command.Parameters[0]
		value := command.Parameters[1]
		r.kvStore.Set(key, value)
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
		r.kvStore.SetWithTTL(key, value, duration)
		protocol.WriteSimpleString(writer, "OK")
		fmt.Printf("key: %s, value: %s, duration: %s\n", key, value, duration)
		return
	}

	protocol.WriteError(writer, "ERROR: SET: Wrong number of arguments")
}

func (r *redisServer) handleGET(
	ctx context.Context,
	writer *bufio.Writer,
	command *protocol.RedisCommand,
) {
	if len(command.Parameters) != 1 {
		protocol.WriteError(writer, "ERROR: Wrong number of arguments")
		fmt.Println("ERROR: GET: Wrong number of arguments")
		return
	}

	key := command.Parameters[0]
	value, ok, err := r.kvStore.Get(key)
	if err != nil {
		if errors.Is(err, kv_store.ErrExpired) {
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

func (r *redisServer) handleDEL(
	ctx context.Context,
	writer *bufio.Writer,
	command *protocol.RedisCommand,
) {
	if len(command.Parameters) < 1 {
		protocol.WriteError(writer, "ERROR: Wrong number of arguments")
		return
	}

	deleted := r.kvStore.Delete(command.Parameters)
	protocol.WriteInteger(writer, deleted)
}

func (r *redisServer) handleKEYS(
	ctx context.Context,
	writer *bufio.Writer,
	command *protocol.RedisCommand,
) {
	if len(command.Parameters) != 1 {
		protocol.WriteError(writer, "ERROR: KEYS. Invalid number of parameters")
		return
	}

	if command.Parameters[0] != "*" && command.Parameters[0] != "\"*\"" {
		protocol.WriteError(writer, "ERROR: KEYS. Not supported")
		return
	}

	protocol.WriteBulkStringArray(writer, r.kvStore.Keys())
}

// Streams

func (r *redisServer) handleXADD(
	ctx context.Context,
	writer *bufio.Writer,
	command *protocol.RedisCommand,

) {
	if len(command.Parameters) < 2 {
		protocol.WriteError(writer, "ERROR: Wrong number of arguments")
		return
	}

	key := command.Parameters[0]
	id := command.Parameters[1]
	values := command.Parameters[2:]
	id, err := r.streamsStore.Append(key, id, values)
	if err != nil {
		protocol.WriteError(writer, err.Error())
		return
	}

	protocol.WriteBulkString(writer, id)
}

func (r *redisServer) handleXRANGE(
	ctx context.Context,

	writer *bufio.Writer,
	command *protocol.RedisCommand,
) {
	if len(command.Parameters) < 3 {
		protocol.WriteError(writer, "ERROR: Wrong number of arguments")
		return
	}

	key := command.Parameters[0]
	start := command.Parameters[1]
	end := command.Parameters[2]

	entries, err := r.streamsStore.Range(key, start, end)
	if err != nil {
		protocol.WriteError(writer, err.Error())
		return
	}

	protocol.WriteArrayLength(writer, len(entries))
	for _, entry := range entries {
		protocol.WriteArrayLength(writer, 2)
		protocol.WriteBulkString(writer, entry.ID.String())
		protocol.WriteArrayLength(writer, len(entry.Values))
		for _, value := range entry.Values {
			protocol.WriteBulkString(writer, value)
		}
	}
}

func (r *redisServer) handleXREAD(
	ctx context.Context,
	writer *bufio.Writer,
	command *protocol.RedisCommand,
) {
	if (len(command.Parameters)-1)%2 != 0 {
		protocol.WriteError(writer, "ERROR: Wrong number of arguments")
		return
	}

	timeoutMs := -1
	if strings.EqualFold(command.Parameters[0], "BLOCK") {
		var err error
		timeoutMs, err = strconv.Atoi(command.Parameters[1])
		if err != nil {
			protocol.WriteError(writer, "ERROR: Invalid timeout")
			return
		}
		command.Parameters = command.Parameters[2:]
	}
	if strings.EqualFold(command.Parameters[0], "STREAMS") {
		command.Parameters = command.Parameters[1:]
	}

	streamsCount := len(command.Parameters) / 2
	// Preprocess start IDs
	for i := 0; i < streamsCount; i++ {
		key := command.Parameters[i]
		command.Parameters[streamsCount+i] = r.streamsStore.ParseStartID(
			key,
			command.Parameters[streamsCount+i],
		)
	}

	time.Sleep(time.Duration(timeoutMs) * time.Millisecond)

	if timeoutMs == 0 {
		err := r.streamsStore.WaitForADD(
			ctx,
			command.Parameters[0],
			command.Parameters[1],
			"+",
		)
		if err != nil {
			protocol.WriteError(writer, err.Error())
			return
		}
	}

	streams := make([]protocol.Stream, 0)

	realStreamsCount := 0
	for i := 0; i < streamsCount; i++ {
		key := command.Parameters[i]
		start := command.Parameters[streamsCount+i]

		entries, err := r.streamsStore.RangeExclusive(key, start, "+")
		if err != nil || len(entries) == 0 {
			continue
		}

		realStreamsCount++
		streams = append(streams, protocol.Stream{
			Key:     key,
			Entries: entries,
		})
	}

	if realStreamsCount == 0 {
		fmt.Printf("No streams\n")
		protocol.WriteNullBulkString(writer)
		return
	}

	protocol.WriteArrayLength(writer, realStreamsCount)
	for _, stream := range streams {
		protocol.WriteStream(writer, &stream)
	}
}

func (r *redisServer) handleINFO(
	ctx context.Context,
	writer *bufio.Writer,
	command *protocol.RedisCommand,
) {
	if len(command.Parameters) != 1 {
		protocol.WriteError(writer, "ERROR: INFO. Invalid number of parameters")
		return
	}

	key := command.Parameters[0]
	if strings.ToUpper(key) != "REPLICATION" {
		protocol.WriteError(writer, "ERROR: INFO. Invalid parameters")
		return
	}

	connectedSlaves := 0
	replicationId := ""
	replicationOffset := 0
	if r.master != nil {
		connectedSlaves = r.master.ConnectedReplicasCount()
		replicationId = r.master.ReplicationId()
		replicationOffset = r.master.ReplicationOffset()
	}

	response := "# Replication\r\n"
	response += fmt.Sprintf("role:%s\r\n", r.role)
	response += fmt.Sprintf("connected_slaves:%d\r\n", connectedSlaves)
	response += fmt.Sprintf("master_replid:%s\r\n", replicationId)
	response += fmt.Sprintf("master_repl_offset:%d\r\n", replicationOffset)
	response += fmt.Sprintf("second_repl_offset:%d\r\n", -1)
	response += fmt.Sprintf("repl_backlog_active:%d\r\n", 0)
	response += fmt.Sprintf("repl_backlog_size:%d\r\n", 1048576)
	response += fmt.Sprintf("repl_backlog_first_byte_offset:%d\r\n", 0)
	response += fmt.Sprintf("repl_backlog_histlen:%d", 0)

	protocol.WriteBulkString(writer, response)
}

func (r *redisServer) handleCONFIG(
	ctx context.Context, writer *bufio.Writer, command *protocol.RedisCommand,
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

func (r *redisServer) handleTYPE(
	ctx context.Context,
	writer *bufio.Writer,
	command *protocol.RedisCommand,
) {
	if len(command.Parameters) != 1 {
		protocol.WriteError(writer, "ERROR: TYPE. Invalid number of parameters")
		return
	}

	key := command.Parameters[0]
	_, ok, err := r.kvStore.Get(key)
	if err == nil && ok {
		protocol.WriteSimpleString(writer, "string")
		return
	}

	_, ok = r.streamsStore.Get(key)
	if ok {
		protocol.WriteSimpleString(writer, "stream")
		return
	}

	protocol.WriteSimpleString(writer, "none")
}
