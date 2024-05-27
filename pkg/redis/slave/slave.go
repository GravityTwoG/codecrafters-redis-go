package redis_slave

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	protocol "github.com/codecrafters-io/redis-starter-go/pkg/redis/protocol"
	redisstore "github.com/codecrafters-io/redis-starter-go/pkg/redis/store"
	"github.com/codecrafters-io/redis-starter-go/pkg/utils"
)

type Slave struct {
	port              string
	replicaOf         string
	replicationOffset int

	store *redisstore.RedisStore
}

func NewSlave(store *redisstore.RedisStore, port string, replicaOf string) *Slave {
	return &Slave{
		port:              port,
		replicaOf:         replicaOf,
		replicationOffset: 0,

		store: store,
	}
}

func (s *Slave) SetupReplication() {
	// Connect to master
	conn, err := net.Dial("tcp", s.replicaOf)
	if err != nil {
		fmt.Println("SLAVE: Error connecting to master: ", err.Error())
		return
	}

	countingReader := &utils.CountingReader{
		Reader: conn,
		N:      0,
	}
	reader := bufio.NewReader(countingReader)
	writer := bufio.NewWriter(conn)

	err = s.sendPING(reader, writer)
	if err != nil {
		fmt.Println("SLAVE: Error sending PING to master: ", err.Error())
		return
	}

	err = s.sendREPLCONF(reader, writer)
	if err != nil {
		fmt.Println("SLAVE: Error sending REPLCONF to master: ", err.Error())
		return
	}

	err = s.sendPSYNC(reader, writer)
	if err != nil {
		fmt.Println("SLAVE: Error sending PSYNC to master: ", err.Error())
		return
	}

	fmt.Printf("SLAVE: Waiting for RDB FILE from master...\n")

	// Get get RDB FILE from master
	rdbFileLen := protocol.ParseBulkStringLen(reader)
	fmt.Printf("SLAVE: RDB FILE length: %d\n", rdbFileLen)
	reader.Discard(rdbFileLen)

	fmt.Printf("SLAVE: RDB FILE received, waiting for commands from master\n")

	s.replicationOffset = 0
	readBefore := countingReader.N - reader.Buffered()
	// Handle commands from master
	for {
		command := protocol.ParseCommand(reader)
		if command == nil {
			break
		}

		if command.Name == protocol.SET {
			s.handleSET(command)
		} else if command.Name == protocol.REPLCONF {
			s.handleGETACK(writer, command)
		} else if command.Name == protocol.PING {
			fmt.Printf("SLAVE: PING from master\n")
		}

		writer.Flush()
		s.replicationOffset = countingReader.N - reader.Buffered() - readBefore

		fmt.Printf("Slave replication offset: %d\n", s.replicationOffset)
	}
}

func (s *Slave) sendPING(reader *bufio.Reader, writer *bufio.Writer) error {
	protocol.WriteBulkStringArray(writer, []string{protocol.PING})
	writer.Flush()

	response := protocol.ParseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if response != "PONG" {
		return errors.New("master response to PING not equal to PONG")
	}

	return nil
}

func (s *Slave) sendREPLCONF(reader *bufio.Reader, writer *bufio.Writer) error {
	protocol.WriteBulkStringArray(writer, []string{
		protocol.REPLCONF, "listening-port", s.port,
	})
	writer.Flush()

	response := protocol.ParseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if response != "OK" {
		return errors.New("master response to REPLCONF not equal to OK")
	}

	protocol.WriteBulkStringArray(writer, []string{
		protocol.REPLCONF, "capa", "psync2",
	})
	writer.Flush()

	response = protocol.ParseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if response != "OK" {
		return errors.New("master response to REPLCONF not equal to OK")
	}

	return nil
}

func (s *Slave) sendPSYNC(reader *bufio.Reader, writer *bufio.Writer) error {
	protocol.WriteBulkStringArray(writer, []string{protocol.PSYNC, "?", "-1"})
	writer.Flush()

	response := protocol.ParseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if !strings.Contains(response, "FULLRESYNC") {
		return errors.New("master response to PSYNC not equal to FULLRESYNC")
	}

	return nil
}

func (s *Slave) handleSET(command *protocol.RedisCommand) {
	// SET foo bar
	if len(command.Parameters) == 2 {
		key := command.Parameters[0]
		value := command.Parameters[1]
		s.store.Set(key, value)

		// SET foo bar PX 10000
	} else if len(command.Parameters) == 4 &&
		strings.ToUpper(command.Parameters[2]) == "PX" {

		key := command.Parameters[0]
		value := command.Parameters[1]
		durationMs, err := strconv.Atoi(command.Parameters[3])
		if err != nil {
			fmt.Println("SLAVE: Error converting duration: ", err.Error())
			return
		}

		duration := time.Duration(durationMs) * time.Millisecond
		s.store.SetWithTTL(key, value, duration)
	}
}

func (s *Slave) handleGETACK(writer *bufio.Writer, command *protocol.RedisCommand) {
	if len(command.Parameters) != 2 {
		protocol.WriteError(writer, "ERROR: REPLCONF. Invalid number of parameters")
		return
	}

	if strings.ToUpper(command.Parameters[0]) == "GETACK" &&
		command.Parameters[1] == "*" {
		protocol.WriteBulkStringArray(writer, []string{
			protocol.REPLCONF, "ACK", fmt.Sprintf("%d", s.replicationOffset),
		})
		return
	}

	protocol.WriteError(writer, "ERROR: REPLCONF. Invalid parameters")
}
