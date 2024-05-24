package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	protocol "github.com/codecrafters-io/redis-starter-go/pkg/redis/protocol"
)

type countingReader struct {
	io.Reader
	n int
}

func (w *countingReader) Read(p []byte) (int, error) {
	n, err := w.Reader.Read(p)
	w.n += n
	return n, err
}

func (r *redisServer) slaveSetupReplication() {
	// Connect to master
	conn, err := net.Dial("tcp", r.replicaOf)
	if err != nil {
		fmt.Println("SLAVE: Error connecting to master: ", err.Error())
		return
	}

	countingReader := &countingReader{
		Reader: conn,
		n:      0,
	}
	reader := bufio.NewReader(countingReader)
	writer := bufio.NewWriter(conn)

	err = r.slaveSendPING(reader, writer)
	if err != nil {
		fmt.Println("SLAVE: Error sending PING to master: ", err.Error())
		return
	}

	err = r.slaveSendREPLCONF(reader, writer)
	if err != nil {
		fmt.Println("SLAVE: Error sending REPLCONF to master: ", err.Error())
		return
	}

	err = r.slaveSendPSYNC(reader, writer)
	if err != nil {
		fmt.Println("SLAVE: Error sending PSYNC to master: ", err.Error())
		return
	}

	fmt.Printf("SLAVE: Waiting for RDB FILE from master...\n")

	// Get get RDB FILE from master
	rdbFileLen := protocol.ParseBulkStringLen(reader)
	fmt.Printf("SLAVE: RDB FILE length: %d\n", rdbFileLen)
	reader.Discard(rdbFileLen)

	fmt.Printf("SLAVE: RDB FILE received\n")
	fmt.Printf("SLAVE: connected to master\n")

	r.slaveReplicationOffset = 0
	readBefore := countingReader.n - reader.Buffered()
	// Handle commands from master
	for {
		command := protocol.ParseCommand(reader)
		if command == nil {
			break
		}

		if command.Name == protocol.SET {
			r.slaveHandleSET(command)
		} else if command.Name == protocol.REPLCONF {
			r.slaveHandleGETACK(writer, command)
		} else if command.Name == protocol.PING {
			fmt.Printf("SLAVE: PING from master\n")
		}

		writer.Flush()
		r.slaveReplicationOffset = countingReader.n - reader.Buffered() - readBefore

		fmt.Printf("Slave replication offset: %d\n", r.slaveReplicationOffset)
	}
}

func (r *redisServer) slaveSendPING(reader *bufio.Reader, writer *bufio.Writer) error {
	protocol.WriteBulkStringArray(writer, []string{protocol.PING})
	writer.Flush()

	response := protocol.ParseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if response != "PONG" {
		return errors.New("master response to PING not equal to PONG")
	}

	return nil
}

func (r *redisServer) slaveSendREPLCONF(reader *bufio.Reader, writer *bufio.Writer) error {
	protocol.WriteBulkStringArray(writer, []string{
		protocol.REPLCONF, "listening-port", r.port,
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

func (r *redisServer) slaveSendPSYNC(reader *bufio.Reader, writer *bufio.Writer) error {
	protocol.WriteBulkStringArray(writer, []string{protocol.PSYNC, "?", "-1"})
	writer.Flush()

	response := protocol.ParseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if !strings.Contains(response, "FULLRESYNC") {
		return errors.New("master response to PSYNC not equal to FULLRESYNC")
	}

	return nil
}

func (r *redisServer) slaveHandleSET(command *protocol.RedisCommand) {
	// SET foo bar
	if len(command.Parameters) == 2 {
		key := command.Parameters[0]
		value := command.Parameters[1]
		r.store.Set(key, value)

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
		r.store.SetWithTTL(key, value, duration)
	}
}

func (r *redisServer) slaveHandleGETACK(writer *bufio.Writer, command *protocol.RedisCommand) {
	if len(command.Parameters) != 2 {
		protocol.WriteError(writer, "ERROR: REPLCONF. Invalid number of parameters")
		return
	}

	if strings.ToUpper(command.Parameters[0]) == "GETACK" &&
		command.Parameters[1] == "*" {
		protocol.WriteBulkStringArray(writer, []string{
			protocol.REPLCONF, "ACK", fmt.Sprintf("%d", r.slaveReplicationOffset),
		})
		return
	}

	protocol.WriteError(writer, "ERROR: REPLCONF. Invalid parameters")
}
