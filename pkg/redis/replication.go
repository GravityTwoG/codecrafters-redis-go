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

	"github.com/codecrafters-io/redis-starter-go/pkg/utils"
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

func (r *redisServer) setupReplication() {
	// Connect to master
	conn, err := net.Dial("tcp", r.replicaOf)
	if err != nil {
		fmt.Println("Error connecting to master: ", err.Error())
		return
	}

	countingReader := &countingReader{
		Reader: conn,
		n:      0,
	}
	reader := bufio.NewReader(countingReader)
	writer := bufio.NewWriter(conn)

	err = r.sendPINGtoMaster(reader, writer)
	if err != nil {
		fmt.Println("Error sending PING to master: ", err.Error())
		return
	}

	err = r.sendREPLCONFtoMaster(reader, writer)
	if err != nil {
		fmt.Println("Error sending REPLCONF to master: ", err.Error())
		return
	}

	err = r.sendPSYNCtoMaster(reader, writer)
	if err != nil {
		fmt.Println("Error sending PSYNC to master: ", err.Error())
		return
	}

	fmt.Printf("Waiting for RDB FILE from master...\n")

	// Get get RDB FILE from master
	rdbFileLen := parseBulkStringLen(reader)
	fmt.Printf("RDB FILE length: %d\n", rdbFileLen)
	reader.Discard(rdbFileLen)

	fmt.Printf("RDB FILE received\n")

	// Handle commands from master
	for {
		countingReader.n = 0
		command := parseCommand(reader)
		if command == nil {
			break
		}

		if command.Name == "SET" {
			r.handleSETfromMaster(writer, command)
		} else if command.Name == "REPLCONF" {
			r.handleREPLCONFfromMaster(writer, command)
		}

		writer.Flush()
		r.slaveReplicationOffset += countingReader.n
	}
}

func (r *redisServer) sendPINGtoMaster(reader *bufio.Reader, writer *bufio.Writer) error {
	writeArrayLength(writer, 1)
	writeBulkString(writer, "PING")
	writer.Flush()

	response := parseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if response != "PONG" {
		return errors.New("master response to PING not equal to PONG")
	}

	return nil
}

func (r *redisServer) sendREPLCONFtoMaster(reader *bufio.Reader, writer *bufio.Writer) error {
	writeBulkStringArray(writer, []string{"REPLCONF", "listening-port", r.port})
	writer.Flush()

	response := parseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if response != "OK" {
		return errors.New("master response to REPLCONF not equal to OK")
	}

	writeBulkStringArray(writer, []string{"REPLCONF", "capa", "psync2"})
	writer.Flush()

	response = parseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if response != "OK" {
		return errors.New("master response to REPLCONF not equal to OK")
	}

	return nil
}

func (r *redisServer) sendPSYNCtoMaster(reader *bufio.Reader, writer *bufio.Writer) error {
	writeBulkStringArray(writer, []string{"PSYNC", "?", "-1"})
	writer.Flush()

	response := parseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if !strings.Contains(response, "FULLRESYNC") {
		return errors.New("master response to PSYNC not equal to FULLRESYNC")
	}

	return nil
}

func (r *redisServer) sendRDBFileToSlave(writer *bufio.Writer) {
	content, err := utils.HexToBin("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		fmt.Println("Error converting hex to binary: ", err.Error())
		return
	}

	writeBulkStringSpecifier(writer, len(content))
	writer.Write(content)
	writer.Flush()
}

func (r *redisServer) sendSETtoSlaves(command *RedisCommand) {
	for _, slave := range r.connectedSlaves {
		slaveWriter := bufio.NewWriter(slave)

		strs := make([]string, 1+len(command.Parameters))
		strs[0] = command.Name
		copy(strs[1:], command.Parameters)

		writeBulkStringArray(slaveWriter, strs)
		slaveWriter.Flush()
	}
}

func (r *redisServer) handleSlave(conn net.Conn, reader *bufio.Reader, writer *bufio.Writer) {
	r.connectedSlaves = append(r.connectedSlaves, conn)
}

// From slave to master
func (r *redisServer) handleREPLCONF(writer *bufio.Writer, command *RedisCommand) {
	if len(command.Parameters) != 2 {
		writeError(writer, "ERROR")
		return
	}

	if strings.ToUpper(command.Parameters[0]) == "LISTENING-PORT" {
		slavePort := command.Parameters[1]
		r.slavePorts = append(r.slavePorts, slavePort)
		writeSimpleString(writer, "OK")
		return
	}

	if strings.ToUpper(command.Parameters[0]) == "CAPA" &&
		strings.ToUpper(command.Parameters[1]) == "PSYNC2" {

		writeSimpleString(writer, "OK")
		return
	}

	writeError(writer, "ERROR")
}

// From slave to master
func (r *redisServer) handlePSYNC(writer *bufio.Writer, command *RedisCommand) {
	if len(command.Parameters) != 2 {
		writeError(writer, "ERROR")
		return
	}

	writeSimpleString(writer, fmt.Sprintf("FULLRESYNC %s %d", r.replicationId, r.replicationOffset))

	r.sendRDBFileToSlave(writer)
}

func (r *redisServer) handleSETfromMaster(writer *bufio.Writer, command *RedisCommand) {
	// SET foo bar
	if len(command.Parameters) == 2 {
		key := command.Parameters[0]
		value := command.Parameters[1]
		r.store.Set(key, value)
		fmt.Printf("SLAVE: key: %s, value: %s\n", key, value)

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
		fmt.Printf("SLAVE: key: %s, value: %s, duration: %s\n", key, value, duration)
	}
}

func (r *redisServer) handleREPLCONFfromMaster(writer *bufio.Writer, command *RedisCommand) {
	if len(command.Parameters) != 2 {
		writeError(writer, "ERROR")
		return
	}

	if strings.ToUpper((command.Parameters[0])) == "GETACK" &&
		command.Parameters[1] == "*" {
		writeBulkStringArray(writer, []string{"REPLCONF", "ACK", fmt.Sprintf("%d", r.slaveReplicationOffset)})
		return
	}

	writeError(writer, "ERROR")
}
