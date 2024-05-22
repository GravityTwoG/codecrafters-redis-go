package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/pkg/utils"
)

func (r *redisServer) handleReplication() {
	conn, err := net.Dial("tcp", r.replicaOf)
	if err != nil {
		fmt.Println("Error connecting to master: ", err.Error())
		return
	}

	reader := bufio.NewReader(conn)
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
	writeArrayLength(writer, 3)
	writeBulkString(writer, "REPLCONF")
	writeBulkString(writer, "listening-port")
	writeBulkString(writer, r.port)
	writer.Flush()

	response := parseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if response != "OK" {
		return errors.New("master response to REPLCONF not equal to OK")
	}

	writeArrayLength(writer, 3)
	writeBulkString(writer, "REPLCONF")
	writeBulkString(writer, "capa")
	writeBulkString(writer, "psync2")
	writer.Flush()

	response = parseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if response != "OK" {
		return errors.New("master response to REPLCONF not equal to OK")
	}

	return nil
}

func (r *redisServer) sendPSYNCtoMaster(reader *bufio.Reader, writer *bufio.Writer) error {
	writeArrayLength(writer, 3)
	writeBulkString(writer, "PSYNC")
	writeBulkString(writer, "?")
	writeBulkString(writer, "-1")
	writer.Flush()

	response := parseSimpleString(reader)
	fmt.Printf("Master response: %s\n", response)
	if !strings.Contains(response, "FULLRESYNC") {
		return errors.New("master response to PSYNC not equal to FULLRESYNC")
	}

	return nil
}

func (r *redisServer) sendRDBFile(writer *bufio.Writer) {
	content, err := utils.HexToBin("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		fmt.Println("Error converting hex to binary: ", err.Error())
		return
	}

	writeBulkStringSpecifier(writer, len(content))
	writer.Write([]byte(content))
	writer.Flush()
}

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

func (r *redisServer) handlePSYNC(writer *bufio.Writer, command *RedisCommand) {
	if len(command.Parameters) != 2 {
		writeError(writer, "ERROR")
		return
	}

	writeSimpleString(writer, fmt.Sprintf("FULLRESYNC %s %d", r.replicationId, r.replicationOffset))

	r.sendRDBFile(writer)
}
