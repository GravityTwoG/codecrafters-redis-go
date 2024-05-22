package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
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