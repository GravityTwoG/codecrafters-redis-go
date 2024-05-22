package redis

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
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

func (r *redisServer) masterSendRDBFile(writer *bufio.Writer) {
	content, err := utils.HexToBin("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		fmt.Println("Error converting hex to binary: ", err.Error())
		return
	}

	writeBulkStringSpecifier(writer, len(content))
	writer.Write(content)
	writer.Flush()
}

func (r *redisServer) masterSendSET(command *RedisCommand) {
	wg := &sync.WaitGroup{}

	for i := range r.connectedReplicas {
		wg.Add(1)
		go func(currentReplica *Replica) {
			defer wg.Done()
			replicaAddr := currentReplica.conn.RemoteAddr().String()
			fmt.Printf("Trying to send SET to slave: %s\n", replicaAddr)
			currentReplica.mutex.Lock()
			defer currentReplica.mutex.Unlock()

			writer := bufio.NewWriter(currentReplica.conn)

			writeBulkStringArray(writer, command.ToStringArray())
			writer.Flush()
			currentReplica.pending = true
			fmt.Printf("Sent SET to slave: %s\n", replicaAddr)
		}(&r.connectedReplicas[i])
	}
	wg.Wait()
}

func (r *redisServer) masterSendGETACK(acksChan *chan int) {
	acks := 0
	wg := &sync.WaitGroup{}

	for i, slave := range r.connectedReplicas {
		if !slave.pending {
			fmt.Printf("Slave not pending: %s\n", slave.conn.RemoteAddr().String())
			continue
		}

		wg.Add(1)
		go func(currentReplica *Replica) {
			defer wg.Done()
			currentReplica.mutex.Lock()
			defer currentReplica.mutex.Unlock()

			writer := bufio.NewWriter(currentReplica.conn)
			reader := bufio.NewReader(currentReplica.conn)
			replicaAddr := currentReplica.conn.RemoteAddr().String()

			writeBulkStringArray(writer, []string{"REPLCONF", "GETACK", "*"})
			fmt.Printf("Sending GETACK to slave: %s\n", replicaAddr)
			writer.Flush()

			command := parseCommand(reader)
			if command == nil {
				fmt.Printf("Slave returned nil: %s\n", replicaAddr)
				return
			}
			if command.Name == "REPLCONF" && command.Parameters[0] == "ACK" {
				currentReplica.pending = false
				acks++
				*acksChan <- acks
				fmt.Printf("Slave returned ACK: %s\n", replicaAddr)
				return
			}
			fmt.Printf("Slave returned not ACK: %s\n", replicaAddr)
		}(&r.connectedReplicas[i])
	}
	wg.Wait()
	close(*acksChan)
}

func (r *redisServer) masterHandleSlave(conn net.Conn, _ *bufio.Reader, writer *bufio.Writer) {
	r.masterSendRDBFile(writer)

	r.connectedReplicas = append(r.connectedReplicas, Replica{
		conn:    conn,
		pending: false,
		mutex:   &sync.Mutex{},
	})
}

func (r *redisServer) masterHandleREPLCONF(writer *bufio.Writer, command *RedisCommand) {
	if len(command.Parameters) != 2 {
		writeError(writer, "ERROR: REPLCONF. Invalid number of parameters")
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

	writeError(writer, "ERROR: REPLCONF. Invalid parameters")
}

func (r *redisServer) masterHandlePSYNC(writer *bufio.Writer, command *RedisCommand) {
	if len(command.Parameters) != 2 {
		writeError(writer, "ERROR: PSYNC. Invalid number of parameters")
		return
	}

	writeSimpleString(writer, fmt.Sprintf("FULLRESYNC %s %d", r.replicationId, r.replicationOffset))
}

func (r *redisServer) masterHandleWAIT(writer *bufio.Writer, command *RedisCommand) {
	if len(command.Parameters) != 2 {
		writeError(writer, "ERROR: WAIT. Invalid number of parameters")
		fmt.Printf("Error in command WAIT: invalid len %d\n", len(command.Parameters))
		return
	}

	replicas, err := strconv.Atoi(command.Parameters[0])
	if err != nil {
		writeError(writer, "ERROR: WAIT. Invalid number of replicas")
		fmt.Println("Error converting replicas: ", err.Error())
		return
	}
	timeoutMs, err := strconv.Atoi(command.Parameters[1])
	if err != nil {
		writeError(writer, "ERROR: WAIT. Invalid timeout")
		fmt.Println("Error converting timeout: ", err.Error())
		return
	}

	timeoutTicker := time.NewTicker(time.Duration(timeoutMs) * time.Millisecond)
	// acknowledgements channel
	acksChan := make(chan int, 1)
	acks := 0

	go r.masterSendGETACK(&acksChan)
	isDone := false
	for !isDone {
		select {
		case acks = <-acksChan:
			if acks >= replicas {
				isDone = true
				timeoutTicker.Stop()
				fmt.Printf("Got all required ACKS: %d\n", acks)
			}
		case <-timeoutTicker.C:
			isDone = true
			acks = replicas
			fmt.Printf("Timeout reached. Sending ACKS: %d\n", acks)
		}
	}

	writeInteger(writer, fmt.Sprintf("%d", acks))
	fmt.Printf("Received ACKS: %d\n", acks)
}
