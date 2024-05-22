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

func (r *redisServer) masterSendGETACKtoReplica(
	replica *Replica, doneChan *chan bool,
) {
	writer := bufio.NewWriter(replica.conn)
	reader := bufio.NewReader(replica.conn)
	replicaAddr := replica.conn.RemoteAddr().String()

	writeBulkStringArray(writer, []string{"REPLCONF", "GETACK", "*"})
	fmt.Printf("Sending GETACK to slave: %s\n", replicaAddr)
	writer.Flush()

	command := parseCommand(reader)
	if command == nil {
		fmt.Printf("Slave returned nil: %s\n", replicaAddr)
		*doneChan <- false
		return
	}
	if command.Name == "REPLCONF" && command.Parameters[0] == "ACK" {
		fmt.Printf("Slave returned ACK: %s\n", replicaAddr)
		replica.pending = false
		*doneChan <- true
		return
	}
	fmt.Printf("Slave returned not ACK: %s\n", replicaAddr)
	*doneChan <- false
}

func (r *redisServer) masterSendGETACK(
	replicas int, timeout time.Duration,
) int {
	acksChan := make(chan int)

	fmt.Printf("Sending GETACK to slaves with timeout: %s\n", timeout.String())

	for i, replica := range r.connectedReplicas {
		if !replica.pending {
			fmt.Printf("Slave not pending: %s\n", replica.conn.RemoteAddr().String())
			continue
		}

		go func(currentReplica *Replica) {
			currentReplica.mutex.Lock()
			defer currentReplica.mutex.Unlock()

			doneChan := make(chan bool)
			go r.masterSendGETACKtoReplica(currentReplica, &doneChan)

			timeoutTicker := time.NewTicker(timeout)
			select {
			case acknowledged := <-doneChan:
				if acknowledged {
					acksChan <- 1
				}
			case <-timeoutTicker.C:
				fmt.Printf("Timeout sending GETACK to slave: %s\n", currentReplica.conn.RemoteAddr().String())
			}
			timeoutTicker.Stop()
		}(&r.connectedReplicas[i])
	}

	acks := 0
	timeoutTicker := time.NewTicker(timeout)
	for {
		select {
		case <-acksChan:
			acks++
			if acks == replicas {
				timeoutTicker.Stop()
				return acks
			}
		case <-timeoutTicker.C:
			return acks
		}
	}
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

	acks := r.masterSendGETACK(replicas, time.Duration(timeoutMs)*time.Millisecond)
	writeInteger(writer, fmt.Sprintf("%d", acks))
	fmt.Printf("Received ACKS: %d\n", acks)
}
