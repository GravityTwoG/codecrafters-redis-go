package redis_master

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	protocol "github.com/codecrafters-io/redis-starter-go/pkg/redis/protocol"
	"github.com/codecrafters-io/redis-starter-go/pkg/utils"
)

type _Replica struct {
	conn    net.Conn
	pending bool
	offset  int
	mutex   *sync.Mutex
}

type Master struct {
	mutex             *sync.RWMutex
	replicationId     string
	replicationOffset int

	slavePorts        []string
	connectedReplicas []_Replica
}

func NewMaster() *Master {
	return &Master{
		mutex:             &sync.RWMutex{},
		replicationId:     utils.RandString(40),
		replicationOffset: 0,

		slavePorts:        make([]string, 0),
		connectedReplicas: make([]_Replica, 0),
	}
}

func (m *Master) ReplicationId() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.replicationId
}

func (m *Master) ReplicationOffset() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.replicationOffset
}

func (m *Master) ConnectedReplicasCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.connectedReplicas)
}

func (m *Master) HandleSlave(conn net.Conn, _ *bufio.Reader) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.connectedReplicas = append(m.connectedReplicas, _Replica{
		conn:    conn,
		pending: false,
		offset:  0,
		mutex:   &sync.Mutex{},
	})
}

func (m *Master) SendRDBFile(writer *bufio.Writer) {
	content, err := utils.HexToBin("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	if err != nil {
		fmt.Println("Error converting hex to binary: ", err.Error())
		return
	}

	protocol.WriteBulkStringSpecifier(writer, len(content))
	writer.Write(content)
	writer.Flush()
}

func (m *Master) SendSET(command *protocol.RedisCommand) {
	wg := &sync.WaitGroup{}

	m.mutex.Lock()
	for i := range m.connectedReplicas {
		wg.Add(1)
		go func(currentReplica *_Replica) {
			defer wg.Done()
			replicaAddr := currentReplica.conn.RemoteAddr().String()
			fmt.Printf("Trying to send SET to slave: %s\n", replicaAddr)
			currentReplica.mutex.Lock()
			defer currentReplica.mutex.Unlock()

			countingWriter := &utils.CountingWriter{
				Writer: currentReplica.conn,
				N:      0,
			}
			writer := bufio.NewWriter(countingWriter)

			protocol.WriteBulkStringArray(writer, command.ToStringArray())
			writer.Flush()
			currentReplica.pending = true
			currentReplica.offset += countingWriter.N
			fmt.Printf("Sent SET to slave: %s\n", replicaAddr)
		}(&m.connectedReplicas[i])
	}
	m.mutex.Unlock()

	wg.Wait()
}

func (m *Master) SendGETACKtoReplica(
	replica *_Replica, timeout time.Duration,
) bool {
	countingWriter := &utils.CountingWriter{
		Writer: replica.conn,
		N:      0,
	}
	writer := bufio.NewWriter(countingWriter)
	reader := bufio.NewReader(replica.conn)

	protocol.WriteBulkStringArray(writer, []string{
		protocol.REPLCONF, "GETACK", "*",
	})
	writer.Flush()

	expectedOffset := replica.offset
	replica.offset += countingWriter.N

	replica.conn.SetReadDeadline(time.Now().Add(timeout))

	command := protocol.ParseCommand(reader)
	if command == nil {
		return false
	}
	if command.Name != protocol.REPLCONF || command.Parameters[0] != "ACK" {
		fmt.Println("Error: Expected REPLCONF ACK")
		return false
	}

	actualOffset, err := strconv.Atoi(command.Parameters[1])
	if err != nil {
		fmt.Println("Error converting offset: ", err.Error())
		return false
	}

	if actualOffset != expectedOffset {
		fmt.Printf("Expected offset: %d\n", expectedOffset)
		fmt.Printf("Actual offset: %d\n", actualOffset)
		return false
	}

	replica.pending = false

	return true
}

func (m *Master) SendGETACK(
	replicas int, timeout time.Duration,
) int {
	fmt.Printf("Sending GETACK to slaves with timeout: %s\n", timeout.String())

	acksChan := make(chan bool)
	wg := &sync.WaitGroup{}

	m.mutex.Lock()
	for i := range m.connectedReplicas {
		wg.Add(1)
		go func(currentReplica *_Replica) {
			defer wg.Done()
			currentReplica.mutex.Lock()
			defer currentReplica.mutex.Unlock()

			replicaAddr := currentReplica.conn.RemoteAddr().String()
			if !currentReplica.pending {
				fmt.Printf("Slave not pending: %s\n", replicaAddr)
				acksChan <- true
				return
			}

			fmt.Printf("Sending GETACK to: %s\n", replicaAddr)

			acknowledged := m.SendGETACKtoReplica(currentReplica, timeout)
			if acknowledged {
				fmt.Printf("Slave acknowledged GETACK: %s\n", replicaAddr)
				acksChan <- true
			} else {
				fmt.Printf("Slave not acknowledged GETACK: %s\n", replicaAddr)
			}
		}(&m.connectedReplicas[i])
	}
	m.mutex.Unlock()

	doneChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(acksChan)
		doneChan <- struct{}{}
		close(doneChan)
	}()

	acks := 0
	timeoutChan := time.After(timeout)
	for {
		select {
		case ack := <-acksChan:
			if ack {
				acks++
			}

		case <-doneChan:
			return acks

		case <-timeoutChan:
			fmt.Println("Timeout!")
			return acks
		}
	}
}

func (m *Master) HandleREPLCONF(
	writer *bufio.Writer, command *protocol.RedisCommand,
) {
	if len(command.Parameters) != 2 {
		protocol.WriteError(writer, "ERROR: REPLCONF. Invalid number of parameters")
		return
	}

	if strings.ToUpper(command.Parameters[0]) == "LISTENING-PORT" {
		m.mutex.Lock()
		defer m.mutex.Unlock()

		slavePort := command.Parameters[1]
		m.slavePorts = append(m.slavePorts, slavePort)
		protocol.WriteSimpleString(writer, "OK")
		return
	}

	if strings.ToUpper(command.Parameters[0]) == "CAPA" &&
		strings.ToUpper(command.Parameters[1]) == "PSYNC2" {

		protocol.WriteSimpleString(writer, "OK")
		return
	}

	protocol.WriteError(writer, "ERROR: REPLCONF. Invalid parameters")
}

func (m *Master) HandlePSYNC(
	writer *bufio.Writer, command *protocol.RedisCommand,
) error {
	if len(command.Parameters) != 2 {
		protocol.WriteError(writer, "ERROR: PSYNC. Invalid number of parameters")
		return errors.New("invalid number of parameters")
	}

	protocol.WriteSimpleString(writer, fmt.Sprintf("FULLRESYNC %s %d", m.replicationId, m.replicationOffset))

	m.SendRDBFile(writer)

	return nil
}

func (m *Master) HandleWAIT(writer *bufio.Writer, command *protocol.RedisCommand) {
	if len(command.Parameters) != 2 {
		protocol.WriteError(writer, "ERROR: WAIT. Invalid number of parameters")
		fmt.Printf("Error in command WAIT: invalid len %d\n", len(command.Parameters))
		return
	}

	replicas, err := strconv.Atoi(command.Parameters[0])
	if err != nil {
		protocol.WriteError(writer, "ERROR: WAIT. Invalid number of replicas")
		fmt.Println("Error converting replicas: ", err.Error())
		return
	}
	timeoutMs, err := strconv.Atoi(command.Parameters[1])
	if err != nil {
		protocol.WriteError(writer, "ERROR: WAIT. Invalid timeout")
		fmt.Println("Error converting timeout: ", err.Error())
		return
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	acks := m.SendGETACK(replicas, timeout)
	protocol.WriteInteger(writer, acks)
	fmt.Printf("Received ACKS: %d\n", acks)
}
