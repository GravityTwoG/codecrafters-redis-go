package redis

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
)

const SIMPLE_STRING_SPECIFIER = '+'
const ERROR_SPECIFIER = '-'
const INTEGER_SPECIFIER = ':'
const BULK_STRING_SPECIFIER = '$'
const ARRAY_SPECIFIER = '*'

type redisServer struct {
	host string
	port string
}

func NewRedisServer(host string, port string) *redisServer {

	return &redisServer{
		host: host,
		port: port,
	}
}

func (r *redisServer) Start() {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", r.host, r.port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	wg := &sync.WaitGroup{}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			break
		}
		wg.Add(1)
		go func() {
			r.handleConnection(conn)
			defer conn.Close()
			defer wg.Done()
		}()
	}

	wg.Wait()
}

func (r *redisServer) handleConnection(conn net.Conn) {

	reader := bufio.NewReader(conn)

	firstByte, err := reader.ReadByte()
	if err != nil || firstByte != ARRAY_SPECIFIER {
		fmt.Println("Error reading byte: ", err.Error())
		return
	}

	stringsCountString, err := reader.ReadBytes('\r')
	if err != nil {
		fmt.Println("Error reading bytes: ", err.Error())
		return
	}
	stringsCountString = stringsCountString[:len(stringsCountString)-1] // remove '\r'
	reader.Discard(1)                                                   // '\n'
	fmt.Printf("Strings count: %s\n", stringsCountString)

	firstByte, err = reader.ReadByte()
	if err != nil || firstByte != BULK_STRING_SPECIFIER {
		fmt.Println("Error reading byte: ", err.Error())
		return
	}
	bulkStringLen, err := reader.ReadBytes('\r')
	if err != nil {
		fmt.Println("Error reading bytes: ", err.Error())
		return
	}
	bulkStringLen = bulkStringLen[:len(bulkStringLen)-1] // remove '\r'
	reader.Discard(1)                                    // '\n'
	fmt.Printf("Bulk string length: %s\n", bulkStringLen)

	command, err := reader.ReadString('\r')
	if err != nil {
		fmt.Println("Error reading bytes: ", err.Error())
		return
	}
	command = command[:len(command)-1] // remove '\r'
	reader.Discard(1)                  // '\n'
	fmt.Println("Received command: ", command)

	if command == "PING" {
		conn.Write([]byte("+PONG\r\n"))
	}
}
