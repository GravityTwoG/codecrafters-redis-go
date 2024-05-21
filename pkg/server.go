package redis

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
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
	writer := bufio.NewWriter(conn)

	for {
		r.handleCommand(reader, writer)
		writer.Flush()
	}
}

func parseParametersCount(reader *bufio.Reader) int {
	firstByte, err := reader.ReadByte()
	if err != nil || firstByte != ARRAY_SPECIFIER {
		fmt.Println("Error reading byte: ", err.Error())
		return -1
	}
	parametersCountStr, err := reader.ReadString('\r')
	if err != nil {
		fmt.Println("Error reading bytes: ", err.Error())
		return -1
	}
	parametersCountStr = parametersCountStr[:len(parametersCountStr)-1] // remove '\r'
	reader.Discard(1)                                                   // '\n'
	fmt.Printf("Strings count: %s\n", parametersCountStr)

	count, err := strconv.Atoi(parametersCountStr)
	if err != nil {
		fmt.Println("Error parsing string: ", err.Error())
		return -1
	}
	return count
}

func parseBulkStringLen(reader *bufio.Reader) int {
	firstByte, err := reader.ReadByte()
	if err != nil || firstByte != BULK_STRING_SPECIFIER {
		fmt.Println("Error reading byte: ", err.Error())
		return -1
	}
	bulkStringLen, err := reader.ReadString('\r')
	if err != nil {
		fmt.Println("Error reading bytes: ", err.Error())
		return -1
	}
	bulkStringLen = bulkStringLen[:len(bulkStringLen)-1] // remove '\r'
	reader.Discard(1)                                    // '\n'
	fmt.Printf("Bulk string length: %s\n", bulkStringLen)

	length, err := strconv.Atoi(bulkStringLen)
	if err != nil {
		fmt.Println("Error parsing bulk string: ", err.Error())
		return -1
	}
	return length
}

func parseCommand(reader *bufio.Reader) []string {
	var command []string
	parametersCount := parseParametersCount(reader)
	if parametersCount == -1 {
		return nil
	}

	for i := 0; i < parametersCount; i++ {
		bulkStringLen := parseBulkStringLen(reader)
		if bulkStringLen == -1 {
			return nil
		}

		str, err := reader.ReadString('\r')
		if err != nil {
			fmt.Println("Error reading bytes: ", err.Error())
			return nil
		}
		str = str[:len(str)-1] // remove '\r'
		reader.Discard(1)      // '\n'
		fmt.Println("Received string: ", str)

		command = append(command, str)
	}

	return command
}

func writeSimpleString(writer *bufio.Writer, str string) {
	writer.Write([]byte{'+'})
	writer.Write([]byte(str + "\r\n"))
}

func writeBulkString(writer *bufio.Writer, str string) {
	writer.Write([]byte{'$'})
	writer.Write([]byte(strconv.Itoa(len(str)) + "\r\n"))
	writer.Write([]byte(str + "\r\n"))
}

func (r *redisServer) handleCommand(reader *bufio.Reader, writer *bufio.Writer) {
	command := parseCommand(reader)
	if len(command) == 0 {
		return
	}
	commandName := strings.ToUpper(command[0])

	if commandName == "PING" {
		writeSimpleString(writer, "PONG")
	} else if commandName == "ECHO" {
		writeBulkString(writer, command[1])
	}
}
