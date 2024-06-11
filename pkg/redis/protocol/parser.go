package redis_protocol

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

const simpleStringSpecifier = '+'
const errorSpecifier = '-'
const integerSpecifier = ':'
const buldStringSpecifier = '$'
const arraySpecifier = '*'

func parseParametersCount(reader *bufio.Reader) int {
	firstByte, err := reader.ReadByte()
	if err != nil || firstByte != arraySpecifier {
		fmt.Println("parseParametersCount: Error reading byte: ", err.Error())
		return -1
	}
	parametersCountStr, err := reader.ReadString('\r')
	if err != nil {
		fmt.Println("parseParametersCount: Error reading bytes: ", err.Error())
		return -1
	}
	parametersCountStr = parametersCountStr[:len(parametersCountStr)-1] // remove '\r'
	reader.Discard(1)                                                   // '\n'
	fmt.Printf("Parameters count: %s\n", parametersCountStr)

	count, err := strconv.Atoi(parametersCountStr)
	if err != nil {
		fmt.Println("parseParametersCount: Error parsing string: ", err.Error())
		return -1
	}
	return count
}

func ParseBulkStringLen(reader *bufio.Reader) int {
	firstByte, err := reader.ReadByte()
	if err != nil || firstByte != buldStringSpecifier {
		fmt.Println("parseBulkStringLen: Error reading byte: ", err.Error())
		return -1
	}
	bulkStringLen, err := reader.ReadString('\r')
	if err != nil {
		fmt.Println("parseBulkStringLen: Error reading bytes: ", err.Error())
		return -1
	}
	bulkStringLen = bulkStringLen[:len(bulkStringLen)-1] // remove '\r'
	reader.Discard(1)                                    // '\n'

	length, err := strconv.Atoi(bulkStringLen)
	if err != nil {
		fmt.Println("parseBulkStringLen: Error parsing bulk string: ", err.Error())
		return -1
	}
	return length
}

func parseBulkString(reader *bufio.Reader) string {
	bulkStringLen := ParseBulkStringLen(reader)
	if bulkStringLen == -1 {
		return ""
	}

	str, err := reader.ReadString('\r')
	if err != nil {
		fmt.Println("parseBulkString: Error reading bytes: ", err.Error())
		return ""
	}
	str = str[:len(str)-1] // remove '\r'
	reader.Discard(1)      // '\n'
	if bulkStringLen != len(str) {
		fmt.Println("parseBulkString: Bulk string length not equal to string length")
		return ""
	}
	fmt.Printf("Bulk string $%d %s\n", bulkStringLen, str)
	return str
}

func ParseSimpleString(reader *bufio.Reader) string {
	firstByte, err := reader.ReadByte()
	if err != nil || firstByte != simpleStringSpecifier {
		fmt.Println("parseSimpleString: Error reading byte: ", err.Error())
		return ""
	}

	str, err := reader.ReadString('\r')
	if err != nil {
		fmt.Println("parseSimpleString: Error reading bytes: ", err.Error())
		return ""
	}

	str = str[:len(str)-1] // remove '\r'
	reader.Discard(1)      // '\n'
	fmt.Printf("Received simple string: %s\n", str)
	return str
}

func ParseCommand(reader *bufio.Reader) *RedisCommand {
	parametersCount := parseParametersCount(reader)
	if parametersCount == -1 {
		return nil
	}

	command := make([]string, 0, parametersCount)
	for i := 0; i < parametersCount; i++ {
		str := parseBulkString(reader)
		if str == "" {
			return nil
		}

		command = append(command, str)
	}
	fmt.Printf("Command parsed\n")

	return &RedisCommand{
		Name:       strings.ToUpper(command[0]),
		Parameters: command[1:],
	}
}
