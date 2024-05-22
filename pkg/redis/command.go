package redis

import (
	"bufio"
	"fmt"
	"strconv"
)

type RedisCommand struct {
	Name       string
	Parameters []string
}

func (c *RedisCommand) ToStringArray() []string {
	return append([]string{c.Name}, c.Parameters...)
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
	fmt.Printf("Parameters count: %s\n", parametersCountStr)

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

	length, err := strconv.Atoi(bulkStringLen)
	if err != nil {
		fmt.Println("Error parsing bulk string: ", err.Error())
		return -1
	}
	return length
}

func parseBulkString(reader *bufio.Reader) string {
	bulkStringLen := parseBulkStringLen(reader)
	if bulkStringLen == -1 {
		return ""
	}

	str, err := reader.ReadString('\r')
	if err != nil {
		fmt.Println("Error reading bytes: ", err.Error())
		return ""
	}
	str = str[:len(str)-1] // remove '\r'
	reader.Discard(1)      // '\n'
	if bulkStringLen != len(str) {
		fmt.Println("Bulk string length not equal to string length")
		return ""
	}
	fmt.Printf("Bulk string $%d %s\n", bulkStringLen, str)
	return str
}

func parseSimpleString(reader *bufio.Reader) string {
	firstByte, err := reader.ReadByte()
	if err != nil || firstByte != SIMPLE_STRING_SPECIFIER {
		fmt.Println("Error reading byte: ", err.Error())
		return ""
	}

	str, err := reader.ReadString('\r')
	if err != nil {
		fmt.Println("Error reading bytes: ", err.Error())
		return ""
	}

	str = str[:len(str)-1] // remove '\r'
	reader.Discard(1)      // '\n'
	fmt.Printf("Received simple string: %s\n", str)
	return str
}

func parseCommand(reader *bufio.Reader) *RedisCommand {
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
		Name:       command[0],
		Parameters: command[1:],
	}
}
