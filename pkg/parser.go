package redis

import (
	"bufio"
	"fmt"
	"strconv"
)

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

func parseCommand(reader *bufio.Reader) *RedisCommand {
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

	return &RedisCommand{
		Name:       command[0],
		Parameters: command[1:],
	}
}
