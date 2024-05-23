package redis_protocol

import (
	"bufio"
	"fmt"
)

func WriteSimpleString(writer *bufio.Writer, str string) {
	writer.Write([]byte(
		fmt.Sprintf("%c%s\r\n", SIMPLE_STRING_SPECIFIER, str),
	))
}

func WriteBulkStringSpecifier(writer *bufio.Writer, length int) {
	writer.Write([]byte(
		fmt.Sprintf("%c%d\r\n", BULK_STRING_SPECIFIER, length),
	))
}

func WriteBulkString(writer *bufio.Writer, str string) {
	WriteBulkStringSpecifier(writer, len(str))
	writer.Write([]byte(str + "\r\n"))
}

func WriteNullBulkString(writer *bufio.Writer) {
	writer.Write([]byte(fmt.Sprintf("%c-1\r\n", BULK_STRING_SPECIFIER)))
}

func writeArrayLength(writer *bufio.Writer, length int) {
	writer.Write([]byte(
		fmt.Sprintf("%c%d\r\n", ARRAY_SPECIFIER, length),
	))
}

func WriteBulkStringArray(writer *bufio.Writer, strs []string) {
	writeArrayLength(writer, len(strs))
	for _, str := range strs {
		WriteBulkString(writer, str)
	}
}

func WriteInteger(writer *bufio.Writer, str string) {
	writer.Write([]byte(fmt.Sprintf("%c%s\r\n", INTEGER_SPECIFIER, str)))
}

func WriteError(writer *bufio.Writer, str string) {
	writer.Write([]byte(fmt.Sprintf("%c%s\r\n", ERROR_SPECIFIER, str)))
}
