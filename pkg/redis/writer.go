package redis

import (
	"bufio"
	"strconv"
)

func writeSimpleString(writer *bufio.Writer, str string) {
	writer.Write([]byte{SIMPLE_STRING_SPECIFIER})
	writer.Write([]byte(str + "\r\n"))
}

func writeBulkStringSpecifier(writer *bufio.Writer, length int) {
	writer.Write([]byte{BULK_STRING_SPECIFIER})
	writer.Write([]byte(strconv.Itoa(length) + "\r\n"))
}

func writeBulkString(writer *bufio.Writer, str string) {
	writeBulkStringSpecifier(writer, len(str))
	writer.Write([]byte(str + "\r\n"))
}

func writeNullBulkString(writer *bufio.Writer) {
	writer.Write([]byte{BULK_STRING_SPECIFIER})
	writer.Write([]byte("-1\r\n"))
}

func writeBulkStringArray(writer *bufio.Writer, strs []string) {
	writeArrayLength(writer, len(strs))
	for _, str := range strs {
		writeBulkString(writer, str)
	}
}

func writeInteger(writer *bufio.Writer, str string) {
	writer.Write([]byte{INTEGER_SPECIFIER})
	writer.Write([]byte(str + "\r\n"))
}

func writeError(writer *bufio.Writer, str string) {
	writer.Write([]byte{ERROR_SPECIFIER})
	writer.Write([]byte(str + "\r\n"))
}

func writeArrayLength(writer *bufio.Writer, length int) {
	writer.Write([]byte{ARRAY_SPECIFIER})
	writer.Write([]byte(strconv.Itoa(length) + "\r\n"))
}
