package redis

import (
	"bufio"
	"strconv"
)

func writeSimpleString(writer *bufio.Writer, str string) {
	writer.Write([]byte{SIMPLE_STRING_SPECIFIER})
	writer.Write([]byte(str + "\r\n"))
}

func writeBulkString(writer *bufio.Writer, str string) {
	writer.Write([]byte{BULK_STRING_SPECIFIER})
	writer.Write([]byte(strconv.Itoa(len(str)) + "\r\n"))
	writer.Write([]byte(str + "\r\n"))
}

func writeInteger(writer *bufio.Writer, str string) {
	writer.Write([]byte{INTEGER_SPECIFIER})
	writer.Write([]byte(str + "\r\n"))
}

func writeError(writer *bufio.Writer, str string) {
	writer.Write([]byte{ERROR_SPECIFIER})
	writer.Write([]byte(str + "\r\n"))
}
