package redis_protocol

import (
	"bufio"
	"fmt"

	streams_store "github.com/codecrafters-io/redis-starter-go/pkg/redis/streams-store"
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

func WriteArrayLength(writer *bufio.Writer, length int) {
	writer.Write([]byte(
		fmt.Sprintf("%c%d\r\n", ARRAY_SPECIFIER, length),
	))
}

func WriteBulkStringArray(writer *bufio.Writer, strs []string) {
	WriteArrayLength(writer, len(strs))
	for _, str := range strs {
		WriteBulkString(writer, str)
	}
}

func WriteInteger(writer *bufio.Writer, val int) {
	writer.Write([]byte(fmt.Sprintf("%c%d\r\n", INTEGER_SPECIFIER, val)))
}

func WriteError(writer *bufio.Writer, str string) {
	writer.Write([]byte(fmt.Sprintf("%c%s\r\n", ERROR_SPECIFIER, str)))
}

type Stream struct {
	Key     string
	Entries []streams_store.StreamEntry
}

func WriteStream(writer *bufio.Writer, stream *Stream) {
	WriteArrayLength(writer, 2)
	WriteBulkString(writer, stream.Key)
	WriteArrayLength(writer, len(stream.Entries))
	for _, entry := range stream.Entries {
		WriteArrayLength(writer, 2)
		WriteBulkString(writer, entry.ID.String())
		WriteArrayLength(writer, len(entry.Values))
		for _, value := range entry.Values {
			WriteBulkString(writer, value)
		}
	}
}
