package redis_persistence

import (
	"bufio"
	"encoding/binary"
)

func firstTwoBits(value byte) byte {
	return (value & 0b11000000) >> 6
}

func lastSixBits(value byte) byte {
	return value & 0b00111111
}

func readInt16(reader *bufio.Reader) (int, error) {
	b := make([]byte, 2)
	n, err := reader.Read(b)
	if err != nil || n != 2 {
		return -1, err
	}

	return int(binary.LittleEndian.Uint16(b)), nil
}

func readInt32(reader *bufio.Reader) (int, error) {
	b := make([]byte, 4)
	n, err := reader.Read(b)
	if err != nil || n != 4 {
		return -1, err
	}

	return int(binary.LittleEndian.Uint32(b)), nil
}

func readInt64(reader *bufio.Reader) (int64, error) {
	b := make([]byte, 8)
	n, err := reader.Read(b)
	if err != nil || n != 8 {
		return -1, err
	}

	return int64(binary.LittleEndian.Uint64(b)), nil
}
