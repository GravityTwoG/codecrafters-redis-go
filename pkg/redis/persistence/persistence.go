package redis_persistence

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	redis_value "github.com/codecrafters-io/redis-starter-go/pkg/redis/redis-value"
)

const RDB_MAGIC = "REDIS"
const RDB_OPCODE_AUX = 0xFA
const RDB_OPCODE_RESIZE_DB = 0xFB
const RDB_OPCODE_EXPIRE_TIME_MS = 0xFC
const RDB_OPCODE_EXPIRE_TIME = 0xFD
const RDB_OPCODE_SELECTDB = 0xFE
const RDB_OPCODE_EOF = 0xFF

// first 2 bits
const RDB_LEN_6BIT = 0b00
const RDB_LEN_14BIT = 0b01
const RDB_LEN_32BIT = 0b10
const RDB_SPECIAL = 0b11

// last 6 bits
const RDB_S_8BIT = 0b000000
const RDB_S_128BIT = 0b000010
const RDB_S_COMPRESSED = 0b000011

var ErrUnsupportedType = errors.New("unsupported-type")

func ParseRDBFile(dir string, dbfilename string) map[string]redis_value.RedisValue {
	file, err := os.Open(path.Join(dir, dbfilename))
	if errors.Is(err, os.ErrNotExist) {
		fmt.Println("File not found: ", err.Error())
		return nil
	}
	if err != nil {
		fmt.Println("Error opening file: ", err.Error())
		return nil
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	err = verifyMagic(reader)
	if err != nil {
		fmt.Println("Error verifying magic: ", err.Error())
		return nil
	}
	err = verifyVersion(reader)
	if err != nil {
		fmt.Println("Error verifying version: ", err.Error())
		return nil
	}

	dbNumber := 0
loop:
	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			fmt.Println("Error reading opcode: ", err.Error())
			return nil
		}

		switch opcode {
		case RDB_OPCODE_AUX:
			fmt.Println("RDB_OPCODE_AUX")
			key, err := parseString(reader)
			if err != nil {
				fmt.Println("Error parsing key: ", err.Error())
				return nil
			}
			value, err := parseString(reader)
			if err != nil {
				fmt.Println("Error parsing value: ", err.Error())
				return nil
			}
			fmt.Printf("%s: %s\n", key, value)

		case RDB_OPCODE_SELECTDB:
			dbNumber, err = parseLen(reader)
			if err != nil {
				fmt.Println("Error parsing int: ", err.Error())
				return nil
			}
			fmt.Println("dbNumber: ", dbNumber)

		case RDB_OPCODE_RESIZE_DB:
			hashTableSize, err := parseLen(reader)
			if err != nil {
				fmt.Println("Error parsing len: ", err.Error())
				return nil
			}
			fmt.Println("hashTableSize: ", hashTableSize)

			expireHashTableSize, err := parseLen(reader)
			if err != nil {
				fmt.Println("Error parsing len: ", err.Error())
				return nil
			}
			fmt.Println("expireHashTableSize: ", expireHashTableSize)

		case RDB_OPCODE_EOF:
			fmt.Println("EOF")
			return nil

		default:
			reader.UnreadByte()
			fmt.Println("Unknown opcode: ", opcode)
			break loop
		}
	}

	store, err := parseDB(reader)
	if err != nil {
		fmt.Println("Error parsing db: ", err.Error())
		return nil
	}
	return store
}

// Check magic string REDIS or [82 69 68 73 83]
func verifyMagic(reader *bufio.Reader) error {
	magic := make([]byte, 5)
	n, err := reader.Read(magic)
	if n < 5 {
		fmt.Println("Error reading magic: ", err.Error())
		return err
	}
	if string(magic) != RDB_MAGIC {
		fmt.Println("Magic not REDIS: ", string(magic))
		return nil
	}

	return nil
}

func verifyVersion(reader *bufio.Reader) error {
	version := make([]byte, 4)
	n, err := reader.Read(version)
	if n < 4 {
		fmt.Println("Error reading version: ", err.Error())
		return err
	}
	if err != nil {
		fmt.Println("Error reading version: ", err.Error())
		return err
	}
	fmt.Println("Version: ", version)
	return nil
}

const RDB_VALUE_TYPE_STRING = 0
const RDB_VALUE_TYPE_LIST = 1
const RDB_VALUE_TYPE_SET = 2

// Sorted set
const RDB_VALUE_TYPE_SSET = 3

// Hash
const RDB_VALUE_TYPE_HASH = 4

// Zipmap
const RDB_VALUE_TYPE_ZMAP = 9

// Ziplist
const RDB_VALUE_TYPE_ZLIST = 10

// Intset
const RDB_VALUE_TYPE_ISET = 11

// Sorted set in ziplist
const RDB_VALUE_TYPE_SSZLIST = 12

// Hashmap in ziplist
const RDB_VALUE_TYPE_HMZLIST = 13

// List in quicklist
const RDB_VALUE_TYPE_LQLIST = 14

func parseDB(reader *bufio.Reader) (map[string]redis_value.RedisValue, error) {
	store := make(map[string]redis_value.RedisValue)
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return store, err
		}
		if b == RDB_OPCODE_EOF {
			return store, nil
		}
		reader.UnreadByte()

		key, value, err := parseKeyValue(reader)
		if err != nil {
			return store, err
		}
		if key == "" {
			return store, nil
		}

		fmt.Printf("key: %s, value: %s\n", key, value)
		store[key] = *value
	}
}

func parseKeyValue(reader *bufio.Reader) (string, *redis_value.RedisValue, error) {
	expiryTime, err := parseExpiryTime(reader)
	if err != nil && err != ErrUnsupportedType {
		return "", nil, err
	}

	valueType, err := reader.ReadByte()
	if err != nil {
		return "", nil, err
	}
	fmt.Println("valueType: ", valueType)
	if valueType == RDB_OPCODE_EOF {
		return "", nil, io.EOF
	}

	key, err := parseString(reader)
	if err != nil {
		fmt.Println("Error parsing key: ", err.Error())
		return "", nil, err
	}

	value, err := parseString(reader)
	if err != nil {
		fmt.Println("Error parsing value: ", err.Error())
		return "", nil, err
	}

	return key, &redis_value.RedisValue{
		Value:     value,
		ExpiresAt: expiryTime,
	}, nil
}

func parseExpiryTime(reader *bufio.Reader) (*time.Time, error) {
	timeType, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	if timeType == RDB_OPCODE_EXPIRE_TIME_MS {
		ms, err := readInt64(reader)
		if err != nil {
			return nil, err
		}

		fmt.Println("now: ", time.Now().UnixMilli())
		fmt.Println("ms: ", ms)
		fmt.Println(time.UnixMilli(ms))

		t := time.UnixMilli(ms)
		return &t, nil
	}

	if timeType == RDB_OPCODE_EXPIRE_TIME {
		sec, err := readInt32(reader)
		if err != nil {
			return nil, err
		}

		t := time.Unix(int64(sec), 0)
		return &t, nil
	}

	fmt.Println("Unsupported time type: ", timeType)
	reader.UnreadByte()
	return nil, ErrUnsupportedType
}

func parseString(reader *bufio.Reader) (string, error) {
	valueType, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	reader.UnreadByte()

	valueType = firstTwoBits(valueType)

	if valueType != RDB_SPECIAL {
		len, err := parseLen(reader)
		if err != nil {
			return "", err
		}

		str := make([]byte, len)
		n, err := reader.Read(str)
		if n < len {
			return "", err
		}

		return string(str), nil
	}

	return parseSpecialValue(reader)
}

func parseSpecialValue(reader *bufio.Reader) (string, error) {
	valueType, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	valueType = lastSixBits(valueType)
	fmt.Println("Value type: ", valueType)

	if valueType == RDB_S_8BIT {
		b, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		return string([]byte{b}), nil
	}

	if valueType == RDB_S_128BIT {
		q := make([]byte, 4)
		n, err := reader.Read(q)
		if n < 4 {
			return "", err
		}
		return string(q), nil
	}

	if valueType == RDB_S_COMPRESSED {
		compressedLen, err := parseLen(reader)
		if err != nil {
			return "", err
		}
		uncompressedLen, err := parseLen(reader)
		if err != nil {
			return "", err
		}

		compressed := make([]byte, compressedLen)
		n, err := reader.Read(compressed)
		if n < compressedLen {
			return "", err
		}
		uncompressed, err := LZFDecompress(compressed)

		if err != nil {
			return "", err
		}
		if uncompressedLen != len(uncompressed) {
			return "", errors.New("invalid length")
		}
		return string(uncompressed), nil
	}

	return "", ErrUnsupportedType
}

// Parse Length Encoding
func parseLen(reader *bufio.Reader) (int, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return -1, err
	}

	first2Bits := firstTwoBits(b)
	if first2Bits == RDB_LEN_6BIT {
		// next 6 bits represents length
		return int(b), nil
	}

	if first2Bits == RDB_LEN_14BIT {
		// next 14 bits represents length
		nextB, err := reader.ReadByte()
		if err != nil {
			return -1, err
		}
		return int(b)<<8 | int(nextB), nil
	}

	if first2Bits == RDB_LEN_32BIT {
		// Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
		return readInt32(reader)
	}

	reader.UnreadByte()
	fmt.Println("Unsupported first 2 bits: ", first2Bits)
	return -1, errors.New("unsupported format")
}
