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
const RDB_ENC_6BIT = 0b00
const RDB_ENC_14BIT = 0b01
const RDB_ENC_32BIT = 0b10
const RDB_ENC_SPECIAL = 0b11

// last 6 bits
// Integer as string
const RDB_SPECIAL_INTS_8BIT = 0b000000
const RDB_SPECIAL_INTS_16BIT = 0b000001
const RDB_SPECIAL_INTS_32BIT = 0b000010
const RDB_SPECIAL_S_COMPRESSED = 0b000011

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

var ErrUnknownOpcode = errors.New("unknown-opcode")
var ErrUnsupportedType = errors.New("unsupported-type")

func ParseRDBFile(dir string, dbfilename string) (map[string]redis_value.RedisValue, error) {
	file, err := os.Open(path.Join(dir, dbfilename))
	if errors.Is(err, os.ErrNotExist) {
		fmt.Println("File not found: ", err.Error())
		return nil, err
	}
	if err != nil {
		fmt.Println("Error opening file: ", err.Error())
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	err = verifyMagic(reader)
	if err != nil {
		fmt.Println("Error verifying magic: ", err.Error())
		return nil, err
	}
	err = verifyVersion(reader)
	if err != nil {
		fmt.Println("Error verifying version: ", err.Error())
		return nil, err
	}

	dbNumber := 0
	hashTableSize := 0
	expireHashTableSize := 0

	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			fmt.Println("Error reading opcode: ", err.Error())
			return nil, err
		}

		switch opcode {
		case RDB_OPCODE_AUX:
			key, err := parseString(reader)
			if err != nil {
				fmt.Println("Error parsing key: ", err.Error())
				return nil, err
			}
			value, err := parseString(reader)
			if err != nil {
				fmt.Println("Error parsing value: ", err.Error())
				return nil, err
			}
			fmt.Printf("%s: %s\n", key, value)

		case RDB_OPCODE_SELECTDB:
			dbNumber, err = parseLen(reader)
			if err != nil {
				fmt.Println("Error parsing dbNumber: ", err.Error())
				return nil, err
			}
			fmt.Println("dbNumber: ", dbNumber)

		case RDB_OPCODE_RESIZE_DB:
			hashTableSize, err = parseLen(reader)
			if err != nil {
				fmt.Println("Error parsing len: ", err.Error())
				return nil, err
			}
			fmt.Println("hashTableSize: ", hashTableSize)

			expireHashTableSize, err = parseLen(reader)
			if err != nil {
				fmt.Println("Error parsing len: ", err.Error())
				return nil, err
			}
			fmt.Println("expireHashTableSize: ", expireHashTableSize)

			store, err := parseDB(reader)
			if err != nil {
				fmt.Println("Error parsing db: ", err.Error())
				return nil, err
			}
			return store, nil

		case RDB_OPCODE_EOF:
			fmt.Println("EOF")
			return nil, io.EOF

		default:
			reader.UnreadByte()
			fmt.Println("Unknown opcode: ", opcode)
			return nil, ErrUnknownOpcode
		}
	}
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
	fmt.Println("Version: ", string(version))
	return nil
}

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
		if key == "" || value == nil {
			return store, nil
		}

		fmt.Printf("key: %s, value: %s\n", key, value)
		store[key] = *value
	}
}

func parseKeyValue(reader *bufio.Reader) (string, *redis_value.RedisValue, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return "", nil, err
	}

	var expiryTime *time.Time = nil
	if b == RDB_OPCODE_EXPIRE_TIME {
		expiryTime, err = parseExpiryTimeSec(reader)
	} else if b == RDB_OPCODE_EXPIRE_TIME_MS {
		expiryTime, err = parseExpiryTimeMs(reader)
	} else {
		// there is no expiry
		reader.UnreadByte()
	}
	if err != nil {
		return "", nil, err
	}

	valueType, err := reader.ReadByte()
	if err != nil {
		return "", nil, err
	}

	key, err := parseString(reader)
	if err != nil {
		fmt.Println("Error parsing key: ", err.Error())
		return "", nil, err
	}

	if valueType != RDB_VALUE_TYPE_STRING {
		fmt.Print("unsupported valueType: ", valueType)
		return "", nil, ErrUnsupportedType
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

func parseExpiryTimeMs(reader *bufio.Reader) (*time.Time, error) {
	ms, err := readInt64(reader)
	if err != nil {
		return nil, err
	}

	t := time.UnixMilli(ms)
	return &t, nil
}

func parseExpiryTimeSec(reader *bufio.Reader) (*time.Time, error) {
	sec, err := readInt32(reader)
	if err != nil {
		return nil, err
	}

	t := time.Unix(int64(sec), 0)
	return &t, nil
}

func parseString(reader *bufio.Reader) (string, error) {
	valueType, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	reader.UnreadByte()

	valueType = firstTwoBits(valueType)

	if valueType != RDB_ENC_SPECIAL {
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

	if valueType == RDB_SPECIAL_INTS_8BIT {
		b, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", b), nil
	}

	if valueType == RDB_SPECIAL_INTS_16BIT {
		value, err := readInt16(reader)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", value), nil
	}

	if valueType == RDB_SPECIAL_INTS_32BIT {
		value, err := readInt32(reader)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", value), nil
	}

	if valueType == RDB_SPECIAL_S_COMPRESSED {
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
		if uncompressedLen < len(uncompressed) {
			return "", errors.New("invalid length")
		}
		return string(uncompressed[:uncompressedLen]), nil
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
	if first2Bits == RDB_ENC_6BIT {
		// next 6 bits represents length
		return int(b), nil
	}

	if first2Bits == RDB_ENC_14BIT {
		// next 14 bits represents length
		nextB, err := reader.ReadByte()
		if err != nil {
			return -1, err
		}
		return int(b)<<8 | int(nextB), nil
	}

	if first2Bits == RDB_ENC_32BIT {
		// Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
		return readInt32(reader)
	}

	reader.UnreadByte()
	return -1, errors.New("special-format")
}
