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

const rdbMagic = "REDIS"
const rdbOpcodeAux = 0xFA
const rdbOpcodeResizeDB = 0xFB
const rdbOpcodeExpireTimeMs = 0xFC
const rdbOpcodeExpireTime = 0xFD
const rdpOpcodeSelectDB = 0xFE
const rdbOpcodeEOF = 0xFF

// first 2 bits
const rdbEnc6Bit = 0b00
const rdbEnc14Bit = 0b01
const rdbEnc32Bit = 0b10
const rdbEncSpecial = 0b11

// last 6 bits
// Integer as string
const rdbSpecialInts8Bit = 0b000000
const rdbSpecialInts16Bit = 0b000001
const rdbSpecialInts32Bit = 0b000010
const rdbSpecialSCompressed = 0b000011

const rdbValueTypeString = 0
const rdbValueTypeList = 1
const rdbValueTypeSet = 2

// Sorted set
const rdbValueTypeSSet = 3

// Hash
const rdbValueTypeHash = 4

// Zipmap
const rdbValueTypeZMap = 9

// Ziplist
const rdbValueTypeZList = 10

// Intset
const rdbValueTypeISet = 11

// Sorted set in ziplist
const rdbValueTypeSSZList = 12

// Hashmap in ziplist
const rdbValueTypeHMZList = 13

// List in quicklist
const rdbValueTypeLQList = 14

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
		case rdbOpcodeAux:
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

		case rdpOpcodeSelectDB:
			dbNumber, err = parseLen(reader)
			if err != nil {
				fmt.Println("Error parsing dbNumber: ", err.Error())
				return nil, err
			}
			fmt.Println("dbNumber: ", dbNumber)

		case rdbOpcodeResizeDB:
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

		case rdbOpcodeEOF:
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
	if string(magic) != rdbMagic {
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
		if b == rdbOpcodeEOF {
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
	if b == rdbOpcodeExpireTime {
		expiryTime, err = parseExpiryTimeSec(reader)
	} else if b == rdbOpcodeExpireTimeMs {
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

	if valueType != rdbValueTypeString {
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

	if valueType != rdbEncSpecial {
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

	if valueType == rdbSpecialInts8Bit {
		b, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", b), nil
	}

	if valueType == rdbSpecialInts16Bit {
		value, err := readInt16(reader)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", value), nil
	}

	if valueType == rdbSpecialInts32Bit {
		value, err := readInt32(reader)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", value), nil
	}

	if valueType == rdbSpecialSCompressed {
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
	if first2Bits == rdbEnc6Bit {
		// next 6 bits represents length
		return int(b), nil
	}

	if first2Bits == rdbEnc14Bit {
		// next 14 bits represents length
		nextB, err := reader.ReadByte()
		if err != nil {
			return -1, err
		}
		return int(b)<<8 | int(nextB), nil
	}

	if first2Bits == rdbEnc32Bit {
		// Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
		return readInt32(reader)
	}

	reader.UnreadByte()
	return -1, errors.New("special-format")
}
