package entry_id

import (
	"errors"
	"strconv"
	"strings"
)

var ErrInvalidID = errors.New("invalid-id")
var ErrIDLessThanMinimum = errors.New("ERR The ID specified in XADD must be greater than 0-0")
var ErrIDsNotIncreasing = errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")

func ParseID(id string) (int, int, error) {
	delimeterIdx := strings.Index(id, "-")
	if delimeterIdx == -1 {
		return -1, -1, ErrInvalidID
	}

	msTimeStr := id[:delimeterIdx]
	if msTimeStr == "" {
		return -1, -1, ErrInvalidID
	}
	msTime, err := strconv.Atoi(msTimeStr)
	if err != nil {
		return -1, -1, err
	}

	seqNumStr := id[delimeterIdx+1:]
	if seqNumStr == "" {
		return -1, -1, ErrInvalidID
	}
	seqNum, err := strconv.Atoi(seqNumStr)
	if err != nil {
		return -1, -1, err
	}

	if msTime < 0 || seqNum < 0 {
		return -1, -1, ErrIDLessThanMinimum
	}
	if msTime == 0 && seqNum == 0 {
		return -1, -1, ErrIDLessThanMinimum
	}

	return msTime, seqNum, nil
}

func AreIDsIncreasing(id1 string, id2 string) bool {
	msTime1, seqNum1, err := ParseID(id1)
	if err != nil {
		return false
	}
	msTime2, seqNum2, err := ParseID(id2)
	if err != nil {
		return false
	}

	if msTime2 > msTime1 {
		return true
	}
	if msTime2 == msTime1 {
		return seqNum2 > seqNum1
	}

	return false
}
