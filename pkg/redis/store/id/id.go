package entry_id

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidID = errors.New("invalid-id")
var ErrIDLessThanMinimum = errors.New("ERR The ID specified in XADD must be greater than 0-0")
var ErrIDsNotIncreasing = errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")

var ErrGenerateSeqNum = errors.New("generate-seq-num")
var ErrGenerateID = errors.New("generate-id")

func ParseID(id string) (int, int, error) {
	if id == "*" {
		return -1, -1, ErrGenerateID
	}

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
	if seqNumStr == "*" {
		return msTime, -1, ErrGenerateSeqNum
	}
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

func GenerateID(currentId string, prevId string) string {
	msTime, _, err := ParseID(currentId)

	if prevId == "" {
		if err == ErrGenerateSeqNum {
			seqNum := 0
			if msTime == 0 {
				seqNum = 1
			}
			return fmt.Sprintf("%d-%d", msTime, seqNum)
		} else if err == ErrGenerateID {
			msTime = int(time.Now().UnixMilli())
			return fmt.Sprintf("%d-%d", msTime, 0)
		}

		return ""
	}

	if err == ErrGenerateSeqNum {

		prevMsTime, prevSeqNum, _ := ParseID(prevId)
		seqNum := prevSeqNum + 1
		if msTime > prevMsTime {
			seqNum = 0
		}
		return fmt.Sprintf("%d-%d", msTime, seqNum)

	} else if err == ErrGenerateID {

		prevMsTime, prevSeqNum, _ := ParseID(prevId)
		msTime = int(time.Now().UnixMilli())
		seqNum := prevSeqNum
		if msTime == prevMsTime {
			seqNum += 1
		}
		return fmt.Sprintf("%d-%d", msTime, seqNum)

	}

	return ""
}
