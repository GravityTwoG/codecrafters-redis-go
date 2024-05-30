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

type EntryID struct {
	MsTime int
	SeqNum int
}

func (e *EntryID) String() string {
	return fmt.Sprintf("%d-%d", e.MsTime, e.SeqNum)
}

func (e *EntryID) Equal(other *EntryID) bool {
	return e.MsTime == other.MsTime && e.SeqNum == other.SeqNum
}

func (e *EntryID) Less(other *EntryID) bool {
	if e.MsTime < other.MsTime {
		return true
	}
	if e.MsTime > other.MsTime {
		return false
	}
	return e.SeqNum < other.SeqNum
}

func (e *EntryID) LessOrEqual(other *EntryID) bool {
	if e.Equal(other) {
		return true
	}
	return e.Less(other)
}

func (e *EntryID) Greater(other *EntryID) bool {
	return other.Less(e)
}

func (e *EntryID) GreaterOrEqual(other *EntryID) bool {
	if e.Equal(other) {
		return true
	}
	return e.Greater(other)
}

func ParseID(id string) (*EntryID, error) {
	if id == "*" {
		return nil, ErrGenerateID
	}

	delimeterIdx := strings.Index(id, "-")
	if delimeterIdx == -1 {
		return nil, ErrInvalidID
	}

	msTimeStr := id[:delimeterIdx]
	if msTimeStr == "" {
		return nil, ErrInvalidID
	}
	msTime, err := strconv.Atoi(msTimeStr)
	if err != nil {
		return nil, err
	}

	seqNumStr := id[delimeterIdx+1:]
	if seqNumStr == "*" {
		return &EntryID{MsTime: msTime, SeqNum: -1}, ErrGenerateSeqNum
	}
	if seqNumStr == "" {
		return nil, ErrInvalidID
	}
	seqNum, err := strconv.Atoi(seqNumStr)
	if err != nil {
		return nil, err
	}

	if msTime < 0 || seqNum < 0 {
		return nil, ErrIDLessThanMinimum
	}
	if msTime == 0 && seqNum == 0 {
		return nil, ErrIDLessThanMinimum
	}

	return &EntryID{
		MsTime: msTime,
		SeqNum: seqNum,
	}, nil
}

func GenerateID(currentId string, prevID *EntryID) *EntryID {
	id, err := ParseID(currentId)

	if prevID == nil {
		if err == ErrGenerateSeqNum {
			newID := &EntryID{
				MsTime: id.MsTime,
				SeqNum: 0,
			}
			if id.MsTime == 0 {
				newID.SeqNum = 1
			}
			return newID
		} else if err == ErrGenerateID {
			return &EntryID{
				MsTime: int(time.Now().UnixMilli()),
				SeqNum: 0,
			}
		}

		return nil
	}

	if err == ErrGenerateSeqNum {
		newID := &EntryID{
			MsTime: id.MsTime,
			SeqNum: prevID.SeqNum + 1,
		}
		if id.MsTime > prevID.MsTime {
			newID.SeqNum = 0
		}
		return newID

	} else if err == ErrGenerateID {
		newID := &EntryID{
			MsTime: int(time.Now().UnixMilli()),
			SeqNum: prevID.SeqNum,
		}
		if newID.MsTime == prevID.MsTime {
			newID.SeqNum += 1
		}
		return newID

	}

	return nil
}
