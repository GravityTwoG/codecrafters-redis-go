package entry_id_test

import (
	"testing"

	entry_id "github.com/codecrafters-io/redis-starter-go/pkg/redis/streams-store/id"

	"github.com/stretchr/testify/assert"
)

func TestParseID(t *testing.T) {
	t.Run("valid id", func(t *testing.T) {
		t.Parallel()

		id := "1526919030474-0"
		parsedID, err := entry_id.ParseID(id)

		assert.Nil(t, err)
		assert.NotNil(t, parsedID)
		assert.Equal(t, 1526919030474, parsedID.MsTime)
		assert.Equal(t, 0, parsedID.SeqNum)
	})

	t.Run("valid id", func(t *testing.T) {
		t.Parallel()

		id := "0-1"
		parsedID, err := entry_id.ParseID(id)

		assert.Nil(t, err)
		assert.NotNil(t, parsedID)
		assert.Equal(t, 0, parsedID.MsTime)
		assert.Equal(t, 1, parsedID.SeqNum)
	})

	t.Run("valid id", func(t *testing.T) {
		t.Parallel()

		id := "1-1"
		parsedID, err := entry_id.ParseID(id)

		assert.Nil(t, err)
		assert.NotNil(t, parsedID)
		assert.Equal(t, 1, parsedID.MsTime)
		assert.Equal(t, 1, parsedID.SeqNum)
	})

	t.Run("invalid id", func(t *testing.T) {
		t.Parallel()

		id := "1526919030474"
		parsedID, err := entry_id.ParseID(id)

		assert.NotNil(t, err)
		assert.Nil(t, parsedID)
	})

	t.Run("invalid id", func(t *testing.T) {
		t.Parallel()

		id := "-1-1"
		parsedID, err := entry_id.ParseID(id)

		assert.NotNil(t, err)
		assert.Nil(t, parsedID)
	})

	t.Run("invalid id", func(t *testing.T) {
		t.Parallel()

		id := "1--1"
		parsedID, err := entry_id.ParseID(id)

		assert.NotNil(t, err)
		assert.Nil(t, parsedID)
	})

	t.Run("generate SeqNum", func(t *testing.T) {
		t.Parallel()

		id := "12-*"
		parsedID, err := entry_id.ParseID(id)

		assert.Equal(t, err, entry_id.ErrGenerateSeqNum)
		assert.NotNil(t, parsedID)
		assert.Equal(t, 12, parsedID.MsTime)
		assert.Equal(t, -1, parsedID.SeqNum)
	})

	t.Run("generate ID", func(t *testing.T) {
		t.Parallel()

		id := "*"
		parsedID, err := entry_id.ParseID(id)

		assert.Equal(t, err, entry_id.ErrGenerateID)
		assert.Nil(t, parsedID)
	})
}

func TestAreIDsIncreasing(t *testing.T) {
	t.Run("valid increasing ids", func(t *testing.T) {
		t.Parallel()

		id1 := entry_id.EntryID{MsTime: 1526919030474, SeqNum: 0}
		id2 := entry_id.EntryID{MsTime: 1526919030474, SeqNum: 1}

		assert.True(t, id2.Greater(&id1))
	})

	t.Run("valid increasing ids", func(t *testing.T) {
		t.Parallel()

		id1 := entry_id.EntryID{
			MsTime: 1526919030474,
			SeqNum: 9,
		}
		id2 := entry_id.EntryID{
			MsTime: 1526919030477,
			SeqNum: 1,
		}

		assert.True(t, id2.Greater(&id1))
	})

	t.Run("invalid increasing ids", func(t *testing.T) {
		t.Parallel()

		id1 := entry_id.EntryID{MsTime: 1, SeqNum: 1}
		id2 := entry_id.EntryID{MsTime: 0, SeqNum: 2}

		assert.False(t, id2.Greater(&id1))
	})
}
