package entry_id_test

import (
	"testing"

	entry_id "github.com/codecrafters-io/redis-starter-go/pkg/redis/store/id"

	"github.com/stretchr/testify/assert"
)

func TestParseID(t *testing.T) {
	t.Run("valid id", func(t *testing.T) {
		t.Parallel()

		id := "1526919030474-0"
		msTime, seqNum, err := entry_id.ParseID(id)

		assert.Nil(t, err)
		assert.Equal(t, 1526919030474, msTime)
		assert.Equal(t, 0, seqNum)
	})

	t.Run("valid id", func(t *testing.T) {
		t.Parallel()

		id := "0-1"
		msTime, seqNum, err := entry_id.ParseID(id)

		assert.Nil(t, err)
		assert.Equal(t, 0, msTime)
		assert.Equal(t, 1, seqNum)
	})

	t.Run("valid id", func(t *testing.T) {
		t.Parallel()

		id := "1-1"
		msTime, seqNum, err := entry_id.ParseID(id)

		assert.Nil(t, err)
		assert.Equal(t, 1, msTime)
		assert.Equal(t, 1, seqNum)
	})

	t.Run("invalid id", func(t *testing.T) {
		t.Parallel()

		id := "1526919030474"
		msTime, seqNum, err := entry_id.ParseID(id)

		assert.NotNil(t, err)
		assert.Equal(t, -1, msTime)
		assert.Equal(t, -1, seqNum)
	})

	t.Run("invalid id", func(t *testing.T) {
		t.Parallel()

		id := "-1-1"
		msTime, seqNum, err := entry_id.ParseID(id)

		assert.NotNil(t, err)
		assert.Equal(t, -1, msTime)
		assert.Equal(t, -1, seqNum)
	})

	t.Run("invalid id", func(t *testing.T) {
		t.Parallel()

		id := "1--1"
		msTime, seqNum, err := entry_id.ParseID(id)

		assert.NotNil(t, err)
		assert.Equal(t, -1, msTime)
		assert.Equal(t, -1, seqNum)
	})

	t.Run("generate SeqNum", func(t *testing.T) {
		t.Parallel()

		id := "12-*"
		msTime, seqNum, err := entry_id.ParseID(id)

		assert.Equal(t, err, entry_id.ErrGenerateSeqNum)
		assert.Equal(t, 12, msTime)
		assert.Equal(t, -1, seqNum)
	})

	t.Run("generate ID", func(t *testing.T) {
		t.Parallel()

		id := "*"
		msTime, seqNum, err := entry_id.ParseID(id)

		assert.Equal(t, err, entry_id.ErrGenerateID)
		assert.Equal(t, -1, msTime)
		assert.Equal(t, -1, seqNum)
	})
}

func TestAreIDsIncreasing(t *testing.T) {
	t.Run("valid increasing ids", func(t *testing.T) {
		t.Parallel()

		id1 := "1526919030474-0"
		id2 := "1526919030474-1"

		assert.True(t, entry_id.AreIDsIncreasing(id1, id2))
	})

	t.Run("valid increasing ids", func(t *testing.T) {
		t.Parallel()

		id1 := "1526919030474-9"
		id2 := "1526919030477-1"

		assert.True(t, entry_id.AreIDsIncreasing(id1, id2))
	})

	t.Run("invalid increasing ids", func(t *testing.T) {
		t.Parallel()

		id1 := "1-1"
		id2 := "0-2"

		assert.False(t, entry_id.AreIDsIncreasing(id1, id2))
	})
}
