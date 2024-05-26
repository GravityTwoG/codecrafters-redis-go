package redis_persistence_tests

import (
	"fmt"
	"testing"

	persistence "github.com/codecrafters-io/redis-starter-go/pkg/redis/persistence"
)

func TestParseRDBFile(t *testing.T) {
	store := persistence.ParseRDBFile("./", "dump.rdb")

	if store == nil {
		t.Fatal("Error parsing RDB file")
		return
	}
	fmt.Println(store)
}
