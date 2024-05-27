package redis_persistence_tests

import (
	"fmt"
	"testing"

	persistence "github.com/codecrafters-io/redis-starter-go/pkg/redis/persistence"
)

func TestParseRDBFile(t *testing.T) {
	store, err := persistence.ParseRDBFile("./", "dump.rdb")
	if err != nil {
		t.Fatal("Error parsing RDB file: ", err.Error())
	}
	fmt.Println(store)
}
