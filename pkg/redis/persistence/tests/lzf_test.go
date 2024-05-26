package redis_persistence_tests

import (
	"fmt"
	"testing"

	persistence "github.com/codecrafters-io/redis-starter-go/pkg/redis/persistence"
)

func TestLZFDecompress(t *testing.T) {
	str := "hello world"

	compressed, err := persistence.LZFCompress([]byte(str))
	if err != nil {
		t.Fatal(err)
	}
	decompressed, err := persistence.LZFDecompress(compressed)
	if err != nil {
		t.Fatal(err)
	}

	if len(decompressed) < len(str) {
		fmt.Println("Decompressed: ", string(decompressed))
		t.Fatal("Decompress failed")
	}
	decompressed = decompressed[:len(str)]

	if string(decompressed) != str {
		fmt.Println("Decompressed: ", string(decompressed))
		t.Fatal("Decompress failed")
	}
}
