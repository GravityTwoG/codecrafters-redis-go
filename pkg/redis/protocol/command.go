package redis_protocol

const (
	PING = "PING"
	ECHO = "ECHO"

	SET  = "SET"
	GET  = "GET"
	KEYS = "KEYS"

	PSYNC = "PSYNC"

	INFO = "INFO"

	REPLCONF = "REPLCONF"
	WAIT     = "WAIT"

	CONFIG = "CONFIG"

	TYPE = "TYPE"
)

type RedisCommand struct {
	Name       string
	Parameters []string
}

func (c *RedisCommand) ToStringArray() []string {
	return append([]string{c.Name}, c.Parameters...)
}
