package redis

type RedisCommand struct {
	Name       string
	Parameters []string
}

func (c *RedisCommand) ToStringArray() []string {
	return append([]string{c.Name}, c.Parameters...)
}
