package redis_value

import "time"

type RedisValue struct {
	Value     string
	ExpiresAt *time.Time
}
