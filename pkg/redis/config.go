package redis

type RedisConfig struct {
	Host       string
	Port       string
	ReplicaOf  string
	Dir        string
	DBFilename string
}
