package redisutil

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/go-redsync/redsync"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/kthomas/go-logger"
)

const defaultRedlockMaxIdle = 3
const defaultRedlockIdleTimeout = 240 * time.Second

var (
	// RedisClient is the redis client
	RedisClient *redis.Client

	// RedisClusterClient is the redis cluster client
	RedisClusterClient *redis.ClusterClient

	// RedisHosts is an array of <host>:<port> strings
	redisHosts []string

	// RedisPassword is the redis password
	redisPassword string

	log *logger.Logger

	redlock      *redsync.Redsync
	redsyncPools []redsync.Pool
)

func init() {
	lvl := os.Getenv("REDIS_LOG_LEVEL")
	if lvl == "" {
		lvl = os.Getenv("REDIS_LOG_LEVEL")
		if lvl == "" {
			lvl = "INFO"
		}
	}
	log = logger.NewLogger("go-redisutil", lvl, true)

	requireRedis()
	requireRedisConn()
}

func requireRedis() {
	redisHosts = make([]string, 0)
	redsyncPools = make([]redsync.Pool, 0)

	if os.Getenv("REDIS_HOSTS") != "" {
		hosts := strings.Split(os.Getenv("REDIS_HOSTS"), ",")
		for _, host := range hosts {
			redisHosts = append(redisHosts, strings.Trim(host, " "))
		}
	} else {
		log.Panicf("failed to parse REDIS_HOSTS from environment")
	}

	if os.Getenv("REDIS_PASSWORD") != "" {
		redisPassword = os.Getenv("REDIS_PASSWORD")
	}
}

func requireRedisConn() {
	if len(redisHosts) == 1 {
		RedisClient = redis.NewClient(&redis.Options{
			Addr:     redisHosts[0],
			Password: redisPassword,
		})
	} else if len(redisHosts) > 1 {
		RedisClusterClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    redisHosts,
			Password: redisPassword,
		})
	}
}

func requireRedsync() {
	for _, host := range redisHosts {
		// FIXME-- add db path
		redisURL := fmt.Sprintf("redis://%s:%s", redisPassword, host)
		redsyncPools = append(redsyncPools, &redigo.Pool{
			MaxIdle:     defaultRedlockMaxIdle,
			IdleTimeout: defaultRedlockIdleTimeout,
			Dial: func() (redigo.Conn, error) {
				return redigo.DialURL(redisURL)
			},
			TestOnBorrow: func(c redigo.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		})
	}

	redlock = redsync.New(redsyncPools)
}
