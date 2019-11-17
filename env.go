package redisutil

import (
	"fmt"
	"os"
	"strconv"
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

	// redisDBIndex is the index of the redis db
	redisDBIndex int

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
}

// RequireRedis reads the environment and initializes the configured
// redis client or cluster client
func RequireRedis() {
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

	if os.Getenv("REDIS_DB_INDEX") != "" {
		idx, err := strconv.ParseInt(os.Getenv("REDIS_DB_INDEX"), 10, 8)
		if err != nil {
			log.Panicf("failed to parse REDIS_DB_INDEX from environment; %s", err.Error())
		}
		redisDBIndex = int(idx)
	}

	if os.Getenv("REDIS_PASSWORD") != "" {
		redisPassword = os.Getenv("REDIS_PASSWORD")
	}

	requireRedisConn()
}

func requireRedisConn() {
	if len(redisHosts) == 1 {
		RedisClient = redis.NewClient(&redis.Options{
			Addr:     redisHosts[0],
			DB:       redisDBIndex,
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
		redisURL := fmt.Sprintf("redis://%s:%s/%d", redisPassword, host, redisDBIndex)
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
