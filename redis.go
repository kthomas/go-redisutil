package redisutil

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-redsync/redsync"
)

const defaultTTL = time.Hour * 24

// Get returns the value for the given key
func Get(key string) (*string, error) {
	var val *string

	if RedisClusterClient != nil {
		valstr, err := RedisClusterClient.Get(key).Result()
		if err != nil {
			log.Warningf("failed to GET key: %s", key, err.Error())
			return nil, err
		}
		val = &valstr
	} else if RedisClient != nil {
		valstr, err := RedisClient.Get(key).Result()
		if err != nil {
			log.Warningf("failed to GET key: %s; %s", key, err.Error())
			return nil, err
		}
		val = &valstr
	}

	return val, nil
}

// Set sets the value for the given key and ttl
func Set(key string, val interface{}, ttl *time.Duration) error {
	keyttl := defaultTTL
	if ttl != nil {
		keyttl = *ttl
	}
	if RedisClusterClient != nil {
		_, err := RedisClusterClient.Set(key, val, keyttl).Result()
		if err != nil {
			log.Warningf("failed to SET key: %s; %s", key, err.Error())
			return err
		}
		log.Debugf("wrote value to key: %s", key)
	} else if RedisClient != nil {
		_, err := RedisClient.Set(key, val, keyttl).Result()
		if err != nil {
			log.Warningf("failed to SET key: %s; %s", key, err.Error())
			return err
		}
		log.Debugf("wrote value to key: %s", key)
	}

	return nil
}

// Decrement atomically decrements the value for the given key
func Decrement(key string) (*int64, error) {
	var val *int64

	if RedisClusterClient != nil {
		valint, err := RedisClusterClient.Decr(key).Result()
		if err != nil {
			log.Warningf("failed to DECR key: %s; %s", key, err.Error())
			return nil, err
		}
		log.Debugf("decremented value at key: %s", key)
		val = &valint
	} else if RedisClient != nil {
		valint, err := RedisClient.Decr(key).Result()
		if err != nil {
			log.Warningf("failed to DECRkey: %s; %s", key, err.Error())
			return nil, err
		}
		log.Debugf("decremented value at key: %s", key)
		val = &valint
	}

	return val, nil
}

// Increment atomically increments the value for the given key
func Increment(key string) (*int64, error) {
	var val *int64

	if RedisClusterClient != nil {
		valint, err := RedisClusterClient.Incr(key).Result()
		if err != nil {
			log.Warningf("failed to INCR key: %s; %s", key, err.Error())
			return nil, err
		}
		log.Debugf("incremented value at key: %s", key)
		val = &valint
	} else if RedisClient != nil {
		valint, err := RedisClient.Incr(key).Result()
		if err != nil {
			log.Warningf("failed to INCR key: %s; %s", key, err.Error())
			return nil, err
		}
		log.Debugf("incremented value at key: %s", key)
		val = &valint
	}

	return val, nil
}

// IncrementFloat atomically inrements the value for the given key by the specified float
func IncrementFloat(key string, delta float64) (*float64, error) {
	var val *float64

	if RedisClusterClient != nil {
		valflt, err := RedisClusterClient.IncrByFloat(key, delta).Result()
		if err != nil {
			log.Warningf("failed to INCRBYFLOAT key: %s; %s", key, err.Error())
			return nil, err
		}
		log.Debugf("incremented value at key: %s", key)
		val = &valflt
	} else if RedisClient != nil {
		valflt, err := RedisClient.IncrByFloat(key, delta).Result()
		if err != nil {
			log.Warningf("failed to INCRBYFLOAT key: %s", key, err.Error())
			return nil, err
		}
		log.Debugf("incremented value at key: %s", key)
		val = &valflt
	}

	return val, nil
}

// WithRedlock attempts to executes the given function after acquiring a lock by the given name;
// returns an error if the lock cannot be acquired or if an error can be propagated from the given
// function; callers should check the error type to ultimately understand why the error occured
// since it can be related to lock acquisition or internal callback execution
func WithRedlock(key string, fn func() error) error {
	if redlock == nil {
		return errors.New("failed to acquire distributed lock; redlock not configured")
	}

	var mutex *redsync.Mutex

	defer func() {
		if r := recover(); r != nil {
			log.Warningf("recovered from failed execution of mutually exclusive callback under lock: %s; %s", key, r)
			if mutex != nil {
				mutex.Unlock()
			}
		}
	}()

	mutex = redlock.NewMutex(fmt.Sprintf("mutex.%s", key))
	defer mutex.Unlock()

	err := mutex.Lock()
	if err != nil {
		log.Warningf("failed to acquire distributed lock; %s", err.Error())
		return err
	}

	log.Debugf("attempting to execute mutually exclusive callback function under lock: %s", key)
	err = fn()
	if err != nil {
		return err
	}

	log.Debugf("executed mutually exclusive callback function under lock: %s", key)
	return nil
}
