package redis

import (
	"encoding/json"
	"github.com/go-redis/redis/v7"
	"mysql-event-cacher/config"
	"time"
)

type RedisClient struct {
	client      *redis.Client
	defaultTime time.Duration
}

func NewRedisClient(cfg *config.Config, defaultTime time.Duration) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	if _, err := client.Ping().Result(); err != nil {
		return nil, err
	} else {
		return &RedisClient{
			client:      client,
			defaultTime: defaultTime,
		}, nil
	}
}

func (r *RedisClient) Store(key string, value interface{}, time time.Duration) error {
	if bytes, err := json.Marshal(value); err != nil {
		return err
	} else {
		return r.client.Set(key, bytes, time).Err()
	}
}

func (r *RedisClient) StoreSimple(key string, value interface{}) error {
	return r.Store(key, value, r.defaultTime)
}

func (r *RedisClient) Load(key string, destination interface{}) error {
	if p, err := r.client.Get(key).Bytes(); err != nil {
		return err
	} else if err = json.Unmarshal(p, destination); err != nil {
		return err
	} else {
		return nil
	}
}
