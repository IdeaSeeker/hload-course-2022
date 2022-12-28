package main

import (
    "context"

    "github.com/go-redis/redis/v8"
)

var cnxt = context.Background()
var redisOptions = redis.Options{Addr: REDIS_HOST, Password: "", DB: 0}
var redisClient *redis.Client = redis.NewClient(&redisOptions)

func RedisSetUrls(tinyurl string, longurl string) error {
    tinyurlKey := constructRedisKey(tinyurl)
    return redisClient.Set(cnxt, tinyurlKey, longurl, 0).Err()
}

func RedisGetLongurl(tinyurl string) (string, error) {
    tinyurlKey := constructRedisKey(tinyurl)
    return redisClient.Get(cnxt, tinyurlKey).Result()
}

func constructRedisKey(tinyurl string) string {
    return USERNAME + "|" + tinyurl
}
