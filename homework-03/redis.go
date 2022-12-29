package main

import (
    "context"

    "github.com/go-redis/redis/v8"
)

const (
    CLICKS_TO_SYNC = 100
)

var cnxt = context.Background()
var redisOptions = redis.Options{Addr: REDIS_HOST, Password: "", DB: 0}
var redisClient *redis.Client = redis.NewClient(&redisOptions)

func RedisSetUrls(tinyurl string, longurl string) error {
    tinyurlKey := constructRedisKey(tinyurl)
    return redisClient.Set(cnxt, tinyurlKey, longurl, 0).Err()
}

func RedisGetLongurl(tinyurl string) (string, error) {
    tinyurlClicksKey := constructRedisClickKey(tinyurl)
    currentClicksNumber, err := redisClient.Incr(cntx, tinyurlClicksKey).Result()
    if err != nil {
        return "", err
    }
    if currentClicksNumber % CLICKS_TO_SYNC == 0 {
        KafkaPushClicks(tinyurl, CLICKS_TO_SYNC)
    }

    tinyurlKey := constructRedisKey(tinyurl)
    return redisClient.Get(cnxt, tinyurlKey).Result()
}

// internal

func constructRedisKey(tinyurl string) string {
    return USERNAME + "|" + tinyurl
}

func constructRedisClickKey(tinyurl string) string {
    return USERNAME + "|clicks|" + tinyurl
}
