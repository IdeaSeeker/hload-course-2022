package main

import (
	"fmt"
	"strconv"
	"github.com/go-redis/redis/v8"
	"dqueue"
	"os"
)

// 158.160.9.8:6379 51.250.106.140:6379 158.160.19.212:6379 158.160.19.2:6379 158.160.9.8 51.250.106.140 158.160.19.212
func main() {
	hosts := os.Args[1:]
	if len(hosts) != 7 {
		fmt.Println("Expected 4 addresses of redis shards and 3 addresses of zookeeper cluster")
		return
	}

	var redisOptions = []*redis.Options{
		{ Addr: hosts[0], Password: "", DB: 0 },
		{ Addr: hosts[1], Password: "", DB: 0 },
		{ Addr: hosts[2], Password: "", DB: 0 },
		{ Addr: hosts[3], Password: "", DB: 0 },
	}
	var zkCluster = []string{ hosts[4], hosts[5], hosts[6] }

	dqueue.Config(&redisOptions, zkCluster)
	q, err := dqueue.Open("nikita:queue2", 4)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		err := q.Push(strconv.Itoa(i))
		if err != nil {
			panic(err)
		}
	}
	println("Pushed 10 values")

	println("Poll:")
	for i := 0; i < 10; i++ {
		value, err := q.Pull()
		if err != nil {
			panic(err)
		} else {
			println(value)
		}
	}
}