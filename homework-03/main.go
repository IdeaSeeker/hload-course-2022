package main

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

const (
	USERNAME    = "nstroganov"
	SERVER_HOST = ":8080"
	WORKER_HOST = ":8081"
	REDIS_HOST  = "..."
	KAFKA_HOST  = "..."
)

type CreateRequest struct {
	Longurl string `json:"longurl"`
}

func urlHandle(c *gin.Context) {
	tinyurl := c.Params.ByName("url")

	longurl, err := RedisGetLongurl(tinyurl)
	if err != nil {
        c.JSON(http.StatusNotFound, "Redis internal error: "+err.Error())
		return
	}

	c.Redirect(302, longurl)
}

func createHandle(c *gin.Context) {
	body := CreateRequest{}
	err := c.BindJSON(&body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, "Wrong JSON format: "+err.Error())
	}
	longurl := body.Longurl

	err = SqlInsertLongurl(longurl)
	if err != nil {
		c.JSON(http.StatusInternalServerError, "Database internal error: "+err.Error())
		return
	}

	longurlId, err := SqlGetLongurlId(longurl)
	if err != nil {
		c.JSON(http.StatusInternalServerError, "Database internal error: "+err.Error())
		return
	}
	tinyurl := LongurlIdToTinyurl(int64(longurlId))

	KafkaPushUrls(tinyurl, longurl)

	c.JSON(http.StatusOK, gin.H{"longurl": longurl, "tinyurl": tinyurl})
}

func setupServerRouter() *gin.Engine {
	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	r.PUT("/create", func(c *gin.Context) {
		createHandle(c)
	})

	return r
}

func setupWorkerRouter() *gin.Engine {
	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	r.GET("/:url", func(c *gin.Context) {
		urlHandle(c)
	})

	return r
}

func RunServer() {
	err := SqlInitDatabase()
    if err != nil {
        panic(err)
    }

    go KafkaRunClicksConsumer("server_group_id")

	r := setupServerRouter()
	r.Run(SERVER_HOST)
}

func RunWorker(groupId string) {
	go KafkaRunUrlConsumer(groupId)

	r := setupWorkerRouter()
	r.Run(WORKER_HOST)
}

// go run main.go kafka.go postgres.go redis.go util.go [server|worker_group_id]
func main() {
	strategy := os.Args[len(os.Args)-1]

	if strategy == "server" {
		RunServer()
	} else {
		RunWorker(strategy)
	}
}
