package main

import (
    "net/http"

    "github.com/gin-gonic/gin"
)

const (
    HOST = ""
    PORT = "8080"
    PROMETHEUS_PORT = "2112"
)

type CreateRequest struct {
    Longurl string `json:"longurl"`
}

func urlHandle(c *gin.Context) {
    tinyurl := c.Params.ByName("url")

    longurl_id := TinyurlToLongurlId(tinyurl)
    longurl, err := SqlGetLongurl(int(longurl_id))
    if err != nil {
        c.Writer.WriteHeader(404)
        return
    }

    c.Redirect(302, longurl)
}

func createHandle(c *gin.Context) {
    body := CreateRequest{}
    err := c.BindJSON(&body)
    if err != nil {
        c.JSON(http.StatusInternalServerError, "Wrong JSON format: " + err.Error())
    }
    longurl := body.Longurl

    err = SqlInsertLongurl(longurl)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"response": "Database internal error: " + err.Error()})
        return
    }

    longurl_id, err := SqlGetLongurlId(longurl)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"response": "Database internal error: " + err.Error()})
        return
    }
    tinyurl := LongurlIdToTinyurl(int64(longurl_id))

    c.JSON(http.StatusOK, gin.H{"longurl": longurl, "tinyurl": tinyurl})
}

func setupRouter() *gin.Engine {
    r := gin.Default()

    r.GET("/ping", func(c *gin.Context) {
        c.String(http.StatusOK, "pong")
    })

    r.GET("/stress", func(c *gin.Context) {
        go StressUrlGet302()
        go StressUrlGet404()
        go StressUrlCreate()
        c.String(http.StatusOK, "OK")
    })

    r.GET("/:url", func(c *gin.Context) {
        ObserveGetRequest(func() { urlHandle(c) })
    })

    r.PUT("/create", func(c *gin.Context) {
        ObservePutRequest(func() { createHandle(c) })
    })

    return r
}

// go run main.go postgres.go prometheus.go stress.go util.go
func main() {
    if err := SqlInitDatabase(); err != nil {
        panic(err)
    }
    RunMetrics("/metrics", HOST, PROMETHEUS_PORT)
    setupRouter().Run(HOST + ":" + PORT)
}
