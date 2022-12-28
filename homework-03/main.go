package main

import (
    "database/sql"
    "fmt"
    "net/http"
    "os"

    "github.com/gin-gonic/gin"
    _ "github.com/lib/pq"
)

const (
    USERNAME   = "nstroganov"
    SERVER_HOST = ":8080"
    WORKER_HOST = ":8081"
    REDIS_HOST = "..."
    KAFKA_HOST = "..."
)

const (
    SQL_DRIVER      = "postgres"
    SQL_CONNECT_URL = "user=postgres password=123 dbname=hloaddb sslmode=disable"
)

type CreateRequest struct {
    Longurl string `json:"longurl"`
}

func urlHandle(c *gin.Context) {
    tinyurl := c.Params.ByName("url")

    longurl, err := RedisGetLongurl(tinyurl)
    if err != nil {
        c.Writer.WriteHeader(404)
        return
    }

    c.Redirect(302, longurl)
}

func createHandle(c *gin.Context, db_conn *sql.DB) {
    body := CreateRequest{}
    err := c.BindJSON(&body)
    if err != nil {
        c.JSON(http.StatusInternalServerError, "Wrong JSON format: "+err.Error())
    }
    longurl := body.Longurl

    _, err = db_conn.Exec("insert into Redirect(longurl) values ($1) on conflict do nothing", longurl)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"response": "Database internal error: " + err.Error()})
        return
    }

    longurl_id := 0
    err = db_conn.QueryRow("select id from Redirect where longurl = $1", longurl).Scan(&longurl_id)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"response": "Database internal error: " + err.Error()})
        return
    }
    tinyurl := LongurlIdToTinyurl(int64(longurl_id))

    // KafkaPushUrls(tinyurl, longurl)

    c.JSON(http.StatusOK, gin.H{"longurl": longurl, "tinyurl": tinyurl})
}

func setupServerRouter(db_conn *sql.DB) *gin.Engine {
    r := gin.Default()

    r.GET("/ping", func(c *gin.Context) {
        c.String(http.StatusOK, "pong")
    })

    r.PUT("/create", func(c *gin.Context) {
        createHandle(c, db_conn)
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
    fmt.Println(sql.Drivers())
    db_conn, err := sql.Open(SQL_DRIVER, SQL_CONNECT_URL)
    if err != nil {
        panic(err)
    }

    err = db_conn.Ping()
    if err != nil {
        panic(err)
    }

    _, err = db_conn.Exec("create table if not exists Redirect(id serial, longurl varchar unique)")
    if err != nil {
        panic(err)
    }

    r := setupServerRouter(db_conn)
    r.Run(SERVER_HOST)
}

func RunWorker() {
    // go KafkaRunConsumer()

    r := setupWorkerRouter()
    r.Run(WORKER_HOST)
}

func main() {
    strategy := os.Args[len(os.Args) - 1]

    if strategy == "server" {
        RunServer()
    } else if strategy == "worker" {
        RunWorker()
    } else {
        fmt.Println("Expected exactly one argument: 'server' or 'worker'")
        panic("Unknown strategy: " + strategy)
    }
}
