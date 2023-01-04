package main

import (
    "database/sql"
    "fmt"
    "net/http"

    "github.com/gin-gonic/gin"
    _ "github.com/lib/pq"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
    HOST = ""
    PORT = "8080"
)

const (
    SQL_DRIVER      = "postgres"
    SQL_CONNECT_URL = "user=postgres password=123 dbname=hloaddb sslmode=disable"
)

var (
    urlOpsProcessed = promauto.NewCounter(prometheus.CounterOpts{
        Name: "url_ops_total",
        Help: "The total number of processed /:url queries",
    })
    urlOpsElapsedTime = promauto.NewSummary(prometheus.SummaryOpts{
        Name: "url_ops_time",
        Help: "Time of /:url query processing",
    })
    createOpsProcessed = promauto.NewCounter(prometheus.CounterOpts{
        Name: "create_ops_total",
        Help: "The total number of processed /create queries",
    })
    createOpsElapsedTime = promauto.NewSummary(prometheus.SummaryOpts{
        Name: "create_ops_time",
        Help: "Time of /create query processing",
    })
)

type CreateRequest struct {
    Longurl string `json:"longurl"`
}

func urlHandle(c *gin.Context, db_conn *sql.DB) {
    tinyurl := c.Params.ByName("url")

    longurl := ""
    longurl_id := TinyurlToLongurlId(tinyurl)
    err := db_conn.QueryRow("select longurl from Redirect where id = $1", longurl_id).Scan(&longurl)
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
        c.JSON(http.StatusInternalServerError, "Wrong JSON format: " + err.Error())
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

    c.JSON(http.StatusOK, gin.H{"longurl": longurl, "tinyurl": tinyurl})
}

func setupRouter(db_conn *sql.DB) *gin.Engine {
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
        urlOpsProcessed.Inc()
        elapsed := MeasureSeconds(func() { urlHandle(c, db_conn) })
        urlOpsElapsedTime.Observe(elapsed)
    })

    r.PUT("/create", func(c *gin.Context) {
        createOpsProcessed.Inc()
        elapsed := MeasureSeconds(func() { createHandle(c, db_conn) })
        createOpsElapsedTime.Observe(elapsed)
    })

    return r
}

// go run main.go util.go stress.go
func main() {
    fmt.Println(sql.Drivers())
    db_conn, err := sql.Open(SQL_DRIVER, SQL_CONNECT_URL)
    if err != nil {
        fmt.Println("Failed to open", err)
        panic("exit")
    }

    err = db_conn.Ping()
    if err != nil {
        fmt.Println("Failed to ping database", err)
        panic("exit")
    }

    _, err = db_conn.Exec("create table if not exists Redirect(id serial, longurl varchar unique)")
    if err != nil {
        fmt.Println("Failed to create table", err)
        panic("exit")
    }

    http.Handle("/metrics", promhttp.Handler())
    go http.ListenAndServe(":2112", nil)

    r := setupRouter(db_conn)
    r.Run(HOST + ":" + PORT)
}
