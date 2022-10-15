package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

const (
	SQL_DRIVER      = "postgres"
	SQL_CONNECT_URL = "user=postgres password=123 dbname=hloaddb sslmode=disable"
	abc             = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

type CreateRequest struct {
	Longurl string `json:"longurl"`
}

func setupRouter(db_conn *sql.DB) *gin.Engine {
	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	r.GET("/:url", func(c *gin.Context) {
		tinyurl := c.Params.ByName("url")

		longurl := ""
		longurl_id := tinyurl_to_longurl_id(tinyurl)
		err := db_conn.QueryRow("select longurl from Redirect where id = $1", longurl_id).Scan(&longurl)
		if err != nil {
			c.Writer.WriteHeader(404)
			return
		}

		c.Redirect(302, longurl)
	})

	r.PUT("/create", func(c *gin.Context) {
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
		tinyurl := longurl_id_tinyurl(int64(longurl_id))

		c.JSON(http.StatusOK, gin.H{"longurl": longurl, "tinyurl": tinyurl})
	})

	return r
}

func longurl_id_tinyurl(longurl_id int64) string {
	tinyurl := ""
	for longurl_id > 0 {
		tinyurl = string(abc[longurl_id%int64(len(abc))]) + tinyurl
		longurl_id /= int64(len(abc))
	}
	for len(tinyurl) < 7 {
		tinyurl = string(abc[0]) + tinyurl
	}
	return tinyurl
}

func tinyurl_to_longurl_id(tinyurl string) int64 {
	longurl_id := int64(0)
	for _, c := range tinyurl {
		longurl_id = longurl_id*int64(len(abc)) + int64(strings.IndexRune(abc, c))
	}
	return longurl_id
}

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

	r := setupRouter(db_conn)
	r.Run(":8080")
}
