package main

import (
    "database/sql"
    "fmt"

    _ "github.com/lib/pq"
)

const (
    SQL_DRIVER      = "postgres"
    SQL_CONNECT_URL = "user=postgres password=123 dbname=hloaddb sslmode=disable"
)

var sqlDB *sql.DB

func SqlInitDatabase() error {
    fmt.Println(sql.Drivers())
    db_conn, err := sql.Open(SQL_DRIVER, SQL_CONNECT_URL)
    if err != nil {
        return err
    }
	sqlDB = db_conn

    err = sqlDB.Ping()
    if err != nil {
        return err
    }

    _, err = db_conn.Exec("create table if not exists Redirect(id serial, longurl varchar unique)")
    return err
}

func SqlInsertLongurl(longurl string) error {
    _, err := sqlDB.Exec("insert into Redirect(longurl) values ($1) on conflict do nothing", longurl)
	return err
}

func SqlGetLongurlId(longurl string) (int, error) {
	longurl_id := 0
    err := sqlDB.QueryRow("select id from Redirect where longurl = $1", longurl).Scan(&longurl_id)
	return longurl_id, err
}

func SqlGetLongurl(longurl_id int) (string, error) {
    longurl := ""
    err := sqlDB.QueryRow("select longurl from Redirect where id = $1", longurl_id).Scan(&longurl)
	return longurl, err
}
