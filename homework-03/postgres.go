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
	db, err := sql.Open(SQL_DRIVER, SQL_CONNECT_URL)
	if err != nil {
		return err
	}
	sqlDB = db

	err = sqlDB.Ping()
	if err != nil {
		return err
	}

	_, err = sqlDB.Exec("create table if not exists Redirect2(id serial, longurl varchar unique, clicks int default 0)")
	return err
}

func SqlInsertLongurl(longurl string) error {
	_, err := sqlDB.Exec("insert into Redirect2(longurl) values ($1) on conflict do nothing", longurl)
	return err
}

func SqlGetLongurlId(longurl string) (int, error) {
	longurlId := 0
	err := sqlDB.QueryRow("select id from Redirect2 where longurl = $1", longurl).Scan(&longurlId)
	return longurlId, err
}

func SqlUpdateClicks(tinyurl string, plusClicksNumber string) error {
	longurlId := TinyurlToLongurlId(tinyurl)
	_, err := sqlDB.Exec("update Redirect2 set clicks = clicks + " + plusClicksNumber + " where id = $1", longurlId)
	return err
}
