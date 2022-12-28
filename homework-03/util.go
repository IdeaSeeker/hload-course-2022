package main

import (
    "strings"
)

const abc = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func LongurlIdToTinyurl(longurl_id int64) string {
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

func TinyurlToLongurlId(tinyurl string) int64 {
    longurl_id := int64(0)
    for _, c := range tinyurl {
        longurl_id = longurl_id*int64(len(abc)) + int64(strings.IndexRune(abc, c))
    }
    return longurl_id
}
