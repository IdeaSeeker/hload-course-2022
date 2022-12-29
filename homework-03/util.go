package main

import "strings"

const abc = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func LongurlIdToTinyurl(longurlId int64) string {
	tinyurl := ""
	for longurlId > 0 {
		tinyurl = string(abc[longurlId%int64(len(abc))]) + tinyurl
		longurlId /= int64(len(abc))
	}
	for len(tinyurl) < 7 {
		tinyurl = string(abc[0]) + tinyurl
	}
	return tinyurl
}

func TinyurlToLongurlId(tinyurl string) int64 {
	longurlId := int64(0)
	for _, c := range tinyurl {
		longurlId = longurlId*int64(len(abc)) + int64(strings.IndexRune(abc, c))
	}
	return longurlId
}
