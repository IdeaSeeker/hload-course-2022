package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
)

var ok_urls = func() []string {
    contents, _ := ioutil.ReadFile("urls.txt")
    return strings.Split(string(contents), "\n")
}()

func StressUrlGet302() {
    tinyurls := []string {}
    for _, url := range ok_urls {
        tinyurls = append(tinyurls, urlCreate(url))
    }

    for i := 0; i < 100000; i++ {
        tinyurl_index := rand.Intn(len(tinyurls))
        urlGet(tinyurls[tinyurl_index])
    }
}

func StressUrlGet404() {
    for i := 0; i < 100000; i++ {
        urlGet(randomString(7))
    }
}

func StressUrlCreate() {
    for i := 0; i < 10000; i++ {
        urlCreate(randomUrl(20))
    }
}

// internal

type CreateResponse struct {
    Longurl string
    Tinyurl string
}

var client = &http.Client{
    CheckRedirect: func(req *http.Request, via []*http.Request) error {
        return http.ErrUseLastResponse
    },
}

func urlGet(tinyurl string) {
    client.Get("http://localhost:8080/" + tinyurl)
}

func urlCreate(longurl string) string {
    var requestJsonBytes = bytes.NewBuffer([]byte(`{"longurl": "` + longurl + `"}`))
    request, err := http.NewRequest("PUT", "http://localhost:8080/create", requestJsonBytes)
    if err != nil {
        panic(err)
    }
    request.Header.Set("Content-Type", "application/json; charset=UTF-8")

    client := &http.Client{}
    response, err := client.Do(request)
    if err != nil {
        panic(err)
    }

    var createResponse CreateResponse
    body, err := ioutil.ReadAll(response.Body)
    if err != nil {
        panic(err)
    }
    json.Unmarshal(body, &createResponse)
    return createResponse.Tinyurl
}

func randomString(length int) string {
    b := make([]byte, length)
    rand.Read(b)
    return fmt.Sprintf("%x", b)[:length]
}

func randomUrl(length int) string {
    return "https://" + randomString(length) + ".com"
}
