package main

import (
    "bytes"
    "encoding/json"
    "io/ioutil"
    "net/http"
)

func StressUrlGet302() {
    tinyurl := urlCreate("https://ya.ru")
    for i := 0; i < 100000; i++ {
        urlGet(tinyurl)
    }
}

func StressUrlGet404() {
    for i := 0; i < 100000; i++ {
        urlGet("tinyurl")
    }
}

func StressUrlCreate() {
    for i := 0; i < 10000; i++ {
        urlCreate("https://ya.ru")
    }
}

// internal

type CreateResponse struct {
    Longurl string
    Tinyurl string
}

func urlGet(tinyurl string) {
    http.Get("http://localhost:8080/" + tinyurl)
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

