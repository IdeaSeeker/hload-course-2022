package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RunMetrics(route string, host string, port string) {
    http.Handle(route, promhttp.Handler())
    go http.ListenAndServe(host + ":" + port, nil)
}

func ObserveGetRequest(get_request func()) {
    withMetrics(get_request, urlOpsProcessed, urlOpsElapsedTime)
}

func ObservePutRequest(put_request func()) {
    withMetrics(put_request, createOpsProcessed, createOpsElapsedTime)
}

// internal

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

func withMetrics(block func(), counter prometheus.Counter, summary prometheus.Summary) {
    counter.Inc()
    elapsed := MeasureSeconds(block)
    summary.Observe(elapsed)
}