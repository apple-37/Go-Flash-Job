package main

import (
	"context"
	"go-flash-job/pkg/model"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTriggerReceipt(t *testing.T) {
	for code, want := range map[int]string{202: "ACCEPTED", 200: "ACCEPTED", 429: "RETRY", 503: "RETRY", 400: "DEAD", 302: "DEAD"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" || r.Header.Get("X-Job-ID") != "crawl-1" {
				t.Error("invalid request")
			}
			w.WriteHeader(code)
		}))
		got := trigger(context.Background(), model.Task{ID: "crawl-1", CallbackURL: server.URL})
		server.Close()
		if got.status != want {
			t.Errorf("HTTP %d: got %s, want %s", code, got.status, want)
		}
	}
}
func TestExpiredBeforeHTTP(t *testing.T) {
	got := trigger(context.Background(), model.Task{LatestAt: time.Now().Unix() - 2})
	if got.status != "EXPIRED" {
		t.Fatalf("expired task: %v", got)
	}
}
