package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"go-flash-job/pkg/model"
)

var httpClient = &http.Client{
	Timeout:       60 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	Transport:     &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 50, IdleConnTimeout: 90 * time.Second},
}

type outcome struct {
	status string
	reason string
}

func trigger(ctx context.Context, t model.Task) outcome {
	if t.LatestAt > 0 && time.Now().Unix() > t.LatestAt {
		return outcome{"EXPIRED", "freshness deadline passed"}
	}
	timeout := min(t.Timeout, 60)
	if timeout <= 0 {
		timeout = 30
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.CallbackURL, bytes.NewReader(t.Payload))
	if err != nil {
		return outcome{"DEAD", err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "return=minimal")
	req.Header.Set("X-Job-ID", t.ID)
	req.Header.Set("X-Job-Name", t.Name)
	req.Header.Set("X-Retry-Count", strconv.Itoa(t.RetryCount))
	resp, err := httpClient.Do(req)
	if err != nil {
		var netErr *net.OpError
		if errors.As(err, &netErr) && netErr.Op == "dial" {
			return outcome{"RETRY", err.Error()}
		}
		return outcome{"UNKNOWN", err.Error()}
	}
	defer resp.Body.Close()
	// Receipt of response headers determines the outcome; never wait for a crawl result.
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return outcome{"ACCEPTED", ""}
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return outcome{"RETRY", resp.Status}
	default:
		return outcome{"DEAD", fmt.Sprintf("crawler returned %s", resp.Status)}
	}
}
