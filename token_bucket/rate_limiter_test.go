/****************************************************************************
**
** Copyright (c) 2020 younglionwell@gmail.com
**
** Permission is hereby granted, free of charge, to any person obtaining
** a copy of this software and associated documentation files (the "Software"),
** to deal in the Software without restriction, including without limitation the
** rights to use, copy, modify, merge, publish, distribute, sublicens-e, and/or
** sell copies of the Software, and to permit persons to whom the Software is
** furnished to do so, subject to the following conditions:
**
** The above copyright notice and this permission notice shall be included in
** all copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
** THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
****************************************************************************/

// Package sliding_window_log provides a rate limiter implemented by
// sliding window log algorithm.
// test for sliding window log algorithm.
package token_bucket

import (
	"context"
	"testing"
	"time"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/rcrowley/go-metrics"
	influxdb "github.com/vrischmann/go-metrics-influxdb"
)

const (
	allowMetricsKey = "TestRateLimiterAllow:TB"
	waitMetricsKey  = "TestRateLimiterWait:TB"
)

func init() {
	go influxdb.InfluxDB(metrics.DefaultRegistry,
		time.Second*5,
		"http://localhost:8086",
		"metrics",
		"metrics",
		"metrics-password",
		"", true)
}

func TestRateLimiter(t *testing.T) {
	go testRateLimiterWait(t)
	testRateLimiterAllow(t)
}

func testRateLimiterAllow(t *testing.T) {
	fmt.Println("TestRateLimiterAllow")
	defer fmt.Println("TestRateLimiterAllow End")

	ctx := context.Background()
	innerRedis := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// for metrics
	counter := metrics.NewCounter()
	metrics.Register(allowMetricsKey, counter)

	limiter := NewRateLimiter(innerRedis, 10, "counter:"+allowMetricsKey, time.Millisecond*60)

	for {
		if limiter.AllowN(ctx, 2) {
			go counter.Inc(1)
		} else {
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func testRateLimiterWait(t *testing.T) {
	fmt.Println("TestRateLimiterWait")
	defer fmt.Println("TestRateLimiterWait End")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	innerRedis := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// for metrics
	counter := metrics.NewCounter()
	metrics.Register(waitMetricsKey, counter)

	limiter := NewRateLimiter(innerRedis, 1000, "counter:"+waitMetricsKey, time.Millisecond*10)

	for {
		if limiter.WaitN(ctx, 3) {
			go counter.Inc(1)
		} else {
			break
		}
	}
}
