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
// token bucket algorithm.

package token_bucket

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"github.com/go-redis/redis/v8"
)

type Limit int64

const Inf = Limit(math.MaxInt64)

// RateLimiter controls how frequently events are allowed to happen.
//
// RateLimiter has two main method: Allow and Wait
//
// If the count of token in bucket is greater or equal to the 1 in
// a period time, Allow returns false
//
// If the count of token in bucket is greater or equal to the limit in
// a period time, Wait blocks until  the count of token in bucket is less
// than the 1 or its associated context.Context is canceled.
type RateLimiter struct {
	actionKey string
	// Add a token into the bucket every period.
	period time.Duration
	// The size of token bucket
	limit      Limit
	expiration time.Duration

	innerRedis *redis.Client
}

type tokenBucket struct {
	TimeStamp int64 `json:"ts"`  // last update time
	Count     int64 `json:"cnt"` // the sum of tokens in this bucket
}

// NewRateLimiter returns a new RateLimiter that add a token into the bucket every period
// and the size of its bucket is limit
func NewRateLimiter(client *redis.Client, limit Limit, actionKey string, period time.Duration) *RateLimiter {
	return &RateLimiter{
		innerRedis: client,
		limit:      limit,
		actionKey:  actionKey,
		period:     period,
		expiration: period * time.Duration(limit) + time.Second, // avoid cold data
	}
}

// Allow is shorthand for AllowN(ctx, 1).
func (limiter *RateLimiter) Allow(ctx context.Context) bool{
	return limiter.AllowN(ctx, 1)
}

// AllowN reports whether n events may happen at time now.
// Use this method if you intend to drop / skip action that exceed the rate limit.
// Otherwise use Wait.
func (limiter *RateLimiter) AllowN(ctx context.Context, n int64) bool {
	// TODO: Should be an atomic operation. (Use LUA script maybe.)
	var bucket tokenBucket
	result, err := limiter.innerRedis.Get(ctx, limiter.actionKey).Result()

	// bucket is not full, check if it's enough for AllowN.
	timestamp := time.Now().UnixNano() / limiter.period.Nanoseconds()
	if err == nil && json.Unmarshal([]byte(result), &bucket) == nil {
		// update bucket
		bucket.Count += (timestamp - bucket.TimeStamp)
		if Limit(bucket.Count) > limiter.limit {
			bucket.Count = int64(limiter.limit)
		}
	} else {
		// the bucket is full by default
		bucket.Count = int64(limiter.limit)
	}
	bucket.TimeStamp = timestamp

	// token(s) is/are not enough.
	if bucket.Count < n {
		val, _ := json.Marshal(bucket)
		limiter.innerRedis.SetEX(ctx, limiter.actionKey, val, limiter.expiration)
		return false
	}

	// get n tokens
	bucket.Count -= n
	val, _ := json.Marshal(bucket)
	limiter.innerRedis.SetEX(ctx, limiter.actionKey, val, limiter.expiration)

	return true
}

// Wait is shorthand for WaitN(ctx, 1).
func (limiter *RateLimiter) Wait(ctx context.Context) bool {
	return limiter.WaitN(ctx, 1)
}

// WaitN blocks until lim permits n events to happen.
func (limiter *RateLimiter) WaitN(ctx context.Context, n int64) bool {
	// Check if ctx is already cancelled
	select {
	case <-ctx.Done():
		return false
	default:
	}

	for {
		if limiter.AllowN(ctx, n) {
			return true
		}

		// Allow failed, wait for limiter.interval
		timer := time.NewTimer(limiter.period)

		select {
		case <-timer.C:
			// keep trying...
			timer.Stop()
		case <-ctx.Done():
			// Context was canceled before we could proceed.
			timer.Stop()
			return false
		}
	}

	// never be here.
	return true
}
