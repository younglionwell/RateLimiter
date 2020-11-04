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

// Package fixed_window_counter provides a rate limiter implemented by
// sliding window counter algorithm.
package sliding_window_counter

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

// Limit defines the maximum frequency of some events.
// Limit is represented as number of events per second.
// A zero Limit allows no events.
type Limit int64

// Inf is the infinite rate limit; it allows all events.
const Inf = Limit(math.MaxInt64)

// RateLimiter controls how frequently events are allowed to happen.
// The zero value is a valid Limiter, but it will reject all events.
//
// RateLimiter has two main method: Allow and Wait
//
// If the count of actionKey is greater or equal to the limit in
// a period time, Allow returns false
//
// If the count of actionKey is greater or equal to the limit in
// a period time, Wait blocks until  the count of actionKey is less than
// the limit or its associated context.Context is canceled.
type RateLimiter struct {
	actionKey string
	// the action only happends limit times in a period time.
	period   time.Duration
	limit    Limit
	// the interval time between two window counter
	interval time.Duration

	innerRedis *redis.Client
}

func (limiter *RateLimiter) getFiledKey(cur time.Time) string {
	return strconv.FormatInt(cur.UnixNano() / limiter.interval.Nanoseconds(), 10)
}

// NewRateLimiter returns a new RateLimiter that allows the action up to happening
// limit times in a period time.
func NewRateLimiter(client *redis.Client, limit Limit, actionKey string, period time.Duration,
	interval time.Duration) *RateLimiter {
	return &RateLimiter{
		innerRedis: client,
		limit:      limit,
		actionKey:  actionKey,
		period:     period,
		interval:   interval,
	}
}

// Allow reports whether the action exceed the rate limit.
// Use this method if you intend to drop / skip action that exceed the rate limit.
// Otherwise use Wait.
func (limiter *RateLimiter) Allow(ctx context.Context) bool {
	// remove expired counter
	// Should be an atomic operation. (Use LUA script maybe.)
	result, err := limiter.innerRedis.HGetAll(ctx, limiter.actionKey).Result()
	if err != nil { // something wrong with actionKey.
		limiter.innerRedis.Del(ctx, limiter.actionKey)
		return true
	}

	// delete expire keys
	timestamp := time.Now().UnixNano() / limiter.interval.Nanoseconds()
	var delKey []string
	wndSize := int64(0)
	for k, v := range result {
		ik, _ := strconv.ParseInt(k, 10, 64)
		iv, _ := strconv.ParseInt(v, 10, 64)
		if ik <= timestamp {
			delKey = append(delKey, k)
		} else {
			wndSize += iv
		}
	}
	limiter.innerRedis.HDel(ctx, limiter.actionKey, delKey...)

	// check if up to limit
	if Limit(wndSize) >= limiter.limit {
		return false
	}

	// add for new action
	limiter.innerRedis.HIncrBy(ctx, limiter.actionKey, limiter.getFiledKey(time.Now().Add(limiter.period)), 1)

	// avoid cold data
	limiter.innerRedis.Expire(ctx, limiter.actionKey, limiter.period+time.Second)

	return true
}

// Wait reports whether the action exceed the rate limit.
func (limiter *RateLimiter) Wait(ctx context.Context) bool {
	// Check if ctx is already cancelled
	select {
	case <-ctx.Done():
		return false
	default:
	}

	for {
		if limiter.Allow(ctx) {
			return true
		}

		// Allow failed, wait for limiter.interval
		timer := time.NewTimer(limiter.interval)

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
