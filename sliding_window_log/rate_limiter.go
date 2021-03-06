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
package sliding_window_log


import (
	"context"
	"math"
	"time"
	"fmt"

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
	period time.Duration
	limit  Limit

	innerRedis *redis.Client
}

// NewRateLimiter returns a new RateLimiter that allows the action up to happening
// limit times in a period time.
func NewRateLimiter(client *redis.Client, limit Limit, actionKey string, period time.Duration) *RateLimiter {
	return &RateLimiter{
		innerRedis: client,
		limit:      limit,
		actionKey:  actionKey,
		period:     period,
	}
}

// Allow reports whether the action exceed the rate limit.
// Use this method if you intend to drop / skip action that exceed the rate limit.
// Otherwise use Wait.
func (limiter *RateLimiter) Allow(ctx context.Context) bool {
	// TODO: the ZRemRangeByScore, ZCard should be atomic operations, otherwise
	// the Allow might allow actions happend more than limiting rate.  
	// remove expired log                   
	timestamp := time.Now().UnixNano()
	_, err := limiter.innerRedis.ZRemRangeByScore(ctx, limiter.actionKey,
		"-inf", fmt.Sprintf("%v", timestamp-limiter.period.Nanoseconds())).Result()
	if err != nil { // something wrong with actionKey.
		limiter.innerRedis.Del(ctx, limiter.actionKey)
		return true
	}

	wndSize, _ := limiter.innerRedis.ZCard(ctx, limiter.actionKey).Result()
	if Limit(wndSize) >= limiter.limit {
		return false
	}

	_, err = limiter.innerRedis.ZAdd(ctx, limiter.actionKey, 
		&redis.Z{float64(timestamp), timestamp}).Result()
	if err != nil {
		return false
	}

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

		// Allow failed, wait for first log in zset to expire.
		// ZRANGE: Returns the specified range of elements in the sorted set stored at key.
		// The elements are considered to be ordered from the lowest to the highest score.
		// Lexicographical order is used for elements with equal score.
		result, err := limiter.innerRedis.ZRangeWithScores(ctx, limiter.actionKey, 0, 0).Result()
		if err != nil || len(result) == 0{
			continue
		}
		ttl := limiter.period.Nanoseconds()-(time.Now().UnixNano()-int64(result[0].Score)) - 1
		if ttl <= 0 {
			continue
		}
		timer := time.NewTimer(time.Duration(ttl) * time.Nanosecond)

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