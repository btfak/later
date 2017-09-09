package queue

import (
	"time"
)

var (
	RedisConnectTimeout  = 50 * time.Millisecond
	RedisReadTimeout     = 50 * time.Millisecond
	RedisWriteTimeout    = 100 * time.Millisecond
	RedisPoolMaxIdle     = 200
	RedisPoolIdleTimeout = 3 * time.Minute
)

var (
	TaskTTL             = 24 * 3600
	ZrangeCount         = 20
	DelayWorkerInterval = 100 * time.Millisecond
	UnackWorkerInterval = 1000 * time.Millisecond
	ErrorWorkerInterval = 1000 * time.Millisecond
	RetryInterval       = 10 //second
)
