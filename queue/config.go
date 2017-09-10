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
	TaskTTL       = 24 * 3600
	ZrangeCount   = 20
	RetryInterval = 10 //second

	DelayWorkerInterval = 100 * time.Millisecond
	UnackWorkerInterval = 1000 * time.Millisecond
	ErrorWorkerInterval = 1000 * time.Millisecond
)

var (
	CallbackTTR         = 3 * time.Second //time to run
	MaxIdleConnsPerHost = 10
	MaxIdleConns        = 1024
	IdleConnTimeout     = time.Minute * 5
)
