package main

import (
	"flag"

	"github.com/btfak/later/queue"
	log "github.com/sirupsen/logrus"
)

var (
	redisURL = flag.String("redis", "redis://127.0.0.1:6379/0", "redis address")
	address  = flag.String("address", ":8080", "serve listen address")
)

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	flag.Parse()
}

func main() {
	err := queue.InitRedis(*redisURL)
	if err != nil {
		log.Fatal(err)
	}
	queue.RunWorker()
	log.Infof("server listen on :%v", *address)
	err = queue.ListenAndServe(*address)
	if err != nil {
		log.Fatal(err)
	}
}
