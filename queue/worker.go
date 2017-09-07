package queue

import (
	"math"
	"time"

	"github.com/garyburd/redigo/redis"
	log "github.com/sirupsen/logrus"
)

func RunWorker() {
	go delayWorker()
	go unackWorker()
	go errorWorker()
}

func delayWorker() {
	ticker := time.NewTicker(DelayWorkerInterval)
	for _ = range ticker.C {
		go func() {
			begin := time.Now().Add(-time.Duration(TaskTTL) * time.Second).Unix()
			end := time.Now().Unix()
			ids, err := getTasks(DelayBucket, begin, end)
			if err != nil {
				log.WithError(err).Error("get tasks fail")
				return
			}
			for _, id := range ids {
				go callback(id)
			}
		}()
	}
}

func unackWorker() {
	ticker := time.NewTicker(UnackWorkerInterval)
	for _ = range ticker.C {
		go func() {
			begin := time.Now().Add(-time.Duration(TaskTTL)).Unix()
			end := time.Now().Unix()
			ids, err := getTasks(UnackBucket, begin, end)
			if err != nil {
				return
			}
			for _, id := range ids {
				unackToDelay(id, time.Now().Unix())
			}
		}()
	}
}

func errorWorker() {
	ticker := time.NewTicker(ErrorWorkerInterval)
	for _ = range ticker.C {
		go func() {
			begin := time.Now().Add(-time.Duration(TaskTTL)).Unix()
			end := time.Now().Unix()
			ids, err := getTasks(ErrorBucket, begin, end)
			if err != nil {
				return
			}
			for _, id := range ids {
				errorToDelay(id, time.Now().Unix())
			}
		}()
	}
}

func callback(id string) {
	task, err := getTask(id)
	if err != nil {
		if err == redis.ErrNil {
			if err = deleteTask(id); err != nil {
				log.WithError(err).Error("delete task fail")
			}
		}
		return
	}
	got, err := delayToUnack(id, time.Now().Unix())
	if err != nil {
		log.WithError(err).Error("transfer from delay to unack fail")
		return
	}
	if !got {
		return
	}
	code, err := post(task)
	if err != nil {
		goto retry
	}
	if code == CodeSuccess {
		if err = deleteTask(id); err != nil {
			log.WithError(err).Error("delete task fail")
		}
		return
	}
	log.Errorf("backend fail, code is %v", code)

retry:
	task.HasRetry++
	if task.HasRetry > task.MaxRetry {
		if err = deleteTask(id); err != nil {
			log.WithError(err).Error("delete task fail")
		}
		return
	}
	err = updateTask(task)
	if err != nil {
		log.WithError(err).Error("update task fail")
	}
	// (1,2,4,8...) * X
	score := time.Now().Unix() + int64(math.Pow(2, float64(task.HasRetry-1)))*int64(RetryInterval)
	err = unackToError(id, score)
	if err != nil {
		log.WithError(err).Error("transfer from unack to error bucket fail")
		return
	}
}
