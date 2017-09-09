package queue

import (
	"encoding/json"
	"fmt"

	"github.com/garyburd/redigo/redis"
)

// Task is the task to execute
type Task struct {
	// ID is a global unique id
	ID string
	// Topic use to classify tasks
	Topic string
	// ExecuteTime is the time to deliver
	ExecuteTime int64
	// MaxRetry is max deliver retry times
	MaxRetry int
	//HasRetry is the current retry times
	HasRetry int
	// Callback is the deliver address
	Callback string
	// Content is the task content to deliver
	Content string
	// CreatTime is the time task created
	CreatTime int64
}

const (
	DelayBucket = "later_delay"
	UnackBucket = "later_unack"
	ErrorBucket = "later_error"
)

func createTask(task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	c := pool.Get()
	defer c.Close()
	_, err = c.Do("SET", task.ID, String(data), "EX", TaskTTL)
	if err != nil {
		return err
	}
	_, err = c.Do("ZADD", DelayBucket, task.ExecuteTime, task.ID)
	return err
}

func updateTask(task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	c := pool.Get()
	defer c.Close()
	ttl, err := redis.Int(c.Do("TTL", task.ID))
	if err != nil {
		return err
	}
	_, err = c.Do("SET", task.ID, String(data), "EX", ttl)
	return err
}

func getTask(id string) (*Task, error) {
	c := pool.Get()
	defer c.Close()
	data, err := redis.String(c.Do("GET", id))
	if err != nil {
		return nil, err
	}
	var task Task
	err = json.Unmarshal(Slice(data), &task)
	return &task, err
}

func getTasks(bucket string, begin int64, end int64) ([]string, error) {
	c := pool.Get()
	defer c.Close()
	return redis.Strings(c.Do("ZRANGEBYSCORE", bucket, begin, end, "LIMIT", "0", fmt.Sprintf("%v", ZrangeCount)))
}

func delayToUnack(id string, score int64) (bool, error) {
	return bucketTransfer(DelayBucket, UnackBucket, id, score)
}

func unackToDelay(id string, score int64) (bool, error) {
	return bucketTransfer(UnackBucket, DelayBucket, id, score)
}

func errorToDelay(id string, score int64) (bool, error) {
	return bucketTransfer(ErrorBucket, DelayBucket, id, score)
}

func bucketTransfer(from string, to string, id string, score int64) (bool, error) {
	c := pool.Get()
	defer c.Close()
	reply, err := redis.Int(c.Do("ZADD", to, score, id))
	if err != nil {
		return false, err
	}
	if reply == 0 {
		return false, nil
	}
	_, err = c.Do("ZREM", from, id)
	return true, err
}

func unackToError(id string, score int64) error {
	c := pool.Get()
	defer c.Close()
	_, err := c.Do("ZADD", ErrorBucket, score, id)
	if err != nil {
		return err
	}
	_, err = c.Do("ZREM", UnackBucket, id)
	return err
}

func deleteTask(id string) error {
	c := pool.Get()
	defer c.Close()
	_, err := c.Do("DEL", id)
	if err != nil {
		return err
	}
	_, err = c.Do("ZREM", DelayBucket, id)
	if err != nil {
		return err
	}
	_, err = c.Do("ZREM", UnackBucket, id)
	if err != nil {
		return err
	}
	_, err = c.Do("ZREM", ErrorBucket, id)
	return err
}
