package queue

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
)

func ListenAndServe(addr string) error {
	http.HandleFunc("/create", createHandler)
	http.HandleFunc("/delete", deleteHandler)
	http.HandleFunc("/query", queryHandler)
	return http.ListenAndServe(addr, nil)
}

type createRequest struct {
	Topic string `json:"topic"`
	// Delay is the number of seconds that should elapse before the task execute
	Delay    int64  `json:"delay"`
	Retry    int    `json:"retry"`
	Callback string `json:"callback"`
	Content  string `json:"content"`
}

type createResponse struct {
	ID string `json:"id"`
}

func createHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(400)
		return
	}
	var request createRequest
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithError(err).Error("io read from frontend fail")
		w.WriteHeader(500)
		return
	}
	err = json.Unmarshal(data, &request)
	if err != nil {
		log.WithError(err).Error("json unmarshal fail")
		w.WriteHeader(400)
		return
	}
	task := &Task{
		ID:          uuid.New(),
		Topic:       request.Topic,
		ExecuteTime: time.Now().Unix() + request.Delay,
		MaxRetry:    request.Retry,
		Callback:    request.Callback,
		Content:     request.Content,
		CreatTime:   time.Now().Unix(),
	}
	err = createTask(task)
	if err != nil {
		log.WithError(err).Error("create task fail")
		w.WriteHeader(500)
		return
	}
	response := createResponse{ID: task.ID}
	respData, err := json.Marshal(response)
	if err != nil {
		log.WithError(err).Error("json marshal fail")
		w.WriteHeader(500)
		return
	}
	w.Write(respData)
}

type deleteRequest struct {
	ID string `json:"id"`
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(400)
		return
	}
	var request deleteRequest
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithError(err).Error("io read from frontend fail")
		w.WriteHeader(500)
		return
	}
	err = json.Unmarshal(data, &request)
	if err != nil {
		log.WithError(err).Error("json unmarshal fail")
		w.WriteHeader(400)
		return
	}
	if request.ID == "" {
		w.WriteHeader(400)
		return
	}
	err = deleteTask(request.ID)
	if err != nil {
		log.WithError(err).Error("delete task fail")
		w.WriteHeader(500)
		return
	}
	w.WriteHeader(200)
}

type queryRequest struct {
	ID string `json:"id"`
}

type queryResponse struct {
	ID          string `json:"id"`
	Topic       string `json:"topic"`
	ExecuteTime int64  `json:"execute_time"`
	MaxRetry    int    `json:"max_retry"`
	HasRetry    int    `json:"has_retry"`
	Callback    string `json:"callback"`
	Content     string `json:"content"`
	CreatTime   int64  `json:"creat_time"`
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(400)
		return
	}
	var request queryRequest
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithError(err).Error("io read from frontend fail")
		w.WriteHeader(500)
		return
	}
	err = json.Unmarshal(data, &request)
	if err != nil {
		log.WithError(err).Error("json unmarshal fail")
		w.WriteHeader(400)
		return
	}
	if request.ID == "" {
		w.WriteHeader(400)
		return
	}
	task, err := getTask(request.ID)
	if err != nil {
		if err == redis.ErrNil {
			w.WriteHeader(404)
			return
		}
		log.WithError(err).Error("get task fail")
		w.WriteHeader(500)
		return
	}
	response := queryResponse{
		ID:          task.ID,
		Topic:       task.Topic,
		ExecuteTime: task.ExecuteTime,
		MaxRetry:    task.MaxRetry,
		HasRetry:    task.HasRetry,
		Callback:    task.Callback,
		Content:     task.Content,
		CreatTime:   task.CreatTime,
	}
	respData, err := json.Marshal(response)
	if err != nil {
		log.WithError(err).Error("json marshal fail")
		w.WriteHeader(500)
		return
	}
	w.Write(respData)
}
