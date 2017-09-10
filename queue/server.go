package queue

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/facebookgo/grace/gracehttp"
	"github.com/garyburd/redigo/redis"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
)

func ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/create", createHandler)
	mux.HandleFunc("/delete", deleteHandler)
	mux.HandleFunc("/query", queryHandler)
	server := http.Server{Addr: addr, Handler: mux}
	return gracehttp.Serve(&server)
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
	var request createRequest
	if ok := decode(w, r, &request); !ok {
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
	err := createTask(task)
	if err != nil {
		log.WithError(err).Error("create task fail")
		w.WriteHeader(500)
		return
	}
	response := createResponse{ID: task.ID}
	write(w, response)
}

type deleteRequest struct {
	ID string `json:"id"`
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	var request deleteRequest
	if ok := decode(w, r, &request); !ok {
		return
	}
	if request.ID == "" {
		w.WriteHeader(400)
		return
	}
	err := deleteTask(request.ID)
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
	var request queryRequest
	if ok := decode(w, r, &request); !ok {
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
	write(w, response)
}

func decode(w http.ResponseWriter, r *http.Request, obj interface{}) bool {
	if r.Method != http.MethodPost {
		w.WriteHeader(400)
		return false
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithError(err).Error("io read from frontend fail")
		w.WriteHeader(500)
		return false
	}
	err = json.Unmarshal(data, obj)
	if err != nil {
		log.WithError(err).Error("json unmarshal fail")
		w.WriteHeader(400)
		return false
	}
	return true
}

func write(w http.ResponseWriter, obj interface{}) {
	respData, err := json.Marshal(obj)
	if err != nil {
		log.WithError(err).Error("json marshal fail")
		w.WriteHeader(500)
		return
	}
	w.Write(respData)
}
