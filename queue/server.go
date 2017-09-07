package queue

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
)

type CreateTaskRequest struct {
	// Topic use to classify tasks
	Topic string `json:"topic"`
	// Delay is the number of seconds that should elapse before the task execute
	Delay int64 `json:"delay"`
	// Retry is max deliver retry times
	Retry int `json:"retry"`
	// Callback is the deliver address
	Callback string `json:"callback"`
	// Content is the task content to deliver
	Content string `json:"content"`
}

type CreateTaskResponse struct {
	ID string `json:"id"`
}

func ListenAndServe(addr string) error {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(400)
			return
		}
		var request CreateTaskRequest
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
		response := CreateTaskResponse{ID: task.ID}
		respData, err := json.Marshal(response)
		if err != nil {
			log.WithError(err).Error("json marshal fail")
			w.WriteHeader(500)
			return
		}
		w.Write(respData)
	})
	return http.ListenAndServe(addr, nil)
}
