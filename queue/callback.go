package queue

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

var HttpClient = &http.Client{
	Timeout: time.Second * 3,
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 10,
		MaxIdleConns:        1024,
		IdleConnTimeout:     time.Minute * 5,
	},
}

type callbackRequest struct {
	ID      string `json:"id"`
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

type callbackResponse struct {
	Code int `json:"code"`
}

const (
	CodeSuccess        = 100
	CodeTooManyRequest = 101
)

func post(task *Task) (int, error) {
	request := callbackRequest{
		ID:      task.ID,
		Topic:   task.Topic,
		Content: task.Content,
	}
	data, err := json.Marshal(request)
	if err != nil {
		log.WithError(err).Error("json marshal fail")
		return 0, err
	}

	content := bytes.NewBuffer(data)
	resp, err := HttpClient.Post(task.Callback, "application/json", content)
	if err != nil {
		log.WithError(err).Error("http post fail")
		return 0, err
	}
	defer resp.Body.Close()

	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithError(err).Error("io read from backend fail")
		return 0, err
	}
	var response callbackResponse
	err = json.Unmarshal(result, &response)
	if err != nil {
		log.WithError(err).Error("json unmarshal fail")
		return 0, err
	}
	return response.Code, nil
}
