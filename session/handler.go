package session

import (
	"errors"
	"log"
	"os"
	"sync"
)

var s *Session
var lock = &sync.Mutex{}
var queueName = os.Getenv("QUEUE")
var rabbitUrl = os.Getenv("RABBIT_URL")

func GetInstance() (*Session, error) {
	if s == nil {
		lock.Lock()
		defer lock.Unlock()
		if s == nil {
			if queueName == "" || rabbitUrl == "" {
				return nil, errors.New("Missing queue or  rabbit_url env variables")
			}
			log.Println("Creating session")
			s = New(queueName, rabbitUrl)
			ready := <-s.IsSessionReady
			log.Println("Session is ready? ", ready)
		}
	}
	return s, nil
}
