package session

import (
	"log"
	"sync"
)

var s *RabbitSession
var lock = &sync.Mutex{}

func GetInstance(queueName string, rabbitUrl string) (*RabbitSession, error) {
	if s == nil {
		lock.Lock()
		defer lock.Unlock()
		if s == nil {
			log.Println("Creating session")
			s = New(queueName, rabbitUrl)
		}
	}

	return s, nil
}
