package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/manueljishi/go-rabbitmq/session"
)

// Here we set the way error messages are displayed in the terminal.
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

const (
	name = "job_queue"
	addr = "amqp://guest:guest@localhost:5672/"
)

func main() {
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)

		i := i

		go func() {
			defer wg.Done()
			socketConnection(i)
		}()

	}
	wg.Wait()
}

func socketConnection(id int) {
	log.Printf("Routine %d\n", id)
	s, err := session.GetInstance(name, addr)
	failOnError(err, "Failed to init session")
	for i := 0; i < 100; i++ {
		message := fmt.Sprintf("Message from thread %d number %d", id, i)
		err := s.ThreadPush([]byte(message))

		if err != nil {
			continue
		} else {
			log.Printf("Sent message from %d", id)
		}
	}
}
