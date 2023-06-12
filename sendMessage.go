package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/manueljishi/go-rabbitmq/session"
)

// Here we set the way error messages are displayed in the terminal.
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

const (
	name = "inversores"
	addr = "amqp://guest:guest@localhost:5672/"
)

func main() {
	t1 := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < 1; i++ {
		wg.Add(1)

		i := i

		go func() {
			defer wg.Done()
			socketConnection(i)
		}()

	}
	wg.Wait()
	log.Printf("Sending all messages took %f seconds", time.Now().Sub(t1).Seconds())
}

func socketConnection(id int) {
	log.Printf("Routine %d\n", id)
	s, err := session.GetInstance(name, addr)
	failOnError(err, "Failed to init session")
	for i := 0; i < 100; i++ {
		for j := 0; j < 150; j++ {
			message := fmt.Sprintf("Message from thread %d number %d", id, i)
			if err := s.Publish([]byte(message)); err != nil {
				break
			}
		}
		time.Sleep(time.Second)
	}
}
