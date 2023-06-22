package main

import (
	"fmt"
	"log"
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
	socketConnection(1)
	log.Printf("Sending 1500000 messages took %f seconds", time.Since(t1).Seconds())
}

func socketConnection(id int) {
	log.Printf("Routine %d\n", id)
	s, err := session.GetInstance(name, addr)
	failOnError(err, "Failed to init session")
	for i := 0; i < 100; i++ {
		log.Println("Sending messages")
		for j := 0; j < 100; j++ {
			message := fmt.Sprintf("Message from thread %d loop %d number %d", id, i, j)
			if err := s.Publish([]byte(message)); err != nil {
				break
			}
		}
		time.Sleep(time.Second * 5)
	}
}
