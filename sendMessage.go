package main

import (
	"log"

	"github.com/manueljishi/go-rabbitmq/session"
)

// Here we set the way error messages are displayed in the terminal.
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	name := "job_queue"
	addr := "amqp://guest:guest@localhost:5672/"
	s := session.New(name, addr)
	// Here we connect to RabbitMQ or send a message if there are any errors connecting.

	defer s.Close()
	for {
		if s.IsSessionReady() {
			break
		}
	}
	// We set the payload for the message.
	body := "Golang is awesome - Keep Moving Forward!"
	err := s.Push([]byte(body))
	failOnError(err, "Failed to push message to queue")
	log.Printf(" [x] Congrats, sending message: %s", body)
}
