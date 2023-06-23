//TODO: checkear el notifyclose del channel, mucho mas sencillo y seguro

package session

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	publishOkCh             = make(chan bool, 1)
	maxReconnectAttempts    = 10
	reconnectAttemptTimeout = 2 * time.Second
)

type Session struct {
	name        string
	publishOkCh chan bool
	channel     *amqp.Channel
	queueName   string
	isErrored   atomic.Bool
}

func New(queueName string, addr string) *Session {

	session := Session{
		name:        queueName,
		publishOkCh: publishOkCh,
		channel:     createConnection(addr, queueName),
		queueName:   queueName,
		isErrored:   atomic.Bool{},
	}
	// session.subscribeNotifier()
	session.reconnectToChannel(addr, queueName)
	return &session
}

func createConnection(addr string, queueName string) *amqp.Channel {
	config := amqp.Config{
		Vhost:      "/",
		Properties: amqp.NewConnectionProperties(),
	}
	config.Properties.SetClientConnectionName("inversores")

	//do not disclose url and creds
	// log.Printf("producer: dialing %s", addr)
	log.Printf("producer: dialing connection to rabbitmq")
	conn, err := amqp.DialConfig(addr, config)
	if err != nil {
		log.Printf("producer: error in dial, could not recover: %s", err)
		return nil
	}

	log.Println("producer: got Connection, getting Channel")
	channel, err := conn.Channel()
	if err != nil {
		log.Printf("error getting a channel: %s", err)
		return nil
	}

	log.Printf("producer: declaring exchange")
	if err := channel.ExchangeDeclare(
		queueName, // name
		"direct",  // type
		true,      // durable
		false,     // auto-delete
		false,     // internal
		false,     // noWait
		nil,       // arguments
	); err != nil {
		log.Printf("producer: Exchange Declare: %s", err)
		return nil
	}

	log.Printf("producer: declaring queue '%s'", queueName)
	queue, err := channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err == nil {
		log.Printf("producer: declared queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
			queue.Name, queue.Messages, queue.Consumers, queueName)
	} else {
		log.Printf("producer: Queue Declare: %s", err)
		return nil
	}

	log.Printf("producer: declaring binding")
	if err := channel.QueueBind(queue.Name, queueName, queueName, false, nil); err != nil {
		log.Printf("producer: Queue Bind: %s", err)
		return nil
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	log.Printf("producer: enabling publisher confirms.")
	if err := channel.Confirm(false); err != nil {
		log.Printf("producer: channel could not be put into confirm mode: %s", err)
		return nil
	}

	return channel
}

func (s *Session) Publish(data []byte) error {

	if s.isErrored.Load() {
		<-publishOkCh
		s.isErrored.Store(false)
	}

	//Sequential access
	_, err := s.channel.PublishWithDeferredConfirmWithContext(
		context.Background(),
		s.queueName,
		s.name,
		true,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			DeliveryMode:    amqp.Persistent,
			Priority:        0,
			AppId:           s.name,
			Body:            data,
		},
	)
	if err != nil {
		log.Printf("producer: error in publish: %s, starting reconnect procedure\n ", err)
	}

	return nil

}

func (s *Session) reconnectToChannel(address string, queueName string) {
	errChan := make(chan *amqp.Error)
	s.channel.NotifyClose(errChan)
	go func() {
		for {
			//receive an error, channel has errored, therefore we need to reconnect
			//channel is closed so we need to reinstantiate
			<-errChan
			log.Println("producer: An error was detected, trying to reconnect")
			s.isErrored.Store(true)
			couldConnect := false
			for i := 0; i < maxReconnectAttempts; i++ {
				channel := createConnection(address, queueName)
				if channel != nil {
					couldConnect = true
					s.channel = channel
					s.publishOkCh <- true
					errChan = make(chan *amqp.Error)
					s.channel.NotifyClose(errChan)
					log.Println("RABBIT SESSION: reconnected, resuming publish activity")
					break
				}
				time.Sleep(reconnectAttemptTimeout)
			}
			if !couldConnect {
				log.Fatal("RABBIT SESSION: Failed to reconnect after max number of attempts")
			}
		}
	}()
}
