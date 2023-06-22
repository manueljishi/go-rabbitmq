//TODO: checkear el notifyclose del channel, mucho mas sencillo y seguro

package session

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	exitCh               = make(chan struct{})
	hasErrorChan         = make(chan bool)
	publishOkCh          = make(chan bool, 1)
	maxReconnectAttempts = 5
	connectionTimeout    = 10 * time.Second
)

type Session struct {
	name         string
	exitCh       chan struct{}
	hasErrorChan chan bool
	publishOkCh  chan bool
	channel      *amqp.Channel
	queueName    string
	isErrored    bool
}

func New(queueName string, addr string) *Session {

	session := Session{
		name:         queueName,
		exitCh:       exitCh,
		hasErrorChan: hasErrorChan,
		publishOkCh:  publishOkCh,
		channel:      createConnection(addr, queueName),
		queueName:    queueName,
		isErrored:    false,
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
		log.Fatalf("producer: error in dial, could not recover: %s", err)
	}

	log.Println("producer: got Connection, getting Channel")
	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("error getting a channel: %s", err)
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
		log.Fatalf("producer: Exchange Declare: %s", err)
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
		log.Fatalf("producer: Queue Declare: %s", err)
	}

	log.Printf("producer: declaring binding")
	if err := channel.QueueBind(queue.Name, queueName, queueName, false, nil); err != nil {
		log.Fatalf("producer: Queue Bind: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	log.Printf("producer: enabling publisher confirms.")
	if err := channel.Confirm(false); err != nil {
		log.Fatalf("producer: channel could not be put into confirm mode: %s", err)
	}

	return channel
}

func (s *Session) Publish(data []byte) error {

	if s.isErrored {
		log.Println("producer: An error was detected, trying to reconnect")
		select {
		case <-publishOkCh:
			s.isErrored = false
			break
		case <-time.After(connectionTimeout):
			log.Fatal("Timeout to reconnect exceeded, exiting application")
		}
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
		s.isErrored = true
		s.hasErrorChan <- true

	}

	return nil

}

func (s *Session) reconnectToChannel(address string, queueName string) {
	go func() {
		for {
			//receive an error, channel has errored, therefore we need to reconnect
			<-s.hasErrorChan
			couldConnect := false
			for i := 0; i < maxReconnectAttempts; i++ {
				channel := createConnection(address, queueName)
				if channel != nil {
					couldConnect = true
					s.channel = channel
					s.publishOkCh <- true
					log.Println("RABBIT SESSION: reconnected, resuming publish activity")
					break
				}
				time.Sleep(time.Second)
			}
			if !couldConnect {
				log.Fatal("RABBIT SESSION: Failed to reconnect after max number of attempts")
			}
		}
	}()
}

//This method is not implemented yed

// func (s *Session) subscribeNotifier() {
// 	unackedMsgs := int32(0)
// 	go func() {
// 		confirmsCh := make(chan amqp.Confirmation)
// 		confirmsCh = s.channel.NotifyPublish(confirmsCh)
// 		for {
// 			msg := <-confirmsCh
// 			if !msg.Ack {
// 				log.Println(msg)
// 				atomic.AddInt32(&unackedMsgs, 1)
// 			}
// 		}
// 	}()
// 	go func() {
// 		for {
// 			<-time.After(5 * time.Second)
// 			log.Printf("There are %d unacked msgs", atomic.LoadInt32(&unackedMsgs))
// 		}
// 	}()
// }
