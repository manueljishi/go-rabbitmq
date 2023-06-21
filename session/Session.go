package session

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	exitCh         = make(chan struct{})
	confirmsCh     = make(chan *amqp.DeferredConfirmation)
	confirmsDoneCh = make(chan struct{})
	publishOkCh    = make(chan struct{}, 1)
)

type Session struct {
	name           string
	exitCh         chan struct{}
	confirmsCh     chan *amqp.DeferredConfirmation
	confirmsDoneCh chan struct{}
	publishOkCh    chan struct{}
	channel        *amqp.Channel
	queueName      string
}

func New(queueName string, addr string) *Session {

	session := Session{
		name:           queueName,
		exitCh:         exitCh,
		confirmsCh:     confirmsCh,
		confirmsDoneCh: confirmsDoneCh,
		publishOkCh:    publishOkCh,
		channel:        createConnection(addr, queueName),
		queueName:      queueName,
	}
	// session.subscribeNotifier()
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
		log.Fatalf("producer: error in dial: %s", err)
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
		log.Fatalf("producer: error in publish: %s", err)
		return err
	}

	return nil

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
