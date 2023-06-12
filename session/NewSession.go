package session

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	exitCh         = make(chan struct{})
	confirmsCh     = make(chan *amqp.DeferredConfirmation)
	confirmsDoneCh = make(chan struct{})
	publishOkCh    = make(chan struct{}, 1)
)

func init() {
	flag.Parse()
}

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

	startConfirmHandler(publishOkCh, confirmsCh, confirmsDoneCh, exitCh)

	session := Session{
		name:           queueName,
		exitCh:         exitCh,
		confirmsCh:     confirmsCh,
		confirmsDoneCh: confirmsDoneCh,
		publishOkCh:    publishOkCh,
		channel:        createConnection(addr, queueName),
		queueName:      queueName,
	}
	setupCloseHandler(exitCh)
	return &session
}

// publish(context.Background(), publishOkCh, confirmsCh, confirmsDoneCh, exitCh)

func createConnection(addr string, queueName string) *amqp.Channel {
	config := amqp.Config{
		Vhost:      "/",
		Properties: amqp.NewConnectionProperties(),
	}
	config.Properties.SetClientConnectionName("producer-with-confirms")

	log.Printf("producer: dialing %s", addr)
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

	// canPublish := false
	// for {
	// 	select {
	// 	case <-publishOkCh:
	// 		log.Println("producer: got the OK to publish")
	// 		canPublish = true
	// 	case <-time.After(time.Second):
	// 		continue
	// 	}
	// 	if canPublish {
	// 		break
	// 	}
	// }

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
	}

	// select {
	// case <-confirmsDoneCh:
	// 	log.Println("producer: stopping, all confirms seen")
	// 	return errors.New("closed client")
	// case confirmsCh <- dConfirmation:
	// 	log.Println("producer: delivered deferred confirm to handler")
	// }
	return nil

}

func startConfirmHandler(publishOkCh chan<- struct{}, confirmsCh <-chan *amqp.DeferredConfirmation, confirmsDoneCh chan struct{}, exitCh <-chan struct{}) {
	log.Println("Start confirmation handler")
	go func() {
		confirms := make(map[uint64]*amqp.DeferredConfirmation)

		for {
			select {
			case <-exitCh:
				exitConfirmHandler(confirms, confirmsDoneCh)
				return
			default:
				break
			}

			outstandingConfirmationCount := len(confirms)

			// Note: 8 is arbitrary, you may wish to allow more outstanding confirms before blocking publish
			if outstandingConfirmationCount <= 1000 {
				select {
				case publishOkCh <- struct{}{}:
					log.Println("confirm handler: sent OK to publish")
				case <-time.After(time.Second * 5):
					log.Println("WARNING: confirm handler: timeout indicating OK to publish!!!!!!")
				}
			} else {
				log.Printf("confirm handler: waiting on %d outstanding confirmations, blocking publish", outstandingConfirmationCount)
			}

			select {
			case confirmation := <-confirmsCh:
				dtag := confirmation.DeliveryTag
				confirms[dtag] = confirmation
			case <-exitCh:
				exitConfirmHandler(confirms, confirmsDoneCh)
				return
			}

			checkConfirmations(confirms)
		}
	}()
}

func waitConfirmations(confirms map[uint64]*amqp.DeferredConfirmation) {
	log.Printf("confirm handler: waiting on %d outstanding confirmations", len(confirms))

	checkConfirmations(confirms)
	log.Printf("%d messages to wait for", len(confirms))

	for k, v := range confirms {
		select {
		case <-v.Done():
			log.Printf("confirm handler: confirmed delivery with tag: %d", k)
			delete(confirms, k)
		case <-time.After(time.Second):
			log.Printf("confirm handler: did not receive confirmation for tag %d", k)
		}
	}

	outstandingConfirmationCount := len(confirms)
	if outstandingConfirmationCount > 0 {
		log.Printf("confirm handler: exiting with %d outstanding confirmations", outstandingConfirmationCount)
	} else {
		log.Println("confirm handler: done waiting on outstanding confirmations")
	}
}

func checkConfirmations(confirms map[uint64]*amqp.DeferredConfirmation) {
	log.Printf("confirm handler: checking %d outstanding confirmations", len(confirms))

	for k, v := range confirms {
		if v.Acked() {
			log.Println("Deleting ")
			delete(confirms, k)
		}
	}
}

func exitConfirmHandler(confirms map[uint64]*amqp.DeferredConfirmation, confirmsDoneCh chan struct{}) {
	log.Println("confirm handler: exit requested")
	waitConfirmations(confirms)
	close(confirmsDoneCh)
	log.Println("confirm handler: exiting")
}

func setupCloseHandler(exitCh chan struct{}) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("close handler: Ctrl+C pressed in Terminal")
		close(exitCh)
	}()
}
