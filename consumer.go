// This example declares a durable Exchange, an ephemeral (auto-delete) Queue,
// binds the Queue to the Exchange with a binding key, and consumes every
// message published to that Exchange with that routing key.
//
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	//"database/sql"
	//_ "github.com/go-sql-driver/mysql"
)

var (
	uri   = flag.String("uri", "amqp://admin:password@localhost:5672/", "AMQP URI")
	queue = flag.String("queue", "queue", "Ephemeral AMQP queue name")
)

func init() {
	flag.Parse()
}

func main() {
	c, err := NewConsumer(*uri, *queue)
	if err != nil {
		log.Fatalf("%s", err)
	}

	log.Printf("running forever")
	select {}

	log.Printf("shutting down")

	if err := c.Shutdown(); err != nil {
		log.Fatalf("error during shutdown: %s", err)
	}
}

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func NewConsumer(amqpURI, queueName string) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		channel: nil,
		done:    make(chan error),
	}

	var err error

	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Printf("declared Exchange, declaring Queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Printf("declared Queue : %q ", queue.Name)

	deliveries, err := c.channel.Consume(
		queue.Name, // name
		"",         // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	go handle(deliveries, c.done)

	return c, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		//check request actionType
		type ActionType struct {
			action string
		}

		var action ActionType
		err := json.Unmarshal(d.Body, &action)
		if err != nil {
			log.Println("error:", err)
		}

		//route request
		switch action {
		case `threadLike`, `threadUnlike`, `threadReport`, `threadBlock`:
			simpleThreadRequest(d.Body)
		case `commentAdd`:
			addComment(d.Body)
		case `commentLike`, `commentUnlike`, `commentReport`, `commentBlock`:
			simpleCommentRequest(d.Body)
		default:
			log.Printf("unknown actionType")
		}

		log.Printf("%s", d.Body)
	}

	log.Printf("handle: deliveries channel closed")
	done <- nil
}

func simpleThreadRequest(msg []byte) {
	type Request struct {
		Thread_id string `json:"thread_id"`
		User      string `json:"user"`
		Action    string `json:"action"`
		Time      int64  `json:"time"`
	}

	var request Request
	err := json.Unmarshal(msg, &request)
	if err != nil {
		log.Println("error:", err)
	}

	switch request.Action {
	case `threadLike`:
	case `threadUnlike`:
	case `threadReport`:
	case `threadBlock`:
	}

	fmt.Printf("%+v", request)

	// log.Printf("thread_id : %s, user : %s, action : %s",
	//     request.thread_id, request.user, request.action)
	// log.Printf("%v",request)
}

func addComment(msg []byte) {

}

func simpleCommentRequest(msg []byte) {
	type Request struct {
		Comment_id string `json:"comment_id"`
		User       string `json:"user"`
		Action     string `json:"action"`
		Time       int64  `json:"time"`
	}

	var request Request
	err := json.Unmarshal(msg, &request)
	if err != nil {
		log.Println("error:", err)
	}

	switch request.Action {
	case `commentLike`:
	case `commentUnlike`:
	case `commentReport`:
	case `commentBlock`:
	}
}
