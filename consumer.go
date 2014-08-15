// This example declares a durable Exchange, an ephemeral (auto-delete) Queue,
// binds the Queue to the Exchange with a binding key, and consumes every
// message published to that Exchange with that routing key.
//
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/couchbaselabs/go-couchbase"
	"github.com/streadway/amqp"
	"log"
	//"database/sql"
	//_ "github.com/go-sql-driver/mysql"
)

var (
	rabbitmqURI  = flag.String("uri", "amqp://admin:password@localhost:5672/", "AMQP URI")
	mainQueue    = flag.String("queue", "requestQueue", "main queue name")
	couchbaseURI = flag.String("couchbase", "http://125.209.198.141:8091/", "couchbase URI")
)

func init() {
	flag.Parse()
}

type User struct {
	Name string `json:"name"`
	Id   string `json:"id"`
}

func main() {
	c, err := NewConsumer(*rabbitmqURI, *mainQueue)
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

	go deliverHandle(deliveries, c.done)

	return c, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func deliverHandle(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		//check request actionType
		type ActionType struct {
			Action string `json:"action"`
		}

		var actionType ActionType
		err := json.Unmarshal(d.Body, &actionType)
		if err != nil {
			log.Println("error:", err)
		}

		//route request
		switch actionType.Action {
		case `threadLike`, `threadUnlike`, `threadReport`, `threadBlock`:
			simpleThreadRequest(d.Body)

		case `commentAdd`:
			addComment(d.Body)

		case `commentLike`, `commentUnlike`, `commentReport`, `commentBlock`:
			simpleCommentRequest(d.Body)

		case `newThread`:
			newThread(d.Body)

		case `newThread_textOnly`:
			newThreadTextOnly(d.Body)

		default:
			log.Printf("unknown actionType")
		}

		d.Ack(false)
	}

	log.Printf("handle: deliveries channel closed")
	done <- nil
}

type Couch struct {
	conn *couchbase.Client
	pool *couchbase.Pool
}

//function return couchBaseConnection
func couchBaseConn(couchbaseURI string) (*Couch, error) {
	c := &Couch{
		conn: nil,
		pool: nil,
	}

	log.Printf("connecting to %q for couchbase", couchbaseURI)
	conn, err := couchbase.Connect(couchbaseURI)
	c.conn = &conn
	if err != nil {
		return nil, fmt.Errorf("Error getting connection: %s", err)
	}

	pool, err := c.conn.GetPool("default")
	c.pool = &pool
	if err != nil {
		return nil, fmt.Errorf("Error getting pool:  %s", err)
	}

	return c, nil
}

func insertCouchbase() {

}

func updateCouchbase() {

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

	log.Println("%+v", request)
}

func addComment(msg []byte) {
	type Request struct {
		Comment_id string `json:"comment_id"`
		User       string `json:"user"`
		Content    string `json:"content"`
		Time       int64  `json:"time"`
	}

	var request Request
	err := json.Unmarshal(msg, &request)
	if err != nil {
		log.Println("error:", err)
	}

	log.Println("%+v", request)
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

	log.Println("%+v", request)
}

func newThreadTextOnly(msg []byte) {
	type Request struct {
		Author  string `json:"auther"`
		Public  string `json:"is_public"`
		Content string `json:"content"`
		Time    int64  `json:"time"`
	}

	var request Request
	err := json.Unmarshal(msg, &request)
	if err != nil {
		log.Println("error:", err)
	}

	log.Println("%+v", request)
}

func newThread(msg []byte) {
	type Request struct {
		Author  string `json:"auther"`
		Public  string `json:"is_public"`
		Content string `json:"content"`
		Photo   string `json:"photh"`
		Time    int64  `json:"time"`
	}

	var request Request
	err := json.Unmarshal(msg, &request)
	if err != nil {
		log.Println("error:", err)
	}

	log.Println("%+v", request)
}
