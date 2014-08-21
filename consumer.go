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
	"strconv"
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

func main() {
	//connect to couchbase to access data
	//to use connection, use var couchConn
	createCouchBaseConn(*couchbaseURI)

	//connect to rabbitmq to get request
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

type Couch struct {
	conn *couchbase.Client
	pool *couchbase.Pool
}

//variable to store couchbase connection
var couchConn Couch

//function return couchBaseConnection
func createCouchBaseConn(couchbaseURI string) error {
	log.Printf("connecting to %q for couchbase", couchbaseURI)
	conn, err := couchbase.Connect(couchbaseURI)
	couchConn.conn = &conn
	if err != nil {
		return fmt.Errorf("Error getting connection: %s", err)
	}

	pool, err := couchConn.conn.GetPool("default")
	couchConn.pool = &pool
	if err != nil {
		return fmt.Errorf("Error getting pool:  %s", err)
	}

	return nil
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
			//newThread(d.Body, true)
		case `newThread_textOnly`:
			newThread(d.Body, false)

		default:
			log.Printf("unknown actionType")
		}i

		d.Ack(false)
	}

	log.Printf("handle: deliveries channel closed")
	done <- nil
}

func insertCouchbase() {

}

func updateCouchbase() {

}

func increaseThreadNum() string {
	bucket, err := couchConn.pool.GetBucket("Thread")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	threadNum, err := bucket.Incr("ThreadNum", 1, 1, 0)
	if err != nil {
		log.Fatalf("Failed to get data from the cluster (%s)\n", err)
	}

	return strconv.FormatUint(threadNum, 10)
}

func simpleThreadRequest(msg []byte) {
	type Request struct {
		Thread_id string `json:"thread_id"`
		User      string `json:"user"`
		Action    string `json:"action"`
		Time      int64  `json:"time"`
	}

	type Thread struct {
		Id      string
		Author  string   `json:"author"`
		Public  string   `json:"is_public"`
		Content string   `json:"content"`
		Photo   string   `json:"photh"`
		Time    int64    `json:"time"`
		Like    []string `json:"like"`
		Report  []string `json:"reportUser"`
		Block   []string `json:"blockUser"`
	}

	var request Request
	err := json.Unmarshal(msg, &request)
	if err != nil {
		log.Println("error:", err)
	}

	bucket, err := couchConn.pool.GetBucket("Thread")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	thread := Thread{}

	err = bucket.Get(request.Thread_id, &thread)
	if err != nil {
		log.Fatalf("Failed to get data from the cluster (%s)\n", err)
	}

	switch request.Action {
	case `threadLike`:
		thread.Like = append(thread.Like, request.User)
	case `threadUnlike`:
		for i, userName := range thread.Like {
			if userName == request.User {
				thread.Like = append(thread.Like[:i], thread.Like[i+1:]...)
				break
			}
		}
	case `threadReport`:
		thread.Report = append(thread.Report, request.User)
	case `threadBlock`:
		thread.Block = append(thread.Block, request.User)
	}

	//update change
	err = bucket.Set(thread.Id, 0, thread)
	if err != nil {
		log.Fatalf("Failed to write data to the cluster (%s)\n", err)
	}

	log.Println(request)
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

	log.Println(request)
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

	log.Println(request)
}

//auto increment가 필요하다.
func newThread(msg []byte, photo bool) {
	type Request struct {
		Id      string
		Author  string   `json:"author"`
		Public  string   `json:"is_public"`
		Content string   `json:"content"`
		Photo   string   `json:"photh"`
		Time    int64    `json:"time"`
		Like    []string `json:"like"`
		Report  []string `json:"reportUser"`
		Block   []string `json:"blockUser"`
	}

	var request Request
	err := json.Unmarshal(msg, &request)
	if err != nil {
		log.Println("error:", err)
	}

	log.Println(request)

	//fill request property
	request.Id = increaseThreadNum()
	if !photo {
		request.Photo = "nil"
	}

	bucket, err := couchConn.pool.GetBucket("Thread")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	added, err := bucket.Add(request.Id, 0, request)
	if err != nil {
		log.Fatalf("Failed to write data to the cluster (%s)\n", err)
	}

	if !added {
		log.Fatalf("A Document with the same id of (%s) already exists.\n", request.Id)
	}
}
