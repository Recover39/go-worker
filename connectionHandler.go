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

type RabbitmqConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan error
}

type rabbitmqHandler func(deliveries <-chan amqp.Delivery, done chan error)

type Couch struct {
	conn *couchbase.Client
	pool *couchbase.Pool
}

//function return couchBaseConnection
func CreateCouchbaseConn(address string) (Couch, error) {
	couchConn := Couch{}

	log.Printf("connecting to %q for couchbase", address)
	conn, err := couchbase.Connect(address)
	if err != nil {
		return nil, fmt.Errorf("Error getting connection: %s", err)
	}

	pool, err := conn.GetPool("default")
	if err != nil {
		return nil, fmt.Errorf("Error getting pool:  %s", err)
	}

	couchConn.conn = &conn
	couchConn.pool = &pool

	return couchConn, nil
}

func CreateBucket(bucketname string) (*couchbase.Bucket, error) {
	conn, err := couchbase.Connect(couchbaseURI)
	if err != nil {
		//log.Println("Make sure that couchbase is at", couchbaseURI)
		return nil, fmt.Errorf("Error getting connection: %s", err)
	}

	pool, err := conn.GetPool("default")
	if err != nil {
		return nil, fmt.Errorf("Error getting pool:  %s", err)
	}

	bucket, err := pool.GetBucket(bucketname)
	if err != nil {
		return nil, fmt.Errorf("Failed to get bucket from couchbase (%s)\n", err)
	}

	return bucket, nil
}

//must run go deliverHandle(deliveries, c.done) after this function
func CreateRabbitmqConsumer(amqpURI, queueName string, deliverFunc rabbitmqHandler) (*RabbitmqConsumer, error) {
	c := &RabbitmqConsumer{
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

	go deliverFunc(deliveries, c.done)

	return c, nil
}

func (c *RabbitmqConsumer) RabbitmqShutdown() error {
	// will close() the deliveries channel
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}
