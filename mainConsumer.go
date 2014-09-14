package main

import (
	"./connectionHandler"
	"./requestHandler"
	"flag"
	"log"
	//"database/sql"
	//_ "github.com/go-sql-driver/mysql"
)

var (
	rabbitmqURI = flag.String("uri", "amqp://recover39:recover@125.209.193.216:5672/", "AMQP URI")
	mainQueue   = flag.String("queue", "requestQueue", "main queue name")
	//couchbaseURI = flag.String("couchbase", "http://125.209.198.141:8091/", "couchbase URI")
)

func init() {
	flag.Parse()
}

func main() {
	//connect to couchbase to access data
	//to use connection, use var couchConn
	//couchbaseConn, err := connectionHandler.CreateCouchbaseConn(*couchbaseURI)

	//connect to rabbitmq to get request
	c, err := connectionHandler.CreateRabbitmqConsumer(*rabbitmqURI, *mainQueue, requestHandler.RouteRequest)
	if err != nil {
		log.Fatalf("%s", err)
	}

	log.Printf("running forever")
	select {}

	log.Printf("shutting down")

	if err := c.RabbitmqShutdown(); err != nil {
		log.Fatalf("error during shutdown: %s", err)
	}
}
