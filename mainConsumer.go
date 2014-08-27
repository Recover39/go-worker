package main

import (
	"./connectionHandler"
	"./requestHandler"
	"flag"
	"fmt"
	"log"
	//"database/sql"
	//_ "github.com/go-sql-driver/mysql"
)

func main() {
	//connect to couchbase to access data
	//to use connection, use var couchConn
	couchbaseConn, err := createCouchBaseConn(*couchbaseURI)

	//connect to rabbitmq to get request
	c, err := connectionHandler.CreateRabbitmqConsumer(*rabbitmqURI, *mainQueue, requestHandler.RouteRequest)
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
