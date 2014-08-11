// This example declares a durable Exchange, an ephemeral (auto-delete) Queue,
// binds the Queue to the Exchange with a binding key, and consumes every
// message published to that Exchange with that routing key.
//
package main

import (
    "flag"
    "fmt"
    "github.com/streadway/amqp"
    "log"
)

func init() {
    flag.Parse()
}

func main() {
    c, err := NewConsumer("amqp://admin:password@localhost:5672/", "queue")
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

    //watching connection end
    go func() {
        fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
    }()

    log.Printf("got Connection, getting Channel")
    c.channel, err = c.conn.Channel()
    if err != nil {
        return nil, fmt.Errorf("Channel: %s", err)
    }

    log.Printf("declaring Queue %q", queueName)
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
        log.Printf(
            "%q",
            d.Body,
        )
        d.Ack(false)
    }
    log.Printf("handle: deliveries channel closed")
    done <- nil
}
