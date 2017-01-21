package messages

import (
	"crypto/sha1"
	"fmt"
	"log"
	"os"
)

// identity returns the same host/process unique string
// for the lifetime of this process so that subscriber
// reconnections reuse the same queue name.
func identity() string {
	hostname, err := os.Hostname()
	h := sha1.New()
	fmt.Fprint(h, hostname)
	fmt.Fprint(h, err)
	fmt.Fprint(h, os.Getpid())
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Subscribe deals with setting up a new listener to events
// published to a certain exchange.
func Subscribe(exchange string, sessions chan chan AmqpSession, messages chan<- AmqpMessage) {
	queue := identity()

	for session := range sessions {
		sub := <-session

		if _, err := sub.QueueDeclare(queue, false, true, true, false, nil); err != nil {
			log.Printf("Cannot consume from exclusive queue: %q, %v", queue, err)
			return
		}

		if err := sub.QueueBind(queue, "", exchange, false, nil); err != nil {
			log.Printf("Cannot consume without binding to exchange: %q, %v", exchange, err)
			return
		}

		deliveries, err := sub.Consume(queue, "", false, true, false, false, nil)
		if err != nil {
			log.Printf("Cannot consume from: %q, %v", queue, err)
			return
		}

		log.Printf("subscribed...")

		for msg := range deliveries {
			amqpMsg := AmqpMessage{}
			amqpMsg.Body = msg.Body
			amqpMsg.Headers = msg.Headers
			amqpMsg.ContentType = msg.ContentType
			messages <- amqpMsg
			sub.Ack(msg.DeliveryTag, false)
		}
	}
}
