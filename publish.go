package messages

import (
	"log"

	"github.com/streadway/amqp"
)

// Publish deals with setting up a process to handle
// publishing messages to an exchange.
func Publish(exchange string, sessions chan chan AmqpSession, messages <-chan AmqpMessage) {
	var (
		running bool
		reading = messages
		pending = make(chan AmqpMessage, 1)
		confirm = make(chan amqp.Confirmation, 1)
	)

	for session := range sessions {
		pub := <-session

		// Publisher confirms this channel/connection
		if err := pub.Confirm(false); err != nil {
			log.Printf("Publisher confirms not supported")
			// Confirms are not supported, simulate by ensuring we
			// provide a negative acknowledgement.
			close(confirm)
		}

		log.Printf("publishing...")

	Publish:
		for {
			var body AmqpMessage
			select {
			case confirmed := <-confirm:
				if !confirmed.Ack {
					log.Printf("nack message %d, body: %q", confirmed.DeliveryTag, string(body.Body))
				}
				reading = messages

			case body = <-pending:
				err := pub.Publish(exchange, "", false, false, amqp.Publishing{
					ContentType:  body.ContentType,
					Headers:      body.Headers,
					Body:         body.Body,
					DeliveryMode: amqp.Transient,
					Priority:     0,
				})
				// Retry failed delivery on the next session.
				if err != nil {
					pending <- body
					pub.Close()
					break Publish
				}

			case body, running = <-reading:
				// All messages have been consumed
				if !running {
					return
				}
				// Work on pending deliveries unit acknowledged.
				pending <- body
				reading = nil
			}
		}
	}
}
