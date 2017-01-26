package messages

import (
	"context"
	"log"

	"github.com/streadway/amqp"
)

// AmqpSession provides a composition of a connection
// and channel to be used in the self-reconnecting AMQP client.
type AmqpSession struct {
	*amqp.Connection
	*amqp.Channel
}

// Close deals with closing the connection of
// the provided session.
func (s AmqpSession) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

// AmqpMessage with headers and content type to work as an intermediary
// for application specific data structures.
type AmqpMessage struct {
	Headers     amqp.Table
	Body        []byte
	ContentType string
}

// Redial continuously connects to the message broker
// exiting when it can no longer make the connection.
func Redial(ctx context.Context, exchange string, url string) chan chan AmqpSession {
	sessions := make(chan chan AmqpSession)
	go func() {
		sess := make(chan AmqpSession)
		defer close(sessions)

		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				log.Println("Shutting down the session factory")
				return
			}

			conn, err := amqp.Dial(url)
			if err != nil {
				log.Fatalf("cannot (re)dial: %v: %q", err, url)
			}

			ch, err := conn.Channel()
			if err != nil {
				log.Fatalf("Cannot create channel: %v", err)
			}

			if err := ch.ExchangeDeclare(exchange, "fanout", false, true, false, false, nil); err != nil {
				log.Fatalf("Cannot declare fanout exchange: %v", err)
			}

			select {
			case sess <- AmqpSession{conn, ch}:
			case <-ctx.Done():
				log.Println("Shutting down new session")
				return
			}
		}
	}()
	return sessions
}
