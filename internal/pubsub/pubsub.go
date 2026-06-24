package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "applicaltion/json",
		Body:        bytes,
	})
	return nil
}

type SimpleQueueType string

const (
	DurableQueue   SimpleQueueType = "durable"
	TransientQueue SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {

	newCh, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	newQueue := amqp.Queue{}
	switch queueType {
	case "durable":
		newQueue, err = newCh.QueueDeclare(queueName, true, false, false, false, nil)
	case "transient":
		newQueue, err = newCh.QueueDeclare(queueName, false, true, true, false, nil)
	}
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	newCh.QueueBind(queueName, key, exchange, false, nil)
	return newCh, newQueue, nil
}
