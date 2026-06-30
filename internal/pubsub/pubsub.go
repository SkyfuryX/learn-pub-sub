package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ch.PublishWithContext(context.Background(), //context
		exchange, //exchange
		key,      //key
		false,    //mandatory
		false,    // immediate
		amqp.Publishing{ //msg
			ContentType: "application/json",
			Body:        bytes,
		})
	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	err := gob.NewEncoder(&buffer).Encode(&val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), //context
		exchange, //exchange
		key,      //key
		false,    //mandatory
		false,    // immediate
		amqp.Publishing{ //msg
			ContentType: "application/gob",
			Body:        buffer.Bytes(),
		})
	if err != nil {
		return err
	}

	return nil
}

func PublishGameLog(gs *gamelogic.GameState, channel *amqp.Channel, msg string) error {
	if err := PublishGob(
		channel,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%v.%v", routing.GameLogSlug, gs.GetUsername()), //GameLogSlug.username
		routing.GameLog{
			CurrentTime: time.Now(),
			Message:     msg,
			Username:    gs.GetUsername(),
		}); err != nil {
		return err
	}

	return nil
}

type SimpleQueueType string

const (
	DurableQueue   SimpleQueueType = "durable"
	TransientQueue SimpleQueueType = "transient"
)

type AckType int

const (
	Ack         AckType = iota // 0
	NackRequeue                // 1
	NackDiscard                // 2
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
	dlxTable := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}
	switch queueType {
	case "durable":
		newQueue, err = newCh.QueueDeclare(queueName, true, false, false, false, dlxTable)
	case "transient":
		newQueue, err = newCh.QueueDeclare(queueName, false, true, true, false, dlxTable)
	}
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	newCh.QueueBind(queueName, key, exchange, false, nil)
	return newCh, newQueue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	channel, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		for msg := range deliveryChan {
			var body T
			err = json.Unmarshal(msg.Body, &body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			acktype := handler(body)
			switch acktype {
			case Ack: //Ack
				msg.Ack(false)
			case NackRequeue: //NackRequeue
				msg.Nack(false, true)
			case NackDiscard: //NackDiscard
				msg.Nack(false, false)
			}

		}
	}()

	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
) error {
	channel, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	deliveryChan, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		for msg := range deliveryChan {
			buffer := bytes.NewBuffer(msg.Body)
			var body T
			err := gob.NewDecoder(buffer).Decode(&body)
			if err != nil {
				fmt.Printf("could not decode gob: %v\n", err)
				continue
			}
			acktype := handler(body)
			switch acktype {
			case Ack: //Ack
				msg.Ack(false)
			case NackRequeue: //NackRequeue
				msg.Nack(false, true)
			case NackDiscard: //NackDiscard
				msg.Nack(false, false)
			}

		}
	}()

	return nil
}
