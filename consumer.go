package amqp_abstraction

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Consumer struct {
	queueProperties *QueueProperties
	name            string
	channel         *amqp.Channel
}

func CreateConsumer(queueProperties *QueueProperties, consumerName string, channel *amqp.Channel) *Consumer {
	return &Consumer{
		queueProperties: queueProperties,
		name:            consumerName,
		channel:         channel,
	}
}

func (consumer *Consumer) ConsumeQueue() (<-chan amqp.Delivery, error) {
	log.Printf("%s Consumer Started", consumer.name)

	if messages, err := consumer.channel.Consume(
		consumer.queueProperties.QueueName,
		consumer.name,
		false,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, err
	} else {
		return messages, nil
	}
}

func getCurrentRetriesIncrementing(delivery *amqp.Delivery) int32 {
	retriesObj := delivery.Headers[RetriesHeader]
	if retries, ok := retriesObj.(int32); !ok {
		fmt.Printf("wtf %d\n", retries)
		delivery.Headers[RetriesHeader] = int32(1)
		return 0
	} else {
		newRetries := retries+1
		delivery.Headers[RetriesHeader] = newRetries
		return retries
	}
}

func (consumer *Consumer) nackMessage(delivery *amqp.Delivery, err error) {
	shouldRequeue := !delivery.Redelivered
	log.Printf("%s: Failed Processing Message, first retry: %t", err, shouldRequeue)
	err = delivery.Reject(shouldRequeue)
	if err != nil {
		log.Printf("Failed Nacking: %s", err)
	}
}

func (consumer *Consumer) ConsumeMessage(
	delivery *amqp.Delivery,
	handle func(body []byte) ([]QueueEvent, error),
) {
	createdEvents, err := handle(delivery.Body)
	if err != nil {
		consumer.nackMessage(delivery, err)
		return
	}
	for _, event := range createdEvents {
		err = SendMessage(consumer.channel, event)
		if err != nil {
			consumer.nackMessage(delivery, err)
			return
		}
	}
	err = delivery.Ack(false)
	if err != nil {
		log.Printf("Failed Acking: %s", err)
	}
}
