package amqp_abstraction

import (
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
		consumer.queueProperties.QueueName, // queue
		consumer.name,                      // consumer
		false,                              // auto-ack
		false,                              // exclusive
		false,                              // no-local
		false,                              // no-wait
		nil,                                // args
	); err != nil {
		return nil, err
	} else {
		return messages, nil
	}
}

func nackMessage(delivery *amqp.Delivery, err error) {
	log.Printf("%s: %s", "Failed Processing Message", err)
	err = delivery.Nack(false, false)
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
		nackMessage(delivery, err)
		return
	}
	for _, event := range createdEvents {
		err = SendMessage(consumer.channel, event)
		if err != nil {
			nackMessage(delivery, err)
			return
		}
	}
	err = delivery.Ack(false)
	if err != nil {
		log.Printf("Failed Acking: %s", err)
	}
}
