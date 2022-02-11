package examples

import (
	"encoding/json"
	"fmt"
	amqpabstraction "github.com/guilbt/amqp-abstraction"
)

type ExampleObj struct {
	ID              int32              `json:"id"`
	Prop1           string             `json:"prop1"`
	Prop2           string             `json:"prop2"`
}

type ExampleObj2 struct {
    Body            ExampleObj         `json:"body"`
}

func handle(messageBody []byte) (createdEvents []amqpabstraction.QueueEvent, err error) {
	exampleObj := ExampleObj{}
	if err = json.Unmarshal(messageBody, &exampleObj); err != nil {
		return nil, err
	}
	secondObjBody := &ExampleObj2{Body: exampleObj}
	secondQueue := &amqpabstraction.QueueProperties{QueueName: "second_queue_name"}
	createdEvents = []amqpabstraction.QueueEvent{{secondQueue, secondObjBody}}
	return createdEvents, nil
}

func main() {
	consumerName := "consumer-name"
	queue := &amqpabstraction.QueueProperties{QueueName: "queue_name"}

	rabbitCh, err := RabbitMQConnect()
	ExitApplicationOnError(err, fmt.Sprintf("Error Fetching Rabbit Connection for consumer %s", consumerName))

	consumer := amqpabstraction.CreateConsumer(queue, consumerName, rabbitCh)
	messages, err := consumer.ConsumeQueue()
	ExitApplicationOnError(err, fmt.Sprintf("Error starting consumer %s", consumerName))

	for message := range messages {
		go consumer.ConsumeMessage(&message, handle)
	}
}