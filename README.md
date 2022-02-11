# Golang - AMQP Abstractions

## Reasoning

Compared to what i'm used to work with in Java, i was having to write a lot to get a Consumer Service running in Golang, so i decided to create abstractions for Queues and Consumers :)

## Usage

You can look up examples of usage in /examples, but it's as simple as creating a handler function and running the Consumer:

```
type ExampleObj struct {
	ID              int32              `json:"id"`
	Prop1           string             `json:"prop1"`
	Prop2           string             `json:"prop2"`
}

type ExampleObj2 struct {
    Body            ExampleObj         `json:"body"`
}

func ExitApplicationOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func handle(messageBody []byte) (createdEvents []amqpAbs.QueueEvent, err error) {
	exampleObj := ExampleObj{}
	if err = json.Unmarshal(messageBody, &exampleObj); err != nil {
		return nil, err
	}
	secondObjBody := &ExampleObj2{Body: exampleObj}
	secondQueue := &amqpAbs.QueueProperties{QueueName: "second_queue_name"}
	createdEvents = []amqpAbs.QueueEvent{{secondQueue, secondObjBody}}
	return createdEvents, nil
}

func main() {
	consumerName := "consumer-name"
	queue := &amqpAbs.QueueProperties{QueueName: "queue_name"}

	rabbitCh, err := connectors.RabbitMQConnect()
	ExitApplicationOnError(err, fmt.Sprintf("Error Fetching Rabbit Connection for consumer %s", consumerName))

	consumer := amqpAbs.CreateConsumer(queue, consumerName, rabbitCh)
	messages, err := consumer.ConsumeQueue()
	ExitApplicationOnError(err, fmt.Sprintf("Error starting consumer %s", consumerName))

	for message := range messages {
		consumer.ConsumeMessage(&message, handle)
	}
}
 
```
