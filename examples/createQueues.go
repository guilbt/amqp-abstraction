package examples

import (
	"fmt"
	amqp_abstraction "github.com/guilbt/amqp-abstraction"
	"github.com/streadway/amqp"
	"log"
)

func createQueueWithDLQFailingOnError(ch *amqp.Channel, properties *amqp_abstraction.QueueProperties) {
	err := amqp_abstraction.CreateQueueWithDLQ(ch, properties)
	ExitApplicationOnError(err, fmt.Sprintf("Error Creating Queue: %s", properties.QueueName))
}

func createQueues() {
	log.Println("Started Creating Necessary Queues...")

	rabbitCh, err := RabbitMQConnect()
	ExitApplicationOnError(err, "Error Fetching Rabbit Connection")

	createQueueWithDLQFailingOnError(rabbitCh, &amqp_abstraction.QueueProperties{QueueName: "queue_name"})
	createQueueWithDLQFailingOnError(rabbitCh, &amqp_abstraction.QueueProperties{QueueName: "second_queue_name"})

	log.Println("Finished Queues Creation")

	err = rabbitCh.Close()
	ExitApplicationOnError(err, "Failed Closing RabbitMQ Channel")
}
