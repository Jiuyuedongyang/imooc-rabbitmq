package main

import "imooc-rabbitmq/RabbitMQ"

func main() {
	rabbitmq:=RabbitMQ.NewRabbitMQPubSub("newProduct")
	rabbitmq.RecieveSub()
}
