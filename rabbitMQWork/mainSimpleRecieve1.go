package main

import (
	"fmt"
	"imooc-rabbitmq/RabbitMQ"
)

func main() {
	rabbitmq:=RabbitMQ.NewRabbitMQSimple("imoocSimple")
	rabbitmq.ConsumeSimple()
	fmt.Println("consumeSimple")
}
