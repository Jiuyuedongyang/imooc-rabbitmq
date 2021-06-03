package main

import "imooc-rabbitmq/RabbitMQ"

func main() {
	imoocTwo:=RabbitMQ.NewRabbitMQRouting("exImooc","imooc_two")
	imoocTwo.RecieveRouting()
}
