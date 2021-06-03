package main

import "imooc-rabbitmq/RabbitMQ"

func main() {
	imoocOne:=RabbitMQ.NewRabbitMQRouting("exImooc","imooc_one")
	imoocOne.RecieveRouting()
}

