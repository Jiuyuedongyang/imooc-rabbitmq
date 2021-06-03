package main

import "imooc-rabbitmq/RabbitMQ"

func main() {
	imoocOne:=RabbitMQ.NewRabbitMQTopic("exImoocTopic","#")
	imoocOne.RecieveTopic()
}
