package main

import (
	"fmt"
	"imooc-rabbitmq/RabbitMQ"
)

func main() {
	rabbitmq:=RabbitMQ.NewRabbitMQSimple("imoocSimple")
	rabbitmq.PublishSimple("hello imooc!1")
	rabbitmq.PublishSimple("hello imooc!2")
	rabbitmq.PublishSimple("hello imooc!3")
	fmt.Println("发送成功")

}
