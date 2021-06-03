package main

import (
	"fmt"
	"imooc-rabbitmq/RabbitMQ"
	"strconv"
	"time"
)

func main() {
	imoocOne:=RabbitMQ.NewRabbitMQTopic("exImoocTopic","imooc.topic..one")
	imoocTwo:=RabbitMQ.NewRabbitMQTopic("exImoocTopic","imooc.topic.two")
	for i := 0; i < 50; i++ {
		imoocOne.PublishTopic("HELLO imooc topic one! "+strconv.Itoa(i))
		imoocTwo.PublishTopic("HELLO imooc topic tow! "+strconv.Itoa(i))
		time.Sleep(1*time.Second)
		fmt.Println(i)

	}
}