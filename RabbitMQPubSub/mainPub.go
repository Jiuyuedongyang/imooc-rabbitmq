package main

import (
	"fmt"
	"imooc-rabbitmq/RabbitMQ"
	"strconv"
	"time"
)

func main() {
	rabbitmq:=RabbitMQ.NewRabbitMQPubSub("newProduct")
	for i := 0; i < 100; i++ {
		rabbitmq.PublishPub("订阅模式生产者第"+strconv.Itoa(i)+"条数据")
		fmt.Println("订阅模式生产者第"+strconv.Itoa(i)+"条数据")
		time.Sleep(1*time.Second)

	}
}
