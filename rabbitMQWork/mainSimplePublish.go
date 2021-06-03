package main

import (
	"fmt"
	"imooc-rabbitmq/RabbitMQ"
	"strconv"
	"time"
)

func main() {
	rabbitmq:=RabbitMQ.NewRabbitMQSimple("imoocSimple")
	for i := 0; i <=50; i++ {
		rabbitmq.PublishSimple("hello imooc! "+strconv.Itoa(i))
	time.Sleep(1*time.Second)
		fmt.Println(i)
	}
	fmt.Println("发送成功")

}
