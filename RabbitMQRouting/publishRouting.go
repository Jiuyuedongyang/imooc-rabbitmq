package main

import (
	"fmt"
	"imooc-rabbitmq/RabbitMQ"
	"strconv"
	"time"
)

func main() {
	imoocOne:=RabbitMQ.NewRabbitMQRouting("exImooc","imooc_one")
	imoocTwo:=RabbitMQ.NewRabbitMQRouting("exImooc","imooc_two")
	for i := 0; i < 10; i++ {
		imoocOne.PublishRouting("hello imooc one"+strconv.Itoa(i))
		imoocTwo.PublishRouting("hello imooc two"+strconv.Itoa(i))
		time.Sleep(1*time.Second)
		fmt.Println(i)
	}
}
