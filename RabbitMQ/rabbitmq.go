package RabbitMQ

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

const MQURL = "amqp://guest:guest@113.31.146.174:5672/imooc"

type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	//队列名称
	QueueName string
	//交换机
	Exchange string
	//key
	Key string
	//连接信息
	Mqurl string
}

//创建RabbitMQ结构体实例
func NewRabbitMQ(queueName string, exchange string, key string) *RabbitMQ {
	rabbitmq := &RabbitMQ{
		QueueName: queueName,
		Exchange:  exchange,
		Key:       key,
		Mqurl:     MQURL,
	}
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "创建连接错误")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "获取channel失败")
	return rabbitmq
}

//断开channel和connection
func (r *RabbitMQ) Destory() {
	r.channel.Close()
	r.conn.Close()
}

//错误处理函数
func (r *RabbitMQ) failOnErr(err error, message string) {
	if err != nil {
		log.Fatalf("%s;%s", message, err)
		panic(fmt.Sprintf("%s;%s", message, err))
	}
}

//简单模式Step: 1.创建简单模式下RabbitMQ实例
func NewRabbitMQSimple(queueName string) *RabbitMQ {
	rabbitmq := NewRabbitMQ(queueName, "", "")

	return rabbitmq
}

//简单模式setp：2.简单模式下生产代码
func (r *RabbitMQ) PublishSimple(message string) {
	//申请队列 固定用法，如果队列不存在会自动创建，如果存在则跳过创建
	//好处：保证队列存在，消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		//控制我们消息是否持久化
		false,
		//是否自动删除
		false,
		//是否具有排他性（不常用，为true时候，就创建只有自己可见队列，其他用户🙅🏻‍♀️访问）
		false,
		//是否阻塞
		false,
		//额外属性
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}
	//2,发送消息到队列中
	r.channel.Publish(
		//默认交换机，默认是direct类型
		r.Exchange,
		r.QueueName,
		//如果为true会更具exchange和routekey规则，如果无法找到符合条件的队列，那么会把发送的消息回退给发送者
		false,
		//如果为true，当exchange发送消息到队列后发现队列上没有绑定消费者，则会把消息发还给发送者
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

func (r *RabbitMQ) ConsumeSimple() {
	//无论生产端还是消费端 都要申请队列
	//申请队列 固定用法，如果队列不存在会自动创建，如果存在则跳过创建
	//好处：保证队列存在，消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		//控制我们消息是否持久化
		false,
		//是否自动删除
		false,
		//是否具有排他性（不常用，为true时候，就创建只有自己可见队列，其他用户不可️访问）
		false,
		//是否阻塞
		false,
		//额外属性
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}
	//接受消息
	msgs, err := r.channel.Consume(
		r.QueueName,
		//用来区分多个消费者
		"",
		//是否自动应答，默认为true
		true,
		//是否具有排他性
		false,
		//如果设置为true表示不能将同一个connection中发送的消息传递给这个connection中的消费者
		false,
		//消息队列是否阻塞 false是阻塞
		false,
		nil)
	if err != nil {
		fmt.Println(err)
	}
	forever := make(chan bool)
	//启用协程处理消息
	go func() {
		for d := range msgs {
			//实现我们要处理的逻辑函数
			log.Printf("Received a message: %s : ", d.Body)
		}
		}()
	log.Printf("[*]waiting for messages,to exit press ctrl+c")

	<-forever
}
