package main

import (
    "fmt"
    "net"
    "strings"
    "time"

    "github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
    KAFKA_URL_TOPIC_NAME = USERNAME + "-url-topic"
)

var kafkaConfig kafka.ConfigMap = kafka.ConfigMap{
    "bootstrap.servers":  KAFKA_HOST,
    "group.id":           getGroutId(),
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": "false",
}

var kafkaProducer *kafka.Producer = func() *kafka.Producer {
    p, err := kafka.NewProducer(&kafkaConfig)
    if err != nil {
        panic(err)
    }

    return p
}()

var kafkaConsumer *kafka.Consumer = func() *kafka.Consumer {
    c, err := kafka.NewConsumer(&kafkaConfig)
    if err != nil {
        panic(err)
    }

    err = c.SubscribeTopics([]string{KAFKA_URL_TOPIC_NAME}, nil)
    if err != nil {
        panic(err)
    }

    return c
}()

func KafkaPushUrls(tinyurl string, longurl string) {
    topic := KAFKA_URL_TOPIC_NAME
    message := constructKafkaMessage(tinyurl, longurl)

    err := kafkaProducer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
        Value:          []byte(message),
    }, nil)
    if err != nil {
        panic(err)
    }
}

func KafkaRunConsumer() {
    for {
        message, err := kafkaConsumer.ReadMessage(time.Second)
        if err != nil {
            fmt.Println(err)
            continue
        }

        tinyurl, longurl := deconstructKafkaMessage(string(message.Value))
        err = RedisSetUrls(tinyurl, longurl)
        if err != nil {
            panic(err)
        }

        _, err = kafkaConsumer.Commit()
        if err != nil {
            panic(err)
        }
    }
}

func constructKafkaMessage(tinyurl string, longurl string) string {
    return tinyurl + "|" + longurl
}

func deconstructKafkaMessage(message string) (string, string) {
    splitted := strings.Split(message, "|")
    return splitted[0], splitted[1]
}

func getGroutId() string {
    conn, err := net.Dial("ip:icmp", "ya.ru")
    if err != nil {
        panic(err)
    }
    return conn.LocalAddr().String()
}
