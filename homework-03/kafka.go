package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

const (
    KAFKA_URL_TOPIC_NAME = USERNAME + "-url-topic"
	KAFKA_CLICKS_TOPIC_NAME = USERNAME + "-clicks-topic"
)

var cntx = context.Background()

var kafkaUrlConsumer *kafka.Reader
var kafkaUrlProducer *kafka.Writer = initKafkaProducer(KAFKA_URL_TOPIC_NAME)

var kafkaClicksConsumer *kafka.Reader
var kafkaClicksProducer *kafka.Writer = initKafkaProducer(KAFKA_CLICKS_TOPIC_NAME)

// urls

func KafkaPushUrls(tinyurl string, longurl string) {
	err := kafkaUrlProducer.WriteMessages(cntx,
		kafka.Message{
			Key:   []byte(tinyurl),
			Value: []byte(longurl),
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("kafkaUrlProducer: pushed {" + tinyurl + ", " + longurl + "}")
}

func KafkaRunUrlConsumer(groupId string) {
	kafkaUrlConsumer = initKafkaConsumer(KAFKA_URL_TOPIC_NAME, groupId)
	for {
		message, err := kafkaUrlConsumer.FetchMessage(cntx)
		if err != nil {
			fmt.Println(err)
			continue
		}

		tinyurl, longurl := string(message.Key), string(message.Value)
		fmt.Println("KafkaUrlConsumer: read {" + tinyurl + ", " + longurl + "}")

		err = RedisSetUrls(tinyurl, longurl)
		if err != nil {
			panic(err)
		}
        fmt.Println("Redis: set " + tinyurl + " -> " + longurl)

		err = kafkaUrlConsumer.CommitMessages(cntx, message)
		if err != nil {
			panic(err)
		}
	}
}

// clicks

func KafkaPushClicks(tinyurl string, plusClicksNumber int64) {
    clicksString := fmt.Sprint(plusClicksNumber)
	err := kafkaClicksProducer.WriteMessages(cntx,
		kafka.Message{
			Key:   []byte(tinyurl),
			Value: []byte(clicksString),
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("kafkaClicksProducer: pushed {" + tinyurl + ", " + clicksString + "}")
}

func KafkaRunClicksConsumer(groupId string) {
	kafkaClicksConsumer = initKafkaConsumer(KAFKA_CLICKS_TOPIC_NAME, groupId)
	for {
		message, err := kafkaClicksConsumer.FetchMessage(cntx)
		if err != nil {
			fmt.Println(err)
			continue
		}

		tinyurl, plusClicksNumber := string(message.Key), string(message.Value)
		fmt.Println("kafkaClicksConsumer: read {" + tinyurl + ", " + plusClicksNumber + "}")

        err = SqlUpdateClicks(tinyurl, plusClicksNumber)
		if err != nil {
			panic(err)
		}
        fmt.Println("Sql: updated clicks on " + tinyurl + " by " + plusClicksNumber)

		err = kafkaClicksConsumer.CommitMessages(cntx, message)
		if err != nil {
			panic(err)
		}
	}
}

// inits

func initKafkaConsumer(topic string, groupId string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{KAFKA_HOST},
		GroupID:   groupId,
		Topic:     topic,
		Partition: 0,
	})
}

func initKafkaProducer(topic string) *kafka.Writer {
    return &kafka.Writer{
        Addr:     kafka.TCP(KAFKA_HOST),
        Topic:    topic,
        Balancer: &kafka.LeastBytes{},
    }
}
