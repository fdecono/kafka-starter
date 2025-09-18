package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "clicks",
	})
	defer writer.Close()

	users := []string{"Fefi", "Lionel", "Fideo"}
	urls := []string{"/goals", "/assists", "/trophies"}

	for i := 0; i < 20; i++ {
		event := fmt.Sprintf(`{"user":"%s", "url":"%s", "ts":"%s"}`,
			users[rand.Intn(len(users))],
			urls[rand.Intn(len(urls))],
			time.Now().Format(time.RFC3339),
		)

		err := writer.WriteMessages(context.Background(), kafka.Message{Value: []byte(event)})
		if err != nil {
			panic(err)
		}

		fmt.Printf("Sent event: %s\n", event)
		time.Sleep(time.Millisecond * 500)
	}
}
