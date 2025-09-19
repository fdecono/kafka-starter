package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type ClickEvent struct {
	User string `json:"user"`
	Url  string `json:"url"`
	Ts   string `json:"ts"`
}

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "clicks",
		GroupID:  "clicks-analytics",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	clickCounts := make(map[string]int)

	fmt.Println("Listening for clicks events...")

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		message, err := reader.ReadMessage(ctx)
		cancel()

		if err != nil {
			// log.Printf("Error reading message: %v", err)
			continue
		}

		var event ClickEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			continue
		}

		clickCounts[event.User]++

		fmt.Printf("[%s]User %s clicked %s (%d total clicks)\n", time.Now().Format("15:04:05.000"), event.User, event.Url, clickCounts[event.User])
	}
}
