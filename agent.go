package main

import (
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := client.Ping().Result()
	if err != nil {
		fmt.Println("Failed to ping redis server")
		os.Exit(1)
	}

	fmt.Println("Agent starting, press Ctrl+C to stop!")

	for {
		moveRegistrationsToProcessing(client)
		time.Sleep(100)
	}
}

func moveRegistrationsToProcessing(client *redis.Client) {
	_, err := client.BRPopLPush("load-lock:registration-queue", "load-lock:registration-queue:processing", time.Second).Result()

	if err != nil && err != redis.Nil {
		fmt.Printf("Error pushing registration to processing queue [%s]\n", err.Error())
	}
}
