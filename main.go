package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis"
)

// Registration model
type registration struct {
	Id    string
	Group string
}

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

		processRegistrations(client)

		time.Sleep(100)
	}
}

func moveRegistrationsToProcessing(client *redis.Client) {
	_, err := client.BRPopLPush("load-lock:registration-queue", "load-lock:registration-queue:processing", time.Second).Result()

	if err != nil && err != redis.Nil {
		fmt.Printf("Error pushing registration to processing queue [%s]\n", err.Error())
	}
}

func processRegistrations(client *redis.Client) {
	msg, err := client.BRPopLPush("load-lock:registration-queue:processing", "load-lock:registration-queue:processing", time.Second).Result()

	if err != nil && err != redis.Nil {
		fmt.Printf("Error getting value from registraiont processing queue [%s]\n", err.Error())
		return
	}

	reg := registration{}
	json.Unmarshal([]byte(msg), reg)

	// Lock on group name so that duplicates aren't added to load-lock:groups-queue.
	numAdded, groupsSetErr := client.SAdd("load-lock:groups-set", reg.Group).Result()
	if groupsSetErr != nil && groupsSetErr != redis.Nil {
		fmt.Printf("Failed to add to groups set [%s]\n", groupsSetErr.Error())
		return
	}

	// The name of the queue to route to.
	var groupQueueName = fmt.Sprintf("load-lock:group-queue:%s", reg.Group)

	_, routeErr := client.LPush(groupQueueName, msg).Result()
	if routeErr != nil && routeErr != redis.Nil {
		fmt.Printf("Failed to route registration to group queue [%s]. [%s]\n", groupQueueName, routeErr.Error())

		// Must ensure group is not locked!
		if numAdded == 1 {
			_, groupSetRemErr := client.SRem("load-lock:groups-set", groupQueueName).Result()

			if groupSetRemErr != nil && groupSetRemErr != redis.Nil {
				fmt.Printf("Failed to clean up groups set! Registrations for group [%s] will no longer be processed. [%s]\n", reg.Group, groupSetRemErr.Error())
			}
		}

		return
	}

	// List this group so that all routes can be processed.
	if numAdded == 1 {
		_, groupsQueueErr := client.LPush("load-lock:groups-queue", groupQueueName).Result()

		if groupsQueueErr != nil && groupsQueueErr != redis.Nil {
			fmt.Printf("Failed to add to groups queue [%s]\n", groupsQueueErr.Error())
			return
		}
	}

	client.LRem("load-lock:registration-queue:processing", 1, msg).Result()
}
