package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

// Registration model
type registration struct {
	ID    string
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

	const maxActiveCount = 5

	client.SetNX("load-lock:active-count", "0", 0)

	for {
		time.Sleep(3000)

		fmt.Println("move to processing")
		moveRegistrationsToProcessing(client)

		fmt.Println("process registrations")
		processRegistrations(client)

		processReleases(client)

		fmt.Println("check active")
		activeCount, activeCountErr := client.Get("load-lock:active-count").Result()
		if activeCountErr != nil && activeCountErr != redis.Nil {
			fmt.Printf("Error checking number of active jobs [%s]", activeCountErr)
			continue
		}

		activeCountInt, _ := strconv.ParseInt(activeCount, 10, 32)
		if activeCountInt < maxActiveCount {
			client.Incr("load-lock:active-count")

			fmt.Println("active good! lets do something")
			selectJobAndUnlock(client)
		} else {
			fmt.Printf("There are already [%d] jobs in progress. Will not start another one", activeCountInt)
		}
	}
}

func moveRegistrationsToProcessing(client *redis.Client) {
	_, err := client.BRPopLPush("load-lock:registration-queue", "load-lock:registration-queue:processing", time.Second).Result()

	if err != nil && err != redis.Nil {
		fmt.Printf("Error pushing registration to processing queue [%s]\n", err.Error())
	}

	fmt.Println("finished moving to processing!")
}

func processRegistrations(client *redis.Client) {
	msg, err := client.BRPopLPush("load-lock:registration-queue:processing", "load-lock:registration-queue:processing", time.Second).Result()

	if err != nil && err != redis.Nil {
		fmt.Printf("Error getting value from registraiont processing queue [%s]\n", err.Error())
		return
	}

	if msg == "" {
		fmt.Println("No messages")
		return
	}

	fmt.Printf("Routing msg [%s]\n", msg)

	reg := registration{}
	unmarshalErr := json.Unmarshal([]byte(msg), &reg)
	if unmarshalErr != nil {
		fmt.Printf("Failed to unmarshal lovely message [%s]", unmarshalErr)
		return
	}

	// Lock on group name so that duplicates aren't added to load-lock:groups-queue.
	numAdded, groupsSetErr := client.SAdd("load-lock:groups-set", reg.Group).Result()
	if groupsSetErr != nil && groupsSetErr != redis.Nil {
		fmt.Printf("Failed to add to groups set [%s]\n", groupsSetErr.Error())
		return
	}

	fmt.Printf("Routing sub [%v]\n", reg)

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

	fmt.Println("finished routing!")
}

func selectJobAndUnlock(client *redis.Client) {
	var selectedGroupQueue string

	stopAfter, _ := client.LLen("load-lock:groups-queue").Result()
	stopCounter := int64(0)

	fmt.Printf("There are [%d] groups in the groups-queue\n", stopAfter)

	for {
		if stopCounter >= stopAfter {
			fmt.Println("Failed to find anything useful. Dying sadly")
			return
		}

		nextGroupQueue, nextGroupQueueErr := client.BRPopLPush("load-lock:groups-queue", "load-lock:groups-queue", time.Second).Result()
		if nextGroupQueueErr != nil && nextGroupQueueErr != redis.Nil {
			return
		}

		addToActiveCount, addToActiveCountErr := client.SAdd("load-lock:active-groups-set", nextGroupQueue).Result()
		if addToActiveCountErr != nil && addToActiveCountErr != redis.Nil {
			return
		}

		if addToActiveCount == 1 {
			selectedGroupQueue = nextGroupQueue
			break
		}

		stopCounter++
	}

	if selectedGroupQueue == "" {
		fmt.Println("There were no candidates for selected group")
		return
	}

	selectedGroupProcessingQueue := fmt.Sprintf("%s:processing", selectedGroupQueue)
	msg, err := client.BRPopLPush(selectedGroupQueue, selectedGroupProcessingQueue, time.Second).Result()
	if err != nil {
		log.Printf("Failed to move group [%s] between queues. Error was [%s]\n", selectedGroupQueue, err)
		return
	}

	fmt.Printf("selectedGroupProcessingQueue: %s\n", selectedGroupProcessingQueue)

	reg := registration{}
	json.Unmarshal([]byte(msg), &reg)

	subName := fmt.Sprintf("load-lock:start:%s", reg.ID)

	fmt.Printf("Publishing message to subname [%s]\n", subName)

	client.Publish(subName, "empty-message").Result()

	client.LRem(selectedGroupProcessingQueue, 1, msg)

	fmt.Println("finished unlocking job!")
}

func processReleases(client *redis.Client) {
	msg, err := client.BRPopLPush("load-lock:release-queue", "load-lock:release-queue:processing", time.Second).Result()

	if err != nil && err != redis.Nil {
		return
	}

	if msg == "" {
		fmt.Println("Nothing on the release queue")
		return
	}

	reg := registration{}
	json.Unmarshal([]byte(msg), &reg)

	groupQueueName := fmt.Sprintf("load-lock:group-queue:%s", reg.Group)

	activeRemoved, activeRemovedErr := client.SRem("load-lock:active-groups-set", groupQueueName).Result()

	if activeRemovedErr != nil && activeRemovedErr != redis.Nil {
		return
	}

	if activeRemoved == 1 {
		client.Decr("load-lock:active-count")
	}

	client.LRem("load-lock:release-queue:processing", 1, msg)
}
