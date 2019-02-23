package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

const redisKeyActiveCount = "load-lock:active-count"

const redisKeyRegistrationQueue = "load-lock:registration-queue"
const redisKeyRegistrationProcessingQueue = "load-lock:registration-queue:processing"

const redisKeyGroupsSet = "load-lock:groups-set"
const redisKeyGroupsQueue = "load-lock:groups-queue"

const redisKeyReleaseQueue = "load-lock:release-queue"
const redisKeyReleaseProcessingQueue = "load-lock:release-queue:processing"

const maxActiveCount = 5

// Registration model
type registration struct {
	ID    string
	Group string
}

func main() {
	isClean := flag.Bool("clean", false, "Performs a clean of redis then shuts down")
	flag.Parse()

	client := getRedisClient()

	_, err := client.Ping().Result()
	if err != nil {
		fmt.Println("Failed to ping redis server")
		os.Exit(1)
	}

	if *isClean {
		cleanAgentData(client)
		fmt.Println("Finished cleaning. Exiting")
	} else {
		runAgent(client)
	}
}

func getRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func runAgent(client *redis.Client) {
	fmt.Println("Agent starting, press Ctrl+C to stop!")

	client.SetNX(redisKeyActiveCount, "0", 0)

	for {
		time.Sleep(3000)

		moveRegistrationsToProcessing(client)

		processRegistrations(client)

		processReleases(client)

		activeCount, activeCountErr := client.Get(redisKeyActiveCount).Result()
		if activeCountErr != nil && activeCountErr != redis.Nil {
			fmt.Printf("Error checking number of active jobs [%s]", activeCountErr)
			continue
		}

		activeCountInt, _ := strconv.ParseInt(activeCount, 10, 32)
		if activeCountInt < maxActiveCount {
			client.Incr(redisKeyActiveCount)
			unlockSucess, _ := selectJobAndUnlock(client)

			if !unlockSucess {
				client.Decr(redisKeyActiveCount)
			}
		} else {
			fmt.Printf("There are already [%d] jobs in progress. Will not start another one\n", activeCountInt)
		}
	}
}

func moveRegistrationsToProcessing(client *redis.Client) {
	_, err := client.BRPopLPush(redisKeyRegistrationQueue, redisKeyRegistrationProcessingQueue, time.Second).Result()

	if err != nil && err != redis.Nil {
		fmt.Printf("Error pushing registration to processing queue [%s]\n", err.Error())
	}
}

func processRegistrations(client *redis.Client) {
	msg, err := client.BRPopLPush(redisKeyRegistrationProcessingQueue, redisKeyRegistrationProcessingQueue, time.Second).Result()

	if err != nil && err != redis.Nil {
		fmt.Printf("Error getting value from registraiont processing queue [%s]\n", err.Error())
		return
	}

	if msg == "" {
		return
	}

	reg := registration{}
	unmarshalErr := json.Unmarshal([]byte(msg), &reg)
	if unmarshalErr != nil {
		fmt.Printf("Failed to unmarshal registration message [%s]. [%s]", msg, unmarshalErr)
		return
	}

	// Lock on group name so that duplicates aren't added to load-lock:groups-queue.
	numAdded, groupsSetErr := client.SAdd(redisKeyGroupsSet, reg.Group).Result()
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
			_, groupSetRemErr := client.SRem(redisKeyGroupsSet, groupQueueName).Result()

			if groupSetRemErr != nil && groupSetRemErr != redis.Nil {
				fmt.Printf("Failed to clean up groups set! Registrations for group [%s] will no longer be processed. [%s]\n", reg.Group, groupSetRemErr.Error())
			}
		}

		return
	}

	// List this group so that all routes can be processed.
	if numAdded == 1 {
		_, groupsQueueErr := client.LPush(redisKeyGroupsQueue, groupQueueName).Result()

		if groupsQueueErr != nil && groupsQueueErr != redis.Nil {
			fmt.Printf("Failed to add to groups queue [%s]\n", groupsQueueErr.Error())
			return
		}
	}

	client.LRem(redisKeyRegistrationProcessingQueue, 1, msg).Result()

	fmt.Println("finished routing!")
}

func selectJobAndUnlock(client *redis.Client) (success bool, err error) {
	var selectedGroupQueue string

	stopAfter, _ := client.LLen(redisKeyGroupsQueue).Result()
	stopCounter := int64(0)

	for {
		if stopCounter >= stopAfter {
			// No groups that we can start processing something from. Stop.
			return false, nil
		}

		nextGroupQueue, nextGroupQueueErr := client.BRPopLPush(redisKeyGroupsQueue, redisKeyGroupsQueue, time.Second).Result()
		if nextGroupQueueErr != nil && nextGroupQueueErr != redis.Nil {
			return false, nextGroupQueueErr
		}

		addToActiveCount, addToActiveCountErr := client.SAdd("load-lock:active-groups-set", nextGroupQueue).Result()
		if addToActiveCountErr != nil && addToActiveCountErr != redis.Nil {
			return false, addToActiveCountErr
		}

		if addToActiveCount == 1 {
			selectedGroupQueue = nextGroupQueue
			break
		}

		stopCounter++
	}

	if selectedGroupQueue == "" {
		return false, nil
	}

	selectedGroupProcessingQueue := fmt.Sprintf("%s:processing", selectedGroupQueue)
	msg, err := client.BRPopLPush(selectedGroupQueue, selectedGroupProcessingQueue, time.Second).Result()
	if err != nil {
		return false, err
	}

	reg := registration{}
	json.Unmarshal([]byte(msg), &reg)

	subName := fmt.Sprintf("load-lock:start:%s", reg.ID)
	client.Publish(subName, "empty-message").Result()

	client.LRem(selectedGroupProcessingQueue, 1, msg)

	return true, nil
}

func processReleases(client *redis.Client) {
	msg, err := client.BRPopLPush(redisKeyReleaseQueue, redisKeyReleaseProcessingQueue, time.Second).Result()

	if err != nil && err != redis.Nil {
		return
	}

	if msg == "" {
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

	client.LRem(redisKeyReleaseProcessingQueue, 1, msg)
}

func cleanAgentData(client *redis.Client) {
	client.FlushDB()
}
