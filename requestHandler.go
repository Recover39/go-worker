package requestHandler

import (
	"./connectionHandler"
	"./dataType"
	"encoding/json"
	"fmt"
	"log"
	//"database/sql"
	//_ "github.com/go-sql-driver/mysql"
)

func RouteRequest(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		//check request actionType
		type ActionType struct {
			Action string `json:"action"`
		}

		var actionType ActionType
		err := json.Unmarshal(d.Body, &actionType)
		if err != nil {
			log.Println("error:", err)
		}

		//route request
		switch actionType.Action {
		case `threadLike`, `threadUnlike`, `threadReport`, `threadHide`:
			simpleThreadRequest(d.Body)

		case `commentAdd`:
			addComment(d.Body)

		case `commentLike`, `commentUnlike`, `commentReport`, `commentHide`:
			simpleCommentRequest(d.Body)

		case `newThread`:
			//newThread(d.Body, true)
		case `newThread_textOnly`:
			newThread(d.Body)

		default:
			log.Printf("unknown actionType")
		}

		d.Ack(false)
	}

	log.Printf("handle: deliveries channel closed")
	done <- nil
}

func IncreaseBucketKey(bucketName string) string {
	bucket, err := connectionHandler.GetBucket(bucketName)
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	bucketKey := bucketName + "Num"

	key, err := bucket.Incr(bucketKey, 1, 1, 0)
	if err != nil {
		log.Fatalf("Failed to get data from the cluster (%s)\n", err)
	}

	return strconv.FormatUint(key, 10)
}

func newThread(msg []byte) {
	var thread dataType.Thread
	err := json.Unmarshal(msg, &thread)
	if err != nil {
		log.Println("error:", err)
	}

	//fill thread property
	thread.Id = increaseBucketKey("Thread")
	// need to set
	// thread.Reader

	bucket, err := connectionHandler.GetBucket("Thread")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	added, err := bucket.Add(thread.Id, 0, thread)
	if err != nil {
		log.Fatalf("Failed to write data to the cluster (%s)\n", err)
	}

	if !added {
		log.Fatalf("A Document with the same id of (%s) already exists.\n", thread.Id)
	}
}

func addComment(msg []byte) {
	var comment dataType.Comment
	err := json.Unmarshal(msg, &comment)
	if err != nil {
		log.Println("error:", err)
	}

	log.Println(comment)

	comment.Id = increaseBucketKey("Comment")

	commentBucket, err := connectionHandler.GetBucket("Comment")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}
	added, err := commentBucket.Add(comment.Id, 0, comment)
	if err != nil {
		log.Fatalf("Failed to write data to the cluster (%s)\n", err)
	}
	if !added {
		log.Fatalf("A Document with the same id of (%s) already exists.\n", comment.Id)
	}

	var thread dataType.Thread

	threadBucket, err := connectionHandler.GetBucket("Thread")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}
	err = threadBucket.Get(comment.Thread_id, &thread)
	if err != nil {
		log.Fatalf("Failed to get data from the cluster (%s)\n", err)
	}

	thread.Comment = append(thread.Comment, comment.Id)

	//update change
	err = threadBucket.Set(thread.Id, 0, thread)
	if err != nil {
		log.Fatalf("Failed to write data to the cluster (%s)\n", err)
	}
}

func simpleThreadRequest(msg []byte) {
	var request dataType.ThreadRequest
	err := json.Unmarshal(msg, &request)
	if err != nil {
		log.Println("error:", err)
	}

	var thread = dataType.Thread

	bucket, err := connectionHandler.GetBucket("Thread")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}
	err = bucket.Get(request.Thread_id, &thread)
	if err != nil {
		log.Fatalf("Failed to get data from the cluster (%s)\n", err)
	}

	switch request.Action {
	case `threadLike`:
		thread.Like = append(thread.Like, request.User)
	case `threadUnlike`:
		for i, userName := range thread.Like {
			if userName == request.User {
				thread.Like = append(thread.Like[:i], thread.Like[i+1:]...)
				break
			}
		}
	case `threadReport`:
		thread.Report = append(thread.Report, request.User)
	case `threadHide`:
		thread.Hide = append(thread.Hide, request.User)
		//update user info
	}

	//update change
	err = bucket.Set(thread.Id, 0, thread)
	if err != nil {
		log.Fatalf("Failed to write data to the cluster (%s)\n", err)
	}

	log.Println(request)
}
