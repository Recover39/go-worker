package requestHandler

import (
	"../connectionHandler"
	"../dataType"
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"strconv"
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
		case `newThread`:
			newThread(d.Body)

		case `threadLike`, `threadUnlike`, `threadReport`, `threadBlock`:
			threadRequestHandler(d.Body)

		case `commentAdd`:
			addComment(d.Body)

		case `commentLike`, `commentUnlike`, `commentReport`, `commentBlock`:
			commentRequestHandler(d.Body)

		case `friendAdd`, `friendDelete`:

		case `userRegister`:
			registerUser(d.Body)

		default:
			log.Printf("unknown actionType")
		}

		d.Ack(false)
	}

	log.Printf("handle: deliveries channel closed")
	done <- nil
}

func registerUser(msg []byte) {
	var newUser dataType.User
	err := json.Unmarshal(msg, &newUser)
	if err != nil {
		log.Println("error:", err)
	}

	bucket, err := connectionHandler.GetBucket("User")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	added, err := bucket.Add(newUser.Id, 0, newUser)
	if err != nil {
		log.Fatalf("Failed to register new user (%s)\n", err)
	}

	if !added {
		log.Fatalf("A User with the same id of (%s) already exists.\n", newUser.Id)
	}

	defer bucket.Close()
}

func increaseBucketKey(bucketName string) string {
	bucket, err := connectionHandler.GetBucket(bucketName)
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	bucketKey := bucketName + "Num"

	key, err := bucket.Incr(bucketKey, 1, 1, 0)
	if err != nil {
		log.Fatalf("Failed to get bucketKey (%s)\n", err)
	}

	defer bucket.Close()

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

	threadBucket, err := connectionHandler.GetBucket("Thread")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	added, err := threadBucket.Add(thread.Id, 0, thread)
	if err != nil {
		log.Fatalf("Failed to write new thread (%s)\n", err)
	}

	if !added {
		log.Fatalf("A Thread with the same id of (%s) already exists.\n", thread.Id)
	}

	var user dataType.User

	userBucket, err := connectionHandler.GetBucket("User")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}
	err = userBucket.Get(thread.Author, &user)
	if err != nil {
		log.Fatalf("Failed to get user to add writeThread (%s)\n", err)
	}

	user.WriteThread = append(user.WriteThread, thread.Id)

	//update change
	err = userBucket.Set(user.Id, 0, user)
	if err != nil {
		log.Fatalf("Failed to re-write user to add writeThread (%s)\n", err)
	}

	defer threadBucket.Close()
	defer userBucket.Close()
}

func addComment(msg []byte) {
	var comment dataType.Comment
	err := json.Unmarshal(msg, &comment)
	if err != nil {
		log.Println("error:", err)
	}

	comment.Id = increaseBucketKey("Comment")

	commentBucket, err := connectionHandler.GetBucket("Comment")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}
	added, err := commentBucket.Add(comment.Id, 0, comment)
	if err != nil {
		log.Fatalf("Failed to write new comment (%s)\n", err)
	}
	if !added {
		log.Fatalf("A Comment with the same id of (%s) already exists.\n", comment.Id)
	}

	var thread dataType.Thread

	threadBucket, err := connectionHandler.GetBucket("Thread")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}
	err = threadBucket.Get(comment.Thread_id, &thread)
	if err != nil {
		log.Fatalf("Failed to get thread to add comment (%s)\n", err)
	}

	thread.Comment = append(thread.Comment, comment.Id)

	//update change
	err = threadBucket.Set(thread.Id, 0, thread)
	if err != nil {
		log.Fatalf("Failed to re-write thread to add comment (%s)\n", err)
	}

	var user dataType.User

	userBucket, err := connectionHandler.GetBucket("User")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}
	err = userBucket.Get(comment.Author, &user)
	if err != nil {
		log.Fatalf("Failed to get user to add writeComment (%s)\n", err)
	}

	user.WriteComment = append(user.WriteComment, comment.Id)

	//update change
	err = userBucket.Set(user.Id, 0, user)
	if err != nil {
		log.Fatalf("Failed to re-write user to add writeComment (%s)\n", err)
	}

	defer commentBucket.Close()
	defer threadBucket.Close()
	defer userBucket.Close()
}

func threadRequestHandler(msg []byte) {
	var request dataType.ThreadRequest
	err := json.Unmarshal(msg, &request)
	if err != nil {
		log.Println("error:", err)
	}

	/////////////////

	var user dataType.User

	userBucket, err := connectionHandler.GetBucket("User")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	err = userBucket.Get(request.User, &user)
	if err != nil {
		log.Fatalf("Failed to get user to change property (%s)\n", err)
	}

	/////////////////

	var thread dataType.Thread

	threadBucket, err := connectionHandler.GetBucket("Thread")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	err = threadBucket.Get(request.Thread_id, &thread)
	if err != nil {
		log.Fatalf("Failed to get thread to change property (%s)\n", err)
	}

	switch request.Action {
	case `threadLike`:
		var exsit bool
		for _, userName := range thread.Like {
			if userName == request.User {
				exsit = true
			}
		}
		if exsit != true {
			thread.Like = append(thread.Like, request.User)
		}

		exsit = false

		for _, thread_id := range user.LikeThread {
			if thread_id == request.Thread_id {
				exsit = true
			}
		}
		if exsit != true {
			user.LikeThread = append(user.LikeThread, request.Thread_id)
		}

	case `threadUnlike`:
		for i, userName := range thread.Like {
			if userName == request.User {
				thread.Like = append(thread.Like[:i], thread.Like[i+1:]...)
				break
			}
		}

		for i, thread_id := range user.LikeThread {
			if thread_id == request.Thread_id {
				user.LikeThread = append(user.LikeThread[:i], user.LikeThread[i+1:]...)
				break
			}
		}
	case `threadReport`:
		var exsit bool
		for _, userName := range thread.Report {
			if userName == request.User {
				exsit = true
			}
		}
		if exsit != true {
			thread.Report = append(thread.Report, request.User)
		}
	case `threadBlock`:
		var exsit bool
		for _, userName := range thread.Block {
			if userName == request.User {
				exsit = true
			}
		}
		if exsit != true {
			thread.Block = append(thread.Block, request.User)
		}

		for _, thread_id := range user.BlockUser {
			if thread_id == thread.Author {
				exsit = true
			}
		}
		if exsit != true {
			user.BlockUser = append(user.BlockUser, request.Thread_id)
		}
	}

	//update change
	err = threadBucket.Set(thread.Id, 0, thread)
	if err != nil {
		log.Fatalf("Failed to re-write thread to change property (%s)\n", err)
	}

	err = userBucket.Set(user.Id, 0, user)
	if err != nil {
		log.Fatalf("Failed to re-write user to add likeThread (%s)\n", err)
	}

	defer threadBucket.Close()
	defer userBucket.Close()
}

func commentRequestHandler(msg []byte) {
	var request dataType.CommentRequest
	err := json.Unmarshal(msg, &request)
	if err != nil {
		log.Println("error:", err)
	}

	/////////////////

	var user dataType.User

	userBucket, err := connectionHandler.GetBucket("User")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	err = userBucket.Get(request.User, &user)
	if err != nil {
		log.Fatalf("Failed to get user to change property (%s)\n", err)
	}

	/////////////////

	var comment dataType.Comment

	commentBucket, err := connectionHandler.GetBucket("Comment")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	err = commentBucket.Get(request.Comment_id, &comment)
	if err != nil {
		log.Fatalf("Failed to get comment to change property (%s)\n", err)
	}

	/////////////////

	switch request.Action {
	case `commentLike`:
		var exsit bool
		for _, userName := range comment.Like {
			if userName == request.User {
				exsit = true
			}
		}
		if exsit != true {
			comment.Like = append(comment.Like, request.User)
		}

		exsit = false

		for _, comment_id := range user.LikeComment {
			if comment_id == request.Comment_id {
				exsit = true
			}
		}
		if exsit != true {
			user.LikeComment = append(user.LikeComment, request.Comment_id)
		}
	case `commentUnlike`:
		for i, userName := range comment.Like {
			if userName == request.User {
				comment.Like = append(comment.Like[:i], comment.Like[i+1:]...)
				break
			}
		}

		for i, comment_id := range user.LikeComment {
			if comment_id == request.Comment_id {
				user.LikeComment = append(user.LikeComment[:i], user.LikeComment[i+1:]...)
			}
		}
	case `commentReport`:
		var exsit bool
		for _, userName := range comment.Report {
			if userName == request.User {
				exsit = true
			}
		}
		if exsit != true {
			comment.Report = append(comment.Report, request.User)
		}
	case `commentBlock`:
		var exsit bool
		for _, userName := range comment.Block {
			if userName == request.User {
				exsit = true
			}
		}
		if exsit != true {
			comment.Block = append(comment.Block, request.User)
		}

		exsit = false

		for _, blockUser := range user.BlockUser {
			if blockUser == comment.Author {
				exsit = true
			}
		}
		if exsit != true {
			user.BlockUser = append(user.BlockUser, comment.Author)
		}
	}

	//update change
	err = commentBucket.Set(comment.Id, 0, comment)
	if err != nil {
		log.Fatalf("Failed to re-write comment to change property (%s)\n", err)
	}

	err = userBucket.Set(user.Id, 0, user)
	if err != nil {
		log.Fatalf("Failed to re-write user to change property (%s)\n", err)
	}

	defer commentBucket.Close()
	defer userBucket.Close()
}
