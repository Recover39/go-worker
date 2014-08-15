package main

import (
	"fmt"
	"log"

	"github.com/couchbaselabs/go-couchbase"
)

type User struct {
	Name string `json:"name"`
	Id   string `json:"id"`
}

func main() {
	connection, err := couchbase.Connect("http://125.209.198.141:8091")
	if err != nil {
		log.Fatalf("Failed to connect to couchbase (%s)\n", err)
	}

	pool, err := connection.GetPool("default")
	if err != nil {
		log.Fatalf("Failed to get pool from couchbase (%s)\n", err)
	}

	bucket, err := pool.GetBucket("default")
	if err != nil {
		log.Fatalf("Failed to get bucket from couchbase (%s)\n", err)
	}

	user := User{"tara", "s:2"}

	added, err := bucket.Add(user.Id, 0, user)
	if err != nil {
		log.Fatalf("Failed to write data to the cluster (%s)\n", err)
	}

	if !added {
		log.Fatalf("A Document with the same id of (%s) already exists.\n", user.Id)
	}

	user = User{}

	err = bucket.Get("s:1", &user)
	if err != nil {
		log.Fatalf("Failed to get data from the cluster (%s)\n", err)
	}

	fmt.Printf("Got back a user with a name of (%s) and id (%s)\n", user.Name, user.Id)

}
