package main

type Thread struct {
	Id      string
	Author  string   `json:"author"`
	Public  string   `json:"is_public"`
	Like    []string `json:"likes"`
	Report  []string `json:"reports"`
	Reader  []string `json:"readers"`
	Hide    []string `json:"hides"`
	Comment []string `json:"comments"`
	Content string   `json:"content"`
	Image   string   `json:"image_url"`
	Time    int64    `json:"pub_date"`
}

type Comment struct {
	Id        string
	Thread_id string   `json:"thread_id"`
	Author    string   `json:"author"`
	Like      []string `json:"likes"`
	Report    []string `json:"reports"`
	Content   string   `json:"content"`
	Time      int64    `json:"pub_date"`
}

type ThreadRequest struct {
	Thread_id string `json:"thread_id"`
	User      string `json:"user"`
	Action    string `json:"action"`
	Time      int64  `json:"time"`
}

type CommentRequest struct {
	Comment_id string `json:"comment_id"`
	User       string `json:"user"`
	Action     string `json:"action"`
	Time       int64  `json:"time"`
}
