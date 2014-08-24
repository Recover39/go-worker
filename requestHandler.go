package main

import (
	"encoding/json"
	"fmt"
)

type Thread struct {
	Id      uint64
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
}

type singleThreadRequest struct {
}

type singleCommentRequest struct {
}
