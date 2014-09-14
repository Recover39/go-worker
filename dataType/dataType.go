package dataType

type User struct {
	Id           string
	RegisterDate int64    `json:"registerDate"`
	Friends      []string `json:"friends"`
	Follower     []string `json:"follower"`
	Following    []string `json:"following"`
	WriteThread  []string `json:"writeThread"`
	WriteComment []string `json:"writeComment"`
	LikeThread   []string `json:"likeThread"`
	LikeComment  []string `json:"likeComment"`
	BlockUser    []string `json:"blockUser"`
	UnreadThread []string `json:"unreadThread"`
	ReadedThread []string `json:"readedThread"`
}

type Thread struct {
	Id      string
	Author  string   `json:"author"`
	Public  string   `json:"is_public"`
	Like    []string `json:"likes"`
	Report  []string `json:"reports"`
	Block   []string `json:"blocks"`
	Reader  []string `json:"readers"`
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
	Block     []string `json:"blocks"`
	Content   string   `json:"content"`
	Time      int64    `json:"pub_date"`
}

type UserRequest struct {
	User       string   `json:"user"`
	FriendList []string `json:"friendList"`
	Action     string   `json:"action"`
	Time       int64    `json:"time"`
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
