package lib

type Node struct {
	key    string
	value  string
	height int
	left   *Node
	right  *Node
}

func NewNode(key string, value string) *Node {
	return &Node{
		key:    key,
		value:  value,
		height: 1,
	}
}
