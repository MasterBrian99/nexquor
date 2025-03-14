package main

type Node struct {
	key    string
	value  string
	left   *Node
	right  *Node
	height int
}
