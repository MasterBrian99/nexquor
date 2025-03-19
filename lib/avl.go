package lib

type AVL struct {
	root *Node
}

func NewAVL() *AVL {
	return &AVL{}
}

func (avl *AVL) Insert(key string, value string) {
	avl.root = avl.insert(avl.root, key, value)
}

func (avl *AVL) insert(node *Node, key string, value string) *Node {
	if node == nil {
		return NewNode(key, value)
	}
	if key < node.key {
		node.left = avl.insert(node.left, key, value)
	} else if key > node.key {
		node.right = avl.insert(node.right, key, value)
	} else {
		return node
	}
	node.height = 1 + max
}

func (avl *AVL) max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (avl *AVL) height(node *Node) int {
	if node == nil {
		return 0
	}
	return node.height
}
