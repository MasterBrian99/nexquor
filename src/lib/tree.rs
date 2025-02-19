
/// A node in the binary tree.
#[derive(Debug)]
pub struct Node {
    key: String,
    value: String,
    left: Subtree,
    right: Subtree,
    height: usize,
}

/// A possibly-empty subtree.
#[derive(Debug)]
pub struct Subtree(Option<Box<Node>>);


impl Node {
        pub fn new(key: String, value: String) -> Node {
            Node {
                key,
                value,
                left: Subtree(None),
                right: Subtree(None),
                height: 1,
            }
        }
}