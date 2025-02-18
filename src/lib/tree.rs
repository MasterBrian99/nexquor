
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
