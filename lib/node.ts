class Node {
  key: string;
  value: string;
  height: number;
  left: Node | null;
  right: Node | null;

  constructor(key: string, value: string) {
    this.key = key;
    this.value = value;
    this.height = 1; // Default height for a new node
    this.left = null;
    this.right = null;
  }
}

export default Node;
