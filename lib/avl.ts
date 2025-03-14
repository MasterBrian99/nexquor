import Node from "./node";

class AVL {
  private root: Node | null = null;

  public insert(key: string, value: string): void {
    this.root = this.insertNode(this.root, key, value);
  }

  public update(key: string, value: string): void {
    this.root = this.updateNode(this.root, key, value);
  }

  public delete(key: string): void {
    this.root = this.deleteNode(this.root, key);
  }

  public getInOrderTraversal(): Array<[string, string]> {
    const res: Array<[string, string]> = [];
    this.inOrderTraversalHelper(this.root, res);
    return res;
  }

  public findKey(key: string): boolean {
    return this.findKeyHelper(this.root, key);
  }

  public findValue(key: string): string | null {
    return this.findValueHelper(this.root, key);
  }

  public empty(): void {
    this.root = null;
  }

  public display(): void {
    this.displayHelper(this.root);
  }

  public printTree(): void {
    this.printTreeHelper(this.root, "", true);
  }

  private insertNode(node: Node | null, key: string, value: string): Node {
    if (node === null) {
      return new Node(key, value);
    }

    if (key < node.key) {
      node.left = this.insertNode(node.left, key, value);
    } else if (key > node.key) {
      node.right = this.insertNode(node.right, key, value);
    } else {
      return node; // Duplicate keys are not allowed
    }

    node.height =
      1 + Math.max(this.getHeight(node.left), this.getHeight(node.right));
    const balance = this.getBalance(node);

    // Left Left Case
    if (balance > 1 && key < (node.left?.key ?? "")) {
      return this.rightRotate(node);
    }

    // Right Right Case
    if (balance < -1 && key > (node.right?.key ?? "")) {
      return this.leftRotate(node);
    }

    // Left Right Case
    if (balance > 1 && key > ((node.left && node.left.key) ?? "")) {
      if (node.left) {
        node.left = this.leftRotate(node.left);
      }
      return this.rightRotate(node);
    }

    // Right Left Case
    if (balance < -1 && key < ((node.right && node.right.key) ?? "")) {
      if (node.right) {
        node.right = this.rightRotate(node.right);
      }
      return this.leftRotate(node);
    }

    return node;
  }

  private updateNode(
    node: Node | null,
    key: string,
    value: string
  ): Node | null {
    if (node === null) {
      return null;
    }

    if (key < node.key) {
      node.left = this.updateNode(node.left, key, value);
    } else if (key > node.key) {
      node.right = this.updateNode(node.right, key, value);
    } else {
      node.value = value;
    }

    node.height =
      1 + Math.max(this.getHeight(node.left), this.getHeight(node.right));
    const balance = this.getBalance(node);

    // Left Left Case
    if (balance > 1 && key < (node.left?.key ?? "")) {
      return this.rightRotate(node);
    }

    // Right Right Case
    if (balance < -1 && key > (node.right?.key ?? "")) {
      return this.leftRotate(node);
    }

    // Left Right Case
    if (balance > 1 && key > ((node.left && node.left.key) ?? "")) {
      if (node.left) {
        node.left = this.leftRotate(node.left);
      }
      return this.rightRotate(node);
    }

    // Right Left Case
    if (balance < -1 && key < ((node.right && node.right.key) ?? "")) {
      if (node.right) {
        node.right = this.rightRotate(node.right);
      }
      return this.leftRotate(node);
    }

    return node;
  }

  private deleteNode(root: Node | null, key: string): Node | null {
    if (root === null) {
      return root;
    }

    if (key < root.key) {
      root.left = this.deleteNode(root.left, key);
    } else if (key > root.key) {
      root.right = this.deleteNode(root.right, key);
    } else {
      if (root.left === null || root.right === null) {
        const temp = root.left ?? root.right;

        if (temp === null) {
          root = null;
        } else {
          root = temp;
        }
      } else {
        const temp = this.minValueNode(root.right);
        root.key = temp.key;
        root.value = temp.value;
        root.right = this.deleteNode(root.right, temp.key);
      }
    }

    if (root === null) {
      return root;
    }

    root.height =
      1 + Math.max(this.getHeight(root.left), this.getHeight(root.right));
    const balance = this.getBalance(root);

    // Left Left Case
    if (balance > 1 && this.getBalance(root.left) >= 0) {
      return this.rightRotate(root);
    }

    // Left Right Case
    if (balance > 1 && this.getBalance(root.left) < 0) {
      if (root.left) {
        root.left = this.leftRotate(root.left);
      }
      return this.rightRotate(root);
    }

    // Right Right Case
    if (balance < -1 && this.getBalance(root.right) <= 0) {
      return this.leftRotate(root);
    }

    // Right Left Case
    if (balance < -1 && this.getBalance(root.right) > 0) {
      if (root.right) {
        root.right = this.rightRotate(root.right);
      }
      return this.leftRotate(root);
    }

    return root;
  }

  private rightRotate(y: Node): Node {
    const x = y.left!;
    const t2 = x.right;
    x.right = y;
    y.left = t2;
    y.height = 1 + Math.max(this.getHeight(y.left), this.getHeight(y.right));
    x.height = 1 + Math.max(this.getHeight(x.left), this.getHeight(x.right));
    return x;
  }

  private leftRotate(x: Node): Node {
    const y = x.right!;
    const t2 = y.left;
    y.left = x;
    x.right = t2;
    x.height = 1 + Math.max(this.getHeight(x.left), this.getHeight(x.right));
    y.height = 1 + Math.max(this.getHeight(y.left), this.getHeight(y.right));
    return y;
  }

  private getHeight(node: Node | null): number {
    return node ? node.height : 0;
  }

  private getBalance(node: Node | null): number {
    return node ? this.getHeight(node.left) - this.getHeight(node.right) : 0;
  }

  private minValueNode(node: Node): Node {
    let current = node;
    while (current.left !== null) {
      current = current.left;
    }
    return current;
  }

  private inOrderTraversalHelper(
    node: Node | null,
    res: Array<[string, string]>
  ): void {
    if (node === null) {
      return;
    }
    this.inOrderTraversalHelper(node.left, res);
    res.push([node.key, node.value]);
    this.inOrderTraversalHelper(node.right, res);
  }

  private findKeyHelper(node: Node | null, key: string): boolean {
    if (node === null) {
      return false;
    }

    if (key < node.key) {
      return this.findKeyHelper(node.left, key);
    } else if (key > node.key) {
      return this.findKeyHelper(node.right, key);
    } else {
      return true;
    }
  }

  private findValueHelper(node: Node | null, key: string): string | null {
    if (node === null) {
      return null;
    }

    if (key < node.key) {
      return this.findValueHelper(node.left, key);
    } else if (key > node.key) {
      return this.findValueHelper(node.right, key);
    } else {
      return node.value;
    }
  }

  private displayHelper(node: Node | null): void {
    if (node !== null) {
      const leftKey = node.left ? node.left.key : "null";
      const rightKey = node.right ? node.right.key : "null";
      console.log(
        `Node Key: ${node.key}, Value: ${node.value}, Left Node: ${leftKey}, Right Node: ${rightKey}`
      );
      this.displayHelper(node.left);
      this.displayHelper(node.right);
    }
  }

  private printTreeHelper(
    node: Node | null,
    indent: string,
    isLast: boolean
  ): void {
    process.stdout.write(indent);
    if (isLast) {
      process.stdout.write("└─ ");
      indent += "  ";
    } else {
      process.stdout.write("├─ ");
      indent += "| ";
    }

    if (node !== null) {
      console.log(`${node.key} : ${node.value}`);
      if (node.left !== null) {
        this.printTreeHelper(node.left, indent, false);
      } else {
        this.printNullNode(indent, false);
      }

      if (node.right !== null) {
        this.printTreeHelper(node.right, indent, true);
      } else {
        this.printNullNode(indent, true);
      }
    } else {
      console.log("null");
    }
  }

  private printNullNode(indent: string, isLast: boolean): void {
    process.stdout.write(indent);
    if (isLast) {
      process.stdout.write("└─ ");
    } else {
      process.stdout.write("├─ ");
    }
    console.log("null");
  }
}

export default AVL;
