import { Node } from "./node";

 class AVL {
    root: Node | null;

    constructor() {
        this.root = null;
    }
    public insert(key: string, value: string) {
        this.root = this.insertNode(this.root, key, value);
    }
    private insertNode(node: Node | null, key: string, value: string): Node {
        if (node === null) {
            return new Node(key, value);
        }
    }
}