export class Node {
    key: string;
    value: string;
    left: Node | null;
    right: Node | null;
    height: number;

    constructor(key: string, value: string) {
        this.key = key;
        this.value = value;
        this.height = 1;
        this.left = null;
        this.right = null;
    }
}