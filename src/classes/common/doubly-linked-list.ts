class Node<T> {
  constructor(
    public value: T,
    public prev: Node<T> = null,
    public next: Node<T> = null,
  ) {}
}

export class DoublyLinkedList<T> {
  private head: Node<T> = null;
  private tail: Node<T> = null;
  private readonly nodeMap = new Map<T, Node<T>>();

  insertAtHead(value: T) {
    const node = new Node(value);
    this.nodeMap.set(value, node);
    if (this.head) {
      node.next = this.head;
      this.head.prev = node;
      this.head = node;
    } else {
      this.head = node;
      this.tail = node;
    }
    return node;
  }

  moveToHead(node: Node<T>) {
    if (this.head === node) return;
    if (node.prev) node.prev.next = node.next;
    if (node.next) node.next.prev = node.prev;
    if (this.tail === node) this.tail = node.prev;
    node.next = this.head;
    node.prev = null;
    if (this.head) this.head.prev = node;
    this.head = node;
    if (!this.tail) this.tail = node;
  }

  removeTail() {
    if (!this.tail) return null;
    const oldTail = this.tail;
    if (this.head === this.tail) {
      this.head = null;
      this.tail = null;
    } else {
      this.tail = oldTail.prev;
      this.tail.next = null;
    }
    this.nodeMap.delete(oldTail.value);
    return oldTail;
  }

  getNode(value: T) {
    return this.nodeMap.get(value);
  }

  remove(node: Node<T>) {
    if (this.head === node) this.head = node.next;
    if (this.tail === node) this.tail = node.prev;
    if (node.next) node.next.prev = node.prev;
    if (node.prev) node.prev.next = node.next;
    this.nodeMap.delete(node.value);
  }

  *reverseValues() {
    let current = this.tail;
    while (current) {
      yield current.value;
      current = current.prev;
    }
  }
}
