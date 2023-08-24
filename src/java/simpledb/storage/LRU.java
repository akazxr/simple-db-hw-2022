package simpledb.storage;

import simpledb.common.Database;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class LRU {
    static class Node {
        PageId pid;

        Node next;

        Node prev;

        Page page;

        public Node() {
        }

        public Node(PageId pid, Page page) {
            this.pid = pid;
            this.page = page;
        }
    }

    class NodeList {
        int size;

        Node head;

        Node tail;

        public NodeList() {
            head = new Node();
            tail = new Node();
            head.next = tail;
            tail.prev = head;
        }

        public void addFirst(Node node) {
            node.next = head.next;
            head.next.prev = node;
            head.next = node;
            node.prev = head;
            size++;
        }

        public void remove(Node node) {
            if (head.next == tail) {
                return;
            }
            node.prev.next = node.next;
            node.next.prev = node.prev;
            size--;
        }

        public Node removeLast() {
            Node last = tail.prev;
            remove(last);
            return last;
        }
    }

    public Map<PageId, Node> map;

    private int capacity;

    private NodeList nodeList;

    public LRU(int capacity) {
        this.capacity = capacity;
        nodeList = new NodeList();
        map = new ConcurrentHashMap<>();
    }

    public Node get(PageId pid) {
        if (!map.containsKey(pid)) {
            throw new NoSuchElementException("no such page in buffer pool");
        }
        Node node = map.get(pid);
        nodeList.remove(node);
        nodeList.addFirst(node);
        return node;
    }

    public void put(PageId pid) {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = map.containsKey(pid) ? map.get(pid).page : dbFile.readPage(pid);
        if (map.containsKey(pid)) {
            nodeList.remove(map.get(pid));
            map.remove(pid);
        }
        Node newNode = new Node(pid, page);
        nodeList.addFirst(newNode);
        map.put(pid, newNode);
        if (nodeList.size > capacity) {
            removeLast();
        }
    }

    public void put(PageId pid, Node newNode) {
        if (map.containsKey(pid)) {
            nodeList.remove(map.get(pid));
            map.remove(pid);
        }
        nodeList.addFirst(newNode);
        map.put(pid, newNode);
        if (nodeList.size > capacity) {
            removeLast();
        }
    }

    public PageId removeLast() {
        Node last = nodeList.removeLast();
        map.remove(last.pid);
        return last.pid;
    }

}
