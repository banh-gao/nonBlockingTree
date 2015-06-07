package it.unitn.zozin.concurrency;

import it.unitn.zozin.concurrency.NonBlockingTree.TreeVisitor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;

abstract class Node<K extends Comparable<K>> {
	
	public K key;
	
	Node(K key) {
		this.key = key;
	}
	
	public abstract void inVisit(TreeVisitor<K> v, int level);
	
	public abstract void preVisit(TreeVisitor<K> v, int level);
	
	@Override
	public String toString() {
		return "Leaf " + key.toString();
	}
	
	public static class Leaf<K extends Comparable<K>> extends Node<K> {

		Leaf(K key) {
			super(key);
		}
		
		public static <K extends Comparable<K>> Leaf<K> getInstance(K key) {
			return new Leaf<K>(key);
		}

		@Override
		public void inVisit(TreeVisitor<K> v, int level) {
			v.visit(this, level);
		}
		
		@Override
		public void preVisit(TreeVisitor<K> v, int level) {
			v.visit(this, level);
		}
	}


	public static class DummyNode<K extends Comparable<K>> extends Leaf<K> {

		private DummyNode(K key) {
			super(key);
		}

		public static <K extends Comparable<K>> DummyNode<K> getInstance(K dummyKey) {
			return new DummyNode<K>(dummyKey);
		}
	}
	
	public static class InternalNode<K extends Comparable<K>> extends Node<K> {
    
		private AtomicReference<Node<K>> left = new AtomicReference<Node<K>>();
		private AtomicReference<Node<K>> right = new AtomicReference<Node<K>>();
		private AtomicStampedReference<OperationInfo<K>> update = new AtomicStampedReference<OperationInfo<K>>(null, Update.CLEAN);

		public static <K extends Comparable<K>> InternalNode<K> getInstance(K key, Node<K> left, Node<K> right) {
			return new InternalNode<K>(key, left, right);
		}

		private InternalNode(K key, Node<K> left, Node<K> right) {
			super(key);
			this.left.set(left);
			this.right.set(right);
		}
		
		public void setChild(Node<K> expectedNode, Node<K> newNode) {
			if(newNode.key.compareTo(key) < 0)
				this.left.compareAndSet(expectedNode, newNode);
			else
				this.right.compareAndSet(expectedNode, newNode);
		}

		public boolean setUpdate(Update<K> expectedUpd, OperationInfo<K> newInfo, int newState) {
			return this.update.compareAndSet(expectedUpd.info, newInfo, expectedUpd.state, newState);
		}

		public Node<K> getLeft() {
			return left.get();
		}

		public Node<K> getRight() {
			return right.get();
		}

		public Update<K> getUpdate() {
			int[] state = new int[1];
			OperationInfo<K> i = update.get(state);
			return new Update<K>(state[0], i);
		}
		
		@Override
		public void inVisit(TreeVisitor<K> v, int level) {
			getLeft().inVisit(v, level+1);
			v.visit(this, level);
			getRight().inVisit(v, level+1);
		}
		
		@Override
		public void preVisit(TreeVisitor<K> v, int level) {
			v.visit(this, level);
			getLeft().preVisit(v, level+1);
			getRight().preVisit(v, level+1);
		}
		
		@Override
		public String toString() {
			return "Internal " + key.toString();
		}
	}
}