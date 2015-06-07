package it.unitn.zozin.concurrency;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * This data structure is a Thread-safe Binary Search Tree providing
 * non-blocking operations.
 * It is based on the algorithm proposed by Ellen et al. in [Non-blocking Binary
 * Search Trees, Proceedings of the 29th ACM SIGACT-SIGOPS Symposium on
 * Principles of Distributed Computing, pages 131-140, 2010].
 * 
 * @author Daniel Zozin
 *
 * @param <K>
 *            The type of values stored in the tree
 */
public class NonBlockingTree<K extends Comparable<K>> extends AbstractSet<K> {

	final InternalNode root;

	private final Leaf DUMMY_NODE1;
	private final Leaf DUMMY_NODE2;
	private final AtomicInteger size = new AtomicInteger();

	private class SearchRes {

		InternalNode gp;
		InternalNode p;
		Leaf l;
		Update pupdate;
		Update gpupdate;
	}

	public static <K extends Comparable<K>> NonBlockingTree<K> getInstance(K minKey, K maxKey) {
		return new NonBlockingTree<K>(minKey, maxKey);
	}

	private NonBlockingTree(K minKey, K maxKey) {
		if (minKey == null || maxKey == null)
			throw new NullPointerException();
		if (minKey.compareTo(maxKey) >= 0)
			throw new IllegalArgumentException();

		this.DUMMY_NODE1 = new Leaf(minKey);
		this.DUMMY_NODE2 = new Leaf(maxKey);

		this.root = new InternalNode(maxKey, DUMMY_NODE1, DUMMY_NODE2);
	}

	/**
	 * Search from root to leaf for the given key
	 */
	@SuppressWarnings("unchecked")
	private SearchRes search(K key) {
		SearchRes r = new SearchRes();

		Node cur = root;
		while (cur instanceof NonBlockingTree.InternalNode) {
			// Remember parent of p
			r.gp = r.p;
			// Remember parent of l
			r.p = (InternalNode) cur;
			// Remember update field of gp
			r.gpupdate = r.pupdate;

			// Remember update field of p
			r.pupdate = r.p.getUpdate();

			// Move down to appropriate child
			if (key.compareTo(cur.key) < 0)
				cur = r.p.getLeft();
			else
				cur = r.p.getRight();
		}

		r.l = (Leaf) cur;

		return r;
	}

	public boolean find(K key) {
		SearchRes r = search(key);
		return r.l.key.compareTo(key) == 0;
	}

	public boolean insert(K key) {
		while (true) {
			SearchRes r = search(key);

			// Cannot insert duplicate key
			if (r.l.key.compareTo(key) == 0)
				return false;

			// Node already flagged, help the other and then retry the insert
			if (r.pupdate.state != Update.CLEAN) {
				help(r.pupdate);
				continue;
			}

			InternalNode newInternal = createSubTree(r.l, key);

			InsertInfo insertOp = new InsertInfo(r.p, newInternal, r.l);

			// Try to set parent flag to insert flag
			if (r.p.setUpdate(r.pupdate, insertOp, Update.IFLAG)) {
				// IFLAG successful, finish insertion
				helpInsert(insertOp);
				return true;
			} else {
				// IFLAG failed, help who caused the failure
				help(r.p.getUpdate());
			}
		}
	}

	/**
	 * @return Subtree with 3 nodes: internal node, new key leaf, sibling leaf
	 *         (old leaf that will we replaced with this tree)
	 */
	private InternalNode createSubTree(Node siblingNode, K k2) {
		Node left, right;
		K max;

		if (siblingNode.key.compareTo(k2) < 0) {
			left = getLeafInstance(siblingNode.key);
			right = getLeafInstance(k2);
			max = k2;
		} else {
			// k1 == k2 not possible since duplicated key are not admitted
			left = getLeafInstance(k2);
			right = getLeafInstance(siblingNode.key);
			max = siblingNode.key;
		}

		return new InternalNode(max, left, right);
	}

	private Leaf getLeafInstance(K key) {
		if (key.compareTo(DUMMY_NODE1.key) == 0)
			return DUMMY_NODE1;
		else if (key.compareTo(DUMMY_NODE2.key) == 0)
			return DUMMY_NODE2;
		else
			return new Leaf(key);
	}

	private void helpInsert(InsertInfo insertOp) {
		// Substitute either left or right child of the parent tree with the new
		// subtree
		if (insertOp.p.spliceChild(insertOp.l, insertOp.newInternal)) {

			// Update size counter
			size.getAndIncrement();

			// Reset parent flag and unset reference to info record
			insertOp.p.setUpdate(new Update(Update.IFLAG, insertOp), insertOp, Update.CLEAN);
		}
	}

	public boolean delete(K key) {
		while (true) {
			SearchRes r = search(key);

			// Key is not in the tree
			if (r.l.key.compareTo(key) != 0)
				return false;

			// If the grandparent state is not clean, help and then retry delete
			if (r.gpupdate.state != Update.CLEAN) {
				help(r.gpupdate);
				continue;
			}

			// If the parent state is not clean, help and then retry delete
			if (r.pupdate.state != Update.CLEAN) {
				help(r.pupdate);
				continue;
			}

			DeleteInfo deleteOp = new DeleteInfo(r.gp, r.p, r.l, r.pupdate);

			// Try to set grandparent flag to delete flag
			if (r.gp.setUpdate(r.gpupdate, deleteOp, Update.DFLAG)) {
				// DFLAG successful, if also the marking succeeds returns,
				// otherwise retry delete
				if (helpDelete(deleteOp))
					return true;
			} else {
				// DFLAG failed, help who caused the failure
				help(r.gp.getUpdate());
			}
		}
	}

	private boolean helpDelete(DeleteInfo deleteOp) {
		// Try to mark the parent of the node to delete
		if (deleteOp.p.setUpdate(deleteOp.pupdate, deleteOp, Update.MARK)) {
			// MARK successful, finish deletion
			helpMarked(deleteOp);
			return true;
		} else {
			// MARK failed, help who caused the failure
			help(deleteOp.p.getUpdate());
			// Backtrack CAS because the delete has to be retried starting by
			// setting the grandparent DFLAG
			deleteOp.gp.setUpdate(deleteOp.gp.getUpdate(), deleteOp, Update.CLEAN);
			return false;
		}
	}

	private void helpMarked(DeleteInfo deleteOp) {
		Node other;

		// Set other to point to the sibling of the node to remove (deleteOp.l)
		if (deleteOp.p.getRight() == deleteOp.l)
			other = deleteOp.p.getLeft();
		else
			other = deleteOp.p.getRight();

		// Set the sibling of the node to remove, as a child of the grandparent
		if (deleteOp.gp.spliceChild(deleteOp.p, other)) {

			// Update size counter
			size.getAndDecrement();

			// Reset grandparent flag and unset reference to info record
			deleteOp.gp.setUpdate(new Update(Update.DFLAG, deleteOp), deleteOp, Update.CLEAN);
		}
	}

	/**
	 * Helps to perform the given update operation
	 */
	@SuppressWarnings("unchecked")
	private void help(Update update) {
		switch (update.state) {
			case Update.IFLAG :
				helpInsert((InsertInfo) update.info);
				break;
			case Update.DFLAG :
				helpDelete((DeleteInfo) update.info);
				break;
			case Update.MARK :
				helpMarked((DeleteInfo) update.info);
				break;
		}
	}

	/**
	 * A generic node entity that composes the tree in memory
	 */
	abstract class Node {

		public K key;

		Node(K key) {
			this.key = key;
		}
	}

	/**
	 * An internal node with a status field used for concurrent updates and two children nodes.
	 */
	public class InternalNode extends Node {

		private AtomicReference<Node> left = new AtomicReference<Node>();
		private AtomicReference<Node> right = new AtomicReference<Node>();
		private AtomicStampedReference<OperationInfo> update = new AtomicStampedReference<OperationInfo>(null, Update.CLEAN);

		InternalNode(K key, Node left, Node right) {
			super(key);
			this.left.set(left);
			this.right.set(right);
		}

		/**
		 * Atomically replace one of the children of the node
		 */
		public boolean spliceChild(Node expectedNode, Node newNode) {
			if (newNode.key.compareTo(key) < 0)
				return this.left.compareAndSet(expectedNode, newNode);
			else
				return this.right.compareAndSet(expectedNode, newNode);
		}

		/**
		 * Atomically set the update state and update info of the node
		 */
		public boolean setUpdate(Update expectedUpd, OperationInfo newInfo, int newState) {
			return this.update.compareAndSet(expectedUpd.info, newInfo, expectedUpd.state, newState);
		}

		public Node getLeft() {
			return left.get();
		}

		public Node getRight() {
			return right.get();
		}

		public Update getUpdate() {
			int[] state = new int[1];
			OperationInfo i = update.get(state);
			return new Update(state[0], i);
		}

		@Override
		public String toString() {
			return "Internal " + key.toString();
		}
	}

	/**
	 * A leaf node that stores the actual value in the tree
	 */
	public class Leaf extends Node {

		Leaf(K key) {
			super(key);
		}

		@Override
		public String toString() {
			return "Leaf " + key.toString();
		}
	}
	
	/**
	 * Keeps the update status of an internal node.
	 */
	class Update {

		public static final int CLEAN = 0;
		public static final int IFLAG = 1;
		public static final int DFLAG = 2;
		public static final int MARK = 3;

		int state;
		OperationInfo info;

		public Update(int state, OperationInfo info) {
			this.state = state;
			this.info = info;
		}

		@Override
		public String toString() {
			return "STATE: " + state + " INFO: " + ((info != null) ? info.toString() : "NULL");
		}
	}


	/**
	 * Operation information used by threads to help each other in completing
	 * the update operation on the tree
	 */
	interface OperationInfo {
	}


	/**
	 * Describes an insert operation
	 */
	class InsertInfo implements OperationInfo {

		InternalNode p;
		InternalNode newInternal;
		Leaf l;

		public InsertInfo(InternalNode p, InternalNode newInternal, Leaf l) {
			this.p = p;
			this.newInternal = newInternal;
			this.l = l;
		}

		@Override
		public String toString() {
			return "HELPINSERT: p=" + p + ", newSub=" + newInternal + ", l=" + l;
		}
	}

	/**
	 * Describes a delete operation
	 */
	class DeleteInfo implements OperationInfo {
		InternalNode gp;
		InternalNode p;
		Leaf l;
		Update pupdate;

		public DeleteInfo(InternalNode gp, InternalNode p, Leaf l, Update pupdate) {
			this.gp = gp;
			this.p = p;
			this.l = l;
			this.pupdate = pupdate;
		}

		@Override
		public String toString() {
			return "HELPDELETE: gp=" + gp + ", p=" + p + ", l=" + l;
		}
	}
	
	/**
	 * METHODS FOR SET INTERFACE COMPLIANCE
	 */

	@Override
	public boolean add(K e) {
		return insert(e);
	}

	@Override
	public Iterator<K> iterator() {
		return new TreeIterator();
	}

	@Override
	public int size() {
		return size.get();
	}

	class TreeIterator implements Iterator<K> {

		final List<K> snapshot = new ArrayList<K>();
		final Iterator<K> listIter;

		private K last;

		public TreeIterator() {
			visit(root);
			this.listIter = snapshot.iterator();
		}

		/**
		 * In-order visit to get a snapshot of the leaves
		 */
		@SuppressWarnings("unchecked")
		private void visit(Node n) {
			if (n instanceof NonBlockingTree.InternalNode)
				visit(((InternalNode) n).getLeft());

			if (n instanceof NonBlockingTree.Leaf && !(n == DUMMY_NODE1 || n == DUMMY_NODE2))
				snapshot.add(n.key);

			if (n instanceof NonBlockingTree.InternalNode)
				visit(((InternalNode) n).getRight());
		}

		@Override
		public boolean hasNext() {
			return listIter.hasNext();
		}

		@Override
		public K next() {
			last = listIter.next();
			return last;
		}

		@Override
		public void remove() {
			NonBlockingTree.this.delete(last);
		}
	}
}