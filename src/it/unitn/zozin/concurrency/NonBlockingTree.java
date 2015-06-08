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

	// Root node of the tree
	final InternalNode root;
	final K dummyKey1;
	final K dummyKey2;

	// Leaves counter
	private final AtomicInteger size = new AtomicInteger();

	// Stores results of a search
	private class SearchRes {

		InternalNode gp;
		InternalNode p;
		Leaf l;
		OperationInfo pinfo;
		int pstate;
		OperationInfo gpinfo;
		int gpstate;
	}

	/**
	 * Creates a new instance of the tree
	 * 
	 * @param dummyKey1
	 *            First dummy key to be used. It has to be lower than dummyKey2
	 * @param dummyKey2
	 *            Second dummy key to be used. It has to be greater than
	 *            dummyKey1
	 * @return
	 */
	public static <K extends Comparable<K>> NonBlockingTree<K> getInstance(K dummyKey1, K dummyKey2) {
		return new NonBlockingTree<K>(dummyKey1, dummyKey2);
	}

	private NonBlockingTree(K dummyKey1, K dummyKey2) {
		if (dummyKey1 == null || dummyKey2 == null)
			throw new NullPointerException();
		if (dummyKey1.compareTo(dummyKey2) >= 0)
			throw new IllegalArgumentException();

		this.dummyKey1 = dummyKey1;
		this.dummyKey2 = dummyKey2;

		Leaf dummy1 = getLeafInstance(dummyKey1);
		Leaf dummy2 = getLeafInstance(dummyKey2);

		this.root = new InternalNode(dummyKey2, dummy1, dummy2);
	}

	/**
	 * Search from root to leaf for the given key
	 */
	@SuppressWarnings("unchecked")
	private SearchRes search(K key) {
		SearchRes r = new SearchRes();

		Node cur = root;

		int[] state = new int[1];
		while (cur instanceof NonBlockingTree.InternalNode) {
			// Remember parent of p
			r.gp = r.p;
			// Remember parent of l
			r.p = (InternalNode) cur;
			// Remember update field of gp
			r.gpinfo = r.pinfo;
			r.gpstate = r.pstate;

			// Remember update field of p
			r.pinfo = r.p.getUpdate(state);
			r.pstate = state[0];

			// Move down to appropriate child
			if (key.compareTo(cur.key) < 0)
				cur = r.p.getLeft();
			else
				cur = r.p.getRight();
		}

		r.l = (Leaf) cur;

		return r;
	}

	/**
	 * Search the given element in the tree. This method is wait-free and its
	 * completion time is in O(n) where n is the number of elements stored in
	 * the tree.
	 * 
	 * @param key
	 *            The element to insert
	 * @return True if the element was inserted, false if it was already in the
	 *         tree
	 */
	public boolean find(K key) {
		SearchRes r = search(key);
		return r.l.key.compareTo(key) == 0;
	}

	/**
	 * Insert the given element in the tree, if not already present. This method
	 * is lock-free and its
	 * completion time depends on the contention level on the tree branch on
	 * which the element has to be stored.
	 * 
	 * @param key
	 *            The element to insert
	 * @return True if the element was inserted, false if it was already in the
	 *         tree
	 */
	@SuppressWarnings("unchecked")
	public boolean insert(K key) {
		OperationInfo opInfo;
		int[] state = new int[1];

		while (true) {
			SearchRes r = search(key);

			// Cannot insert duplicate key
			if (r.l.key.compareTo(key) == 0)
				return false;

			// Node already flagged, help the other and then retry the insert
			if (r.pstate != OperationInfo.CLEAN) {
				help(r.pinfo, r.pstate);
				continue;
			}

			InternalNode newInternal = createSubTree(r.l, key);

			opInfo = new InsertInfo(r.p, newInternal, r.l);

			// Try to set parent flag to insert flag
			if (r.p.setUpdate(r.pinfo, r.pstate, opInfo, OperationInfo.IFLAG)) {
				// IFLAG successful, finish insertion
				helpInsert((NonBlockingTree<K>.InsertInfo) opInfo);
				return true;
			} else {
				// IFLAG failed, help who caused the failure
				opInfo = r.p.getUpdate(state);
				help(opInfo, state[0]);
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

	/**
	 * Creates a new leaf object
	 */
	private Leaf getLeafInstance(K key) {
		return new Leaf(key);
	}

	private void helpInsert(InsertInfo insertOp) {
		// Substitute either left or right child of the parent tree with the new
		// subtree
		if (insertOp.p.spliceChild(insertOp.l, insertOp.newInternal)) {

			// Update size counter
			size.getAndIncrement();

			// Reset parent flag and unset reference to info record
			insertOp.p.setUpdate(insertOp, OperationInfo.IFLAG, insertOp, OperationInfo.CLEAN);
		}
	}

	/**
	 * Deletes the given element from the tree. This method is lock-free and its
	 * completion time depends on the contention level on the tree branch on
	 * which the element is stored.
	 * 
	 * @param key
	 *            The element to delete
	 * @return True if the element was deleted, false if it was not in the tree
	 */
	@SuppressWarnings("unchecked")
	public boolean delete(K key) {
		OperationInfo opInfo;
		int[] state = new int[1];

		while (true) {
			SearchRes r = search(key);

			// Key is not in the tree
			if (r.l.key.compareTo(key) != 0)
				return false;

			// If the grandparent state is not clean, help and then retry delete
			if (r.gpstate != OperationInfo.CLEAN) {
				help(r.gpinfo, r.gpstate);
				continue;
			}

			// If the parent state is not clean, help and then retry delete
			if (r.gpstate != OperationInfo.CLEAN) {
				help(r.gpinfo, r.gpstate);
				continue;
			}

			opInfo = new DeleteInfo(r.gp, r.p, r.l, r.pinfo, r.pstate);

			// Try to set grandparent flag to delete flag
			if (r.gp.setUpdate(r.gpinfo, r.gpstate, opInfo, OperationInfo.DFLAG)) {
				// DFLAG successful, if also the marking succeeds returns,
				// otherwise retry delete
				if (helpDelete((NonBlockingTree<K>.DeleteInfo) opInfo))
					return true;
			} else {
				// DFLAG failed, help who caused the failure
				opInfo = r.gp.getUpdate(state);
				help(opInfo, state[0]);
			}
		}
	}

	private boolean helpDelete(DeleteInfo deleteOp) {
		// Try to mark the parent of the node to delete
		if (deleteOp.p.setUpdate(deleteOp.pinfo, deleteOp.pstate, deleteOp, OperationInfo.MARK)) {
			// MARK successful, finish deletion
			helpMarked(deleteOp);
			return true;
		} else {
			// MARK failed, help who caused the failure
			int[] ongoingState = new int[1];
			OperationInfo ongoingOp = deleteOp.p.getUpdate(ongoingState);
			help(ongoingOp, ongoingState[0]);

			// Backtrack CAS: the delete has to be retried starting by retrying
			// to set the grandparent DFLAG
			ongoingOp = deleteOp.gp.getUpdate(ongoingState);
			deleteOp.gp.setUpdate(ongoingOp, ongoingState[0], deleteOp, OperationInfo.CLEAN);

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
			deleteOp.gp.setUpdate(deleteOp, OperationInfo.DFLAG, deleteOp, OperationInfo.CLEAN);
		}
	}

	/**
	 * Helps to perform the given update operation
	 */
	@SuppressWarnings("unchecked")
	private void help(OperationInfo ongoingOp, int ongoingState) {
		switch (ongoingState) {
			case OperationInfo.IFLAG :
				helpInsert((InsertInfo) ongoingOp);
				break;
			case OperationInfo.DFLAG :
				helpDelete((DeleteInfo) ongoingOp);
				break;
			case OperationInfo.MARK :
				helpMarked((DeleteInfo) ongoingOp);
				break;
		}
	}

	/**
	 * A generic keyed node entity that forms the tree in memory
	 */
	abstract class Node {

		public final K key;

		Node(K key) {
			this.key = key;
		}
	}

	/**
	 * An internal node with two children nodes and a status field used for
	 * concurrent updates.
	 */
	class InternalNode extends Node {

		// Left child
		private AtomicReference<Node> left = new AtomicReference<Node>();
		// Right child
		private AtomicReference<Node> right = new AtomicReference<Node>();
		// Update status of the node (ongoing insert or delete operation)
		private AtomicStampedReference<OperationInfo> update = new AtomicStampedReference<OperationInfo>(null, OperationInfo.CLEAN);

		private InternalNode(K key, Node left, Node right) {
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
		public boolean setUpdate(OperationInfo expInfo, int expState, OperationInfo newInfo, int newState) {
			return this.update.compareAndSet(expInfo, newInfo, expState, newState);
		}

		public Node getLeft() {
			return left.get();
		}

		public Node getRight() {
			return right.get();
		}

		public OperationInfo getUpdate(int[] state) {
			return update.get(state);
		}

		@Override
		public String toString() {
			return "Internal " + key.toString();
		}
	}

	/**
	 * A leaf node that stores the actual value in the tree.
	 */
	class Leaf extends Node {

		private Leaf(K key) {
			super(key);
		}

		@Override
		public String toString() {
			return "Leaf " + key.toString();
		}
	}

	/**
	 * Operation information used by threads to help each other in completing
	 * an update operation on the tree
	 */
	interface OperationInfo {

		public static final int CLEAN = 0;
		public static final int IFLAG = 1;
		public static final int DFLAG = 2;
		public static final int MARK = 3;
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
		OperationInfo pinfo;
		int pstate;

		public DeleteInfo(InternalNode gp, InternalNode p, Leaf l, OperationInfo pinfo, int pstate) {
			this.gp = gp;
			this.p = p;
			this.l = l;
			this.pinfo = pinfo;
			this.pstate = pstate;
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
		 * Get a leaves snapshot with an in-order visit of the tree (wait-free and upper bounded in O(n))
		 */
		@SuppressWarnings("unchecked")
		private void visit(Node n) {
			if (n instanceof NonBlockingTree.InternalNode)
				visit(((InternalNode) n).getLeft());

			if (n instanceof NonBlockingTree.Leaf && !isDummy(n.key))
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
		
		private boolean isDummy(K key) {
			return key.compareTo(dummyKey1) == 0 || key.compareTo(dummyKey2) == 0;
		}
	}
}