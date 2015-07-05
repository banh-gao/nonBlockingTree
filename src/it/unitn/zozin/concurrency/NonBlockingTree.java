package it.unitn.zozin.concurrency;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
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
	final InternalNode<K> root;

	final K dummyKey1;
	final K dummyKey2;

	/**
	 * Creates a new instance of the tree
	 * 
	 * @param dummyKey1
	 *            First dummy key to be used. It has to be lower than dummyKey2
	 *            but higher that any other key.
	 * @param dummyKey2
	 *            Second dummy key to be used. It has to be greater than all
	 *            other keys.
	 */
	public NonBlockingTree(K dummyKey1, K dummyKey2) {
		if (dummyKey1 == null || dummyKey2 == null)
			throw new NullPointerException();
		if (dummyKey1.compareTo(dummyKey2) >= 0)
			throw new IllegalArgumentException();

		this.dummyKey1 = dummyKey1;
		this.dummyKey2 = dummyKey2;

		this.root = new InternalNode<K>(dummyKey2, new Leaf<K>(dummyKey1), new Leaf<K>(dummyKey2));
	}

	// Stores results of a search
	private class SearchRes {

		InternalNode<K> gp;
		InternalNode<K> p;
		Leaf<K> l;
		OperationInfo pinfo;
		int pstate;
		OperationInfo gpinfo;
		int gpstate;
	}

	/**
	 * Search from root to leaf for the given key
	 */
	private SearchRes search(K key) {
		SearchRes r = new SearchRes();

		Node<K> cur = root;

		int[] state = new int[1];
		while (cur instanceof NonBlockingTree.InternalNode) {
			// Remember parent of p
			r.gp = r.p;
			// Remember parent of l
			r.p = (InternalNode<K>) cur;
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

		r.l = (Leaf<K>) cur;

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
		if (key == null)
			throw new NullPointerException();
		if (key.compareTo(dummyKey1) >= 0)
			throw new IllegalArgumentException();

		SearchRes r = search(key);
		return r.l.key.compareTo(key) == 0;
	}

	/**
	 * Insert the given element in the tree, if not already present. This method
	 * is lock-free and its completion time depends on the contention level of
	 * the node that will become the parent of the new element.
	 * 
	 * @param key
	 *            The element to insert
	 * @return True if the element was inserted, false if it was already in the
	 *         tree
	 */
	@SuppressWarnings("unchecked")
	public boolean insert(K key) {
		if (key == null)
			throw new NullPointerException();
		if (key.compareTo(dummyKey1) >= 0)
			throw new IllegalArgumentException();

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

			InternalNode<K> newInternal = createSubTree(r.l, key);

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
	private InternalNode<K> createSubTree(Node<K> siblingNode, K newKey) {
		Node<K> left, right;
		K max;

		if (siblingNode.key.compareTo(newKey) < 0) {
			left = new Leaf<K>(siblingNode.key);
			right = new Leaf<K>(newKey);
			max = newKey;
		} else {
			// siblingNode == newKey not possible since duplicated key are not
			// admitted
			left = new Leaf<K>(newKey);
			right = new Leaf<K>(siblingNode.key);
			max = siblingNode.key;
		}

		return new InternalNode<K>(max, left, right);
	}

	/**
	 * Tries to complete the given insert operation on the tree
	 */
	private void helpInsert(InsertInfo insertOp) {
		// Substitute either left or right child of the parent tree with the new
		// subtree
		if (insertOp.p.spliceChild(insertOp.l, insertOp.newInternal)) {

			// Reset parent flag
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
		if (key == null)
			throw new NullPointerException();
		if (key.compareTo(dummyKey1) >= 0)
			throw new IllegalArgumentException();

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

	/**
	 * Tries to complete start the given delete operation on the tree
	 */
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

	/**
	 * Tries to complete the given delete operation on the tree
	 */
	private void helpMarked(DeleteInfo deleteOp) {
		Node<K> other;

		// Set other to point to the sibling of the node to remove (deleteOp.l)
		if (deleteOp.p.getRight() == deleteOp.l)
			other = deleteOp.p.getLeft();
		else
			other = deleteOp.p.getRight();

		// Set the sibling of the node to remove, as a child of the grandparent
		if (deleteOp.gp.spliceChild(deleteOp.p, other)) {

			// Reset grandparent flag
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
	static abstract class Node<K extends Comparable<K>> {

		final K key;

		private Node(K key) {
			this.key = key;
		}
	}

	/**
	 * An internal node with two children nodes and a status field used for
	 * concurrent updates.
	 */
	static class InternalNode<K extends Comparable<K>> extends Node<K> {

		// Atomic reference updaters used to update internal nodes' left and
		// right children
		@SuppressWarnings("rawtypes")
		private static final AtomicReferenceFieldUpdater<InternalNode, Node> leftUpdater = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Node.class, "left");
		@SuppressWarnings("rawtypes")
		private static final AtomicReferenceFieldUpdater<InternalNode, Node> rightUpdater = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Node.class, "right");

		// Left child (child key < this key)
		private volatile Node<K> left;
		// Right child (this key < child key)
		private volatile Node<K> right;

		// Update status of the node (ongoing insert or delete operation)
		private AtomicStampedReference<OperationInfo> update = new AtomicStampedReference<OperationInfo>(null, OperationInfo.CLEAN);

		private InternalNode(K key, Node<K> left, Node<K> right) {
			super(key);
			this.left = left;
			this.right = right;
		}

		/**
		 * Atomically tries to replace one of the children of the node
		 */
		public boolean spliceChild(Node<K> expectedNode, Node<K> newNode) {
			if (newNode.key.compareTo(key) < 0)
				return casLeft(expectedNode, newNode);
			else
				return casRight(expectedNode, newNode);
		}

		private boolean casLeft(Node<K> cmp, Node<K> val) {
			return leftUpdater.compareAndSet(this, cmp, val);
		}

		private boolean casRight(Node<K> cmp, Node<K> val) {
			return rightUpdater.compareAndSet(this, cmp, val);
		}

		/**
		 * Atomically tries to set the update state and operation info of the
		 * node
		 */
		public boolean setUpdate(OperationInfo expInfo, int expState, OperationInfo newInfo, int newState) {
			return this.update.compareAndSet(expInfo, newInfo, expState, newState);
		}

		public Node<K> getLeft() {
			return left;
		}

		public Node<K> getRight() {
			return right;
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
	static class Leaf<K extends Comparable<K>> extends Node<K> {

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

		InternalNode<K> p;
		InternalNode<K> newInternal;
		Leaf<K> l;

		public InsertInfo(InternalNode<K> p, InternalNode<K> newInternal, Leaf<K> l) {
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

		InternalNode<K> gp;
		InternalNode<K> p;
		Leaf<K> l;
		OperationInfo pinfo;
		int pstate;

		public DeleteInfo(InternalNode<K> gp, InternalNode<K> p, Leaf<K> l, OperationInfo pinfo, int pstate) {
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

	/**
	 * Returns the number of elements in this tree. If this tree
	 * contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
	 * <tt>Integer.MAX_VALUE</tt>.
	 * <p>Beware that, unlike in most collections, this method is <em>NOT</em> a
	 * constant-time operation. Because of the asynchronous nature of these
	 * trees, determining the current number of elements requires an O(n)
	 * traversal.
	 * 
	 * @return the number of elements in this tree
	 */
	@Override
	public int size() {
		int count = 0;
		Iterator<K> i = iterator();
		while (i.hasNext()) {
			i.next();
			if (++count == Integer.MAX_VALUE)
				break;
		}
		return count;
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
		 * Get a leaves snapshot with an in-order visit of the tree (wait-free
		 * and upper bounded in O(n))
		 */
		private void visit(Node<K> n) {
			if (n instanceof NonBlockingTree.InternalNode)
				visit(((InternalNode<K>) n).getLeft());

			if (n instanceof NonBlockingTree.Leaf && !isDummy(n.key))
				snapshot.add(n.key);

			if (n instanceof NonBlockingTree.InternalNode)
				visit(((InternalNode<K>) n).getRight());
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
