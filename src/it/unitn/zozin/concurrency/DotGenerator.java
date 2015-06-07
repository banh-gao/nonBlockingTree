package it.unitn.zozin.concurrency;

import java.util.ArrayList;
import java.util.List;
import it.unitn.zozin.concurrency.Node.InternalNode;
import it.unitn.zozin.concurrency.Node.Leaf;
import it.unitn.zozin.concurrency.NonBlockingTree.TreeVisitor;

public class DotGenerator {

	public static String generate(NonBlockingTree<Integer> t) {
		final StringBuilder b = new StringBuilder();
		b.append("graph {\n");

		t.preVisit(new TreeVisitor<Integer>() {

			List<String> s = new ArrayList<String>();
			
			@Override
			public void visit(Node<Integer> node, int level) {
				if (node instanceof InternalNode) {
					s.add(level, getLabel(node));
				} else if (node instanceof Leaf) {
					for(int i = 0; i < level; i++) {
						b.append(s.get(i));
						b.append(" -- ");
					}
					b.append(getLabel(node));
					b.append(";\n");
				}
			}
		});
		b.append("}");
		return b.toString();
	}

	static String getLabel(Node<Integer> node) {
		if (isRoot(node))
			return "ROOT";
		else if (isDummy(node))
			return (node.key.compareTo(Main.D1) == 0) ? "DUMMY1" : "DUMMY2";
		else
			return (node instanceof InternalNode) ? "I" + node.key.toString() : "L" + node.key.toString();
	}

	static boolean isRoot(Node<Integer> n) {
		return (n instanceof Node.InternalNode && n.key.equals(Main.D2));
	}

	static boolean isDummy(Node<Integer> n) {
		return (n instanceof Node.DummyNode);
	}
}
