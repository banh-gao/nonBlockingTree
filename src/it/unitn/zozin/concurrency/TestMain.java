package it.unitn.zozin.concurrency;

import it.unitn.zozin.concurrency.NonBlockingTree.InternalNode;
import it.unitn.zozin.concurrency.NonBlockingTree.Leaf;
import it.unitn.zozin.concurrency.NonBlockingTree.Node;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMain {

	static int MAX_THREADS;
	static int MAX_VALUE;
	static int NUM_TASKS;

	static NonBlockingTree<Integer> tree = NonBlockingTree.getInstance(Integer.MAX_VALUE - 1, Integer.MAX_VALUE);

	static ExecutorService threadPool;
	static final Random r = new Random();
	static final AtomicInteger taskId = new AtomicInteger(1);
	static Runnable TREE_TASK = new Runnable() {

		@Override
		public void run() {
			int id = taskId.getAndIncrement();
			int v = r.nextInt(MAX_VALUE + 1);
			System.out.println(String.format("[TASK %s] INSERT(%s): %b", id, v, tree.insert(v)));
			System.out.println(String.format("[TASK %s] DELETE(%s): %b", id, v, tree.delete(v)));
			System.out.println(String.format("[TASK %s] FIND(%s): %b", id, v, tree.find(v)));
			System.out.println(String.format("[TASK %s] INSERT(%s): %b", id, v, tree.insert(v)));
		}
	};

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.out.println("Usage: " + new java.io.File(TestMain.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getName() + " <numTasks> <numThreads> <maxValue> <outFile>");
			System.out.println("numTasks\tNumber of tasks to execute. Each task performs 2 insert, 1 find and 1 delete.");
			System.out.println("numThreads\tNumber of threads to be used to run the tasks concurrently.");
			System.out.println("maxValue\tMaximum integer value to be inserted in the tree. The values used for the test are randomly choosen in the interval [0-maxValue].");
			System.out.println("outFile \tFile where to save the tree in dot format at the end of the test.");
			return;
		}

		TestMain.NUM_TASKS = Integer.parseInt(args[0]);
		TestMain.MAX_THREADS = Integer.parseInt(args[1]);
		TestMain.MAX_VALUE = Integer.parseInt(args[2]);
		String OUT_FILE = args[3];

		runTest();

		generatePrintout(OUT_FILE);
	}

	private static void runTest() throws InterruptedException {
		threadPool = Executors.newFixedThreadPool(MAX_THREADS);
		System.out.println(String.format("Running %s operations using %s threads...", 5 * NUM_TASKS, MAX_THREADS));

		long start = System.currentTimeMillis();

		for (int i = 0; i < NUM_TASKS; i++)
			threadPool.execute(TREE_TASK);

		threadPool.shutdown();
		if (!threadPool.awaitTermination(100, TimeUnit.SECONDS)) {
			throw new IllegalStateException("Completion waiting timed out");
		}

		System.out.println(String.format("Total time:\t%s ms", System.currentTimeMillis() - start));
		System.out.println(String.format("Operations:\t%s", 5 * NUM_TASKS));
		System.out.println(String.format("Threads:\t%s", MAX_THREADS));
	}

	private static void generatePrintout(String file) throws IOException {
		System.out.println(String.format("Generating tree printout in %s ...", file));
		String g = GraphGenerator.generate(tree);
		Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"));
		writer.write(g);
		writer.close();
		System.out.println(String.format("Tree printout generated in %s.", file));
	}

}

class GraphGenerator {

	private static final String DUMMY1 = "∞₁";
	private static final String DUMMY2 = "∞₂";

	static final StringBuilder b = new StringBuilder();
	static final List<Integer> path = new ArrayList<Integer>();
	static int nextId = 0;
	private static NonBlockingTree<Integer> t;

	public static String generate(NonBlockingTree<Integer> t) {
		GraphGenerator.t = t;
		b.append("digraph {\n");

		visit(t.root, 0);

		b.append("}");
		return b.toString();
	}

	@SuppressWarnings("rawtypes")
	private static void visit(Node n, int level) {
		appendNode(n, level);
		if (n instanceof InternalNode) {
			visit(((InternalNode) n).getLeft(), level + 1);
			visit(((InternalNode) n).getRight(), level + 1);
		}
	}

	@SuppressWarnings("rawtypes")
	private static void appendNode(Node node, int level) {
		int id = nextId++;
		String label = genLabel(node);

		// Set node style
		b.append(id);
		if (node instanceof InternalNode) {
			b.append(" [label=\"" + label + "\",shape=circle];\n");
			if (path.size() < level)
				path.set(level, id);
			else
				path.add(level, id);
		} else if (node instanceof Leaf) {
			if (t.isDummy((Integer) node.key))
				b.append(" [label=\"" + label + "\",shape=box];\n");
			else
				b.append(" [label=\"" + label + "\",style=filled,fillcolor=lightgrey,shape=box];\n");
		}

		// Ignore path to root
		if (level == 0) {
			return;
		}

		// Print edge from parent to current node
		b.append(path.get(level - 1));
		b.append(" -> ");
		b.append(id);
		b.append(";\n");
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static String genLabel(Node node) {
		if (isRoot(node)) {
			System.out.println(node);
			return "R";
		} else if (node.key.compareTo(t.dummyKey1) == 0)
			return DUMMY1;
		else if (node.key.compareTo(t.dummyKey2) == 0)
			return DUMMY2;
		else
			return node.key.toString();
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static boolean isRoot(Node n) {
		return (n instanceof InternalNode && n.key.compareTo(t.dummyKey2) == 0);
	}
}