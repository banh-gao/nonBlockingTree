package it.unitn.zozin.concurrency;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import it.unitn.zozin.concurrency.NonBlockingTree.TreeVisitor;

public class Main {

	static final Integer D1 = Integer.MIN_VALUE;
	static final Integer D2 = Integer.MAX_VALUE;
	static final NonBlockingTree<Integer> t = new NonBlockingTree<Integer>(D1, D2);
	
	public static void main(String[] args) throws Exception {
		t.insert(2);
		t.insert(4);
		t.insert(7);
		t.insert(9);
		
		
		String g = DotGenerator.generate(t);
		System.out.println(g);
		Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("test.dot"), "utf-8"));
		writer.write(g);
		writer.close();
		Runtime.getRuntime().exec("dot ./test.dot -Tpng > ./img.png").waitFor();
	}
	

}