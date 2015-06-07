import it.unitn.zozin.concurrency.NonBlockingTree;
import java.util.Set;



public class TestAPI {

	public static void main(String[] args) {
		Set<Integer> s = new NonBlockingTree<Integer>(Integer.MIN_VALUE, Integer.MAX_VALUE);
		s.add(3);
		s.add(4);
		s.add(5);
		s.add(6);
		s.add(7);
		System.out.println(s);
	}

}
