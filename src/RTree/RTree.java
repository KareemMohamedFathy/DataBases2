package RTree;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

import BpTree.Ref;

import java.awt.Dimension;
//import java.awt.int;

public class RTree implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int order;
	private RTreeNode root;

	/**
	 * Creates an empty B+ tree
	 * 
	 * @param order the maximum number of keys in the nodes of the tree
	 */
	public RTree(int order) {
		this.order = order;
		root = new RTreeLeafNode(this.order);
		root.setRoot(true);
	}

	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * 
	 * @param key             the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 */
	///////////////////////////////////// insert
	public void insert(int key, Ref recordReference) {
		PushUp pushUp = root.insert(key, recordReference, null, -1);
		if (pushUp != null) {
			RTreeInnerNode newRoot = new RTreeInnerNode(order);
			newRoot.insertLeftAt(0, pushUp.key, root);
			newRoot.setChild(1, pushUp.newNode);
			root.setRoot(false);
			root = newRoot;
			root.setRoot(true);
		}
	}

	/**
	 * Looks up for the record that is associated with the specified key
	 * 
	 * @param key the key to find its record
	 * @return the reference of the record associated with this key
	 */
	public Ref search(int key) {
		return root.search(key);
	}

	public RTreeLeafNode searchmanga(int key) {

		return root.searchmanga(key);
	}

	/**
	 * Delete a key and its associated record from the tree.
	 * 
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it
	 *         was not in the tree
	 */
	public boolean delete(int key) {
		boolean done = root.delete(key, null, -1);
		// go down and find the new root in case the old root is deleted
		while (root instanceof RTreeInnerNode && !root.isRoot())
			root = ((RTreeInnerNode) root).getFirstChild();
		return done;
	}

	/**
	 * Returns a string representation of the B+ tree.
	 */
	public String toString() {

		// <For Testing>
		// node : (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		Queue<RTreeNode> cur = new LinkedList<RTreeNode>(), next;
		cur.add(root);
		while (!cur.isEmpty()) {
			next = new LinkedList<RTreeNode>();
			while (!cur.isEmpty()) {
				RTreeNode curNode = cur.remove();
				System.out.print(curNode);
				if (curNode instanceof RTreeLeafNode)
					System.out.print("->");
				else {
					System.out.print("{");
					RTreeInnerNode parent = (RTreeInnerNode) curNode;
					for (int i = 0; i <= parent.numberOfKeys; ++i) {
						System.out.print(parent.getChild(i).index + ",");
						next.add(parent.getChild(i));
					}
					System.out.print("} ");
				}

			}
			System.out.println();
			cur = next;
		}
		// </For Testing>
		return s;
	}

	public void R_saveTree(String path) // path = tablename+column
	{

		try {
			FileOutputStream fs = new FileOutputStream("data/" + path + ".class");
			ObjectOutputStream os = new ObjectOutputStream(fs);
			os.writeObject(this);
			os.close();
		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	
	
//	public static String ptoString(int p1) {
//		int[] x = p1.xpoints;
//		int[] y = p1.ypoints;
//		String s = "";
//		for (int i = 0; i < x.length; i++) {
//			s += "(" + x[i] + "," + y[i] + ")";
//		}
//		return s;
//	}
}
