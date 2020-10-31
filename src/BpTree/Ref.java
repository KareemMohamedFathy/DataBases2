package BpTree;

import java.io.Serializable;
import java.util.ArrayList;

public class Ref implements Serializable {

	/**
	 * This class represents a pointer to the record. It is used at the leaves of
	 * the B+ tree
	 */
	private static final long serialVersionUID = 1L;
	private int pageNo, indexInPage;
	public ArrayList<Ref> Refs = new ArrayList<Ref>();

	public Ref(int pageNo, int indexInPage) {
		this.pageNo = pageNo;
		this.indexInPage = indexInPage;
	}

	/**
	 * @return the page at which the record is saved on the hard disk
	 */
	public int getPage() {
		return pageNo;
	}

	/**
	 * @return the index at which the record is saved in the page
	 */
	public int getIndexInPage() {
		return indexInPage;
	}

	public boolean check(Ref ref1) {
		return this.getPage() == ref1.getPage() && this.getIndexInPage() == ref1.getIndexInPage();
	}
}
