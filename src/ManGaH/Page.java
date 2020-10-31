package ManGaH;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.ObjectInputStream.GetField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;
import ManGaH.Table;

// TouchDate
//page is file
//Primary key -> 0 Index
// get method --> key -> value

public class Page implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	final int pSize = DBApp.PSize; //// take it from DBApp.prop
	int noRows;
	String name;
	String tablename;
	Vector<Vector> records = new Vector<Vector>();
	Hashtable columnNameIndex = new Hashtable();
	int row; // fake size
	int column; // fake size
	String Key; // Clusterd KEY
	int type; // passing type of key from table 0->String , 1->Integer , 2->Double , 3->Date ,
				// 4->Polygon

	public Vector getTuple(Hashtable x) {
		Vector tuple = new Vector();
		tuple.setSize(column);
		int index;

		for (Object i : x.keySet()) {

			index = (int) columnNameIndex.get(i);
			tuple.set(index, x.get(i));
		}
		return tuple;
	}

	public Page(String name, String Key, String tablename, String[] colName, int type) {
		this.name = name;
		this.tablename = tablename;
		this.Key = Key;
		row = pSize;
		this.type = type;

		for (int i = 0; i < colName.length; i++) {
			columnNameIndex.put(colName[i], i);
		}
		column = colName.length;

	}

	public Hashtable getHashtable(Vector x) {
		Hashtable temp = new Hashtable();

		for (Object i : columnNameIndex.keySet()) {

			temp.put(i, x.get((int) columnNameIndex.get(i)));

		}
		return temp;
	}

	public int getpSize() {
		return pSize;
	}

	public Object getLast() {
		return records.lastElement().get(0);
	}

	public Vector getLastTuple() {
		return records.lastElement();
	}

	public void savePage(String path) // path = tablename
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

	public void getRecords() {

		for (int i = 0; i < records.size(); i++) {
			System.out.println(records.get(i));
		}
	}

	public void insert(Hashtable x) {
		Vector tuple = new Vector();
		tuple.setSize(column);
		int index;

		for (Object i : x.keySet()) {

			index = (int) columnNameIndex.get(i);
			tuple.set(index, x.get(i));
		}
		records.add(noRows, tuple);
		noRows++;
		Collections.sort(records, new Sort());

	}

	public void delete(Hashtable x) {

		Vector<Object> tuple = new Vector<Object>();
		tuple.setSize(column);
		int index;

		for (Object i : x.keySet()) {
			index = (int) columnNameIndex.get(i);
			tuple.set(index, x.get(i));
		}

		for (int i = 0; i < records.size(); i++) {
			boolean f = true;
			for (Object x1 : x.keySet()) {
				int hello = (int) columnNameIndex.get(x1);
				if ((tuple.get(hello).getClass().toString().equals("class java.awt.Polygon"))) {
					Polygon p1 = (Polygon) tuple.get(hello);
//					System.out.println(p1.xpoints[0]);
					if (Arrays.equals(p1.xpoints, ((Polygon) records.get(i).get(hello)).xpoints)&& Arrays.equals(p1.ypoints, ((Polygon) records.get(i).get(hello)).ypoints)) {
						continue;
					} else {
						f = false;
					}
				}
				if (records.get(i).get(hello).equals(tuple.get(hello))) {

				} else {
					f = false;
				}
			}
			if (f) {

				records.remove(records.get(i));
				noRows--;
				i--;
			}
		}
	}

	public boolean deleteIndex(Hashtable x, int row_Index) {
		Vector<Object> tuple = new Vector<Object>();
		tuple.setSize(column);
		int index;
		for (Object i : x.keySet()) {
			index = (int) columnNameIndex.get(i);
			tuple.set(index, x.get(i));
		}
		while (tuple.contains(null)) {
			tuple.remove(null);
		}

		if (records.get(row_Index).containsAll(tuple)) {
			records.remove(row_Index);
			noRows--;
			return true;
		} else {
			return false;
		}
	}

	public boolean deleteIndexv2(Hashtable x) {
		Vector<Object> tuple = new Vector<Object>();
		tuple.setSize(column);
		int index;
		boolean flag = false;
		for (Object i : x.keySet()) {
			index = (int) columnNameIndex.get(i);
			tuple.set(index, x.get(i));
		}

		for (int i = 0; i < records.size(); i++) {

			boolean f = true;
			for (Object x1 : x.keySet()) {
				int hello = (int) columnNameIndex.get(x1);
				if (records.get(i).get(hello).equals(tuple.get(hello))) {

				} else {
					f = false;
				}
			}
			if (f) {

				records.remove(records.get(i));
				flag = true;
				noRows--;
				i--;
			}

		}
		return flag;
	}

	public int getIndex(String key) throws ParseException // Binary Search to get the index of a primary key
	{

		int high = noRows - 1; // length -1
		int low = 0;
		int ans = -1;
		while (high >= low) {
			int mid = (high + low) / 2;
			if (type == 0) {
				if (records.get(mid).get(0).equals(key)) {
					ans = mid;
					break;
				}
				String s = records.get(mid).get(0) + "";
				if (s.compareTo(key) > 0) {
					high = mid - 1;
				} else {
					low = mid + 1;
				}

			} else if (type == 1) {
				if ((int) records.get(mid).get(0) == Integer.parseInt(key)) {
					ans = mid;
					break;
				}
				if ((int) (records.get(mid).get(0)) > Integer.parseInt(key)) {
					high = mid - 1;
				} else {
					low = mid + 1;
				}
			} else if (type == 2) {
				if ((int) records.get(mid).get(0) == Double.parseDouble(key)) {
					ans = mid;
					break;
				}
				if ((int) (records.get(mid).get(0)) > Double.parseDouble(key)) {
					high = mid - 1;
				} else {
					low = mid + 1;
				}
			} else if (type == 3) {
				/*
				 * date must be in yyyy/MM/dd fixed because update call
				 */
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");

				if (records.get(mid).get(0).equals(sdf.parse(key))) {
					ans = mid;
					break;
				}
				Date s = (Date) records.get(mid).get(0);
				Date key1 = sdf.parse(key);
				if (s.compareTo(key1) > 0) {
					high = mid - 1;
				} else {
					low = mid + 1;
				}

			} else {

				StringTokenizer st = new StringTokenizer(key);
				ArrayList xx = new ArrayList();
				ArrayList yy = new ArrayList();
				while (st.hasMoreTokens()) {
					xx.add(st.nextToken("(,)"));
					yy.add(st.nextToken("(,)"));
				}

				int[] x = new int[xx.size()];
				int[] y = new int[xx.size()];

				for (int i = 0; i < xx.size(); i++) {
					x[i] = Integer.parseInt((String) xx.get(i));
					y[i] = Integer.parseInt((String) yy.get(i));
				}

				Polygon p1 = new Polygon(x, y, xx.size());
				for (int z : ((Polygon) records.get(mid).get(0)).xpoints) {
//					System.out.println(z);
				}
				if (Arrays.equals(x, ((Polygon) records.get(mid).get(0)).xpoints)
						&& Arrays.equals(y, ((Polygon) records.get(mid).get(0)).ypoints)) {

					ans = mid;
					break;
				}
				if (PolyCompare(p1, ((Polygon) records.get(mid).get(0))) < 0) {
					high = mid - 1;
				} else {
					low = mid + 1;
				}

			}

		}
		return ans;
	}

	public int UpperBound(String key) throws ParseException // Binary Search to get the index of a primary key
	{

		int high = noRows - 1; // length -1
		int low = 0;
		int ans = -1;
		while (high >= low) {
			int mid = (high + low) / 2;
			if (type == 0) {

				String s = records.get(mid).get(0) + "";
				if (s.compareTo(key) > 0) {
					high = mid - 1;

				} else {
					low = mid + 1;
					ans = mid;

				}

			} else if (type == 1) {

				if ((int) (records.get(mid).get(0)) > Integer.parseInt(key)) {
					high = mid - 1;

				} else {
					ans = mid;

					low = mid + 1;
				}
			} else if (type == 2) {
				if ((int) (records.get(mid).get(0)) > Double.parseDouble(key)) {

					high = mid - 1;
				} else {
					ans = mid;

					low = mid + 1;
				}
			} else if (type == 3) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
				Date s = (Date) records.get(mid).get(0);
				Date key1 = sdf.parse(key);
				if (s.compareTo(key1) > 0) {
					high = mid - 1;

				} else {
					ans = mid;

					low = mid + 1;
				}
			} else {

				StringTokenizer st = new StringTokenizer(key);
				ArrayList xx = new ArrayList();
				ArrayList yy = new ArrayList();
				while (st.hasMoreTokens()) {
					xx.add(st.nextToken("(,)"));
					yy.add(st.nextToken("(,)"));
				}

				int[] x = new int[xx.size()];
				int[] y = new int[xx.size()];
				for (int i = 0; i < xx.size(); i++) {
					x[i] = Integer.parseInt((String) xx.get(i));
					y[i] = Integer.parseInt((String) yy.get(i));
				}
				Polygon p1 = new Polygon(x, y, xx.size());
				if (PolyCompare(p1, ((Polygon) records.get(mid).get(0))) < 0) {

					high = mid - 1;
				} else {
					ans = mid;

					low = mid + 1;
				}

			}

		}
		return ans;
	}

	public int Poly_LowerBound(Polygon key) throws ParseException {
		int high = noRows - 1; // length -1
		int low = 0;
		int ans = -1;
		while (high >= low) {
			int mid = (high + low) / 2;
			Polygon p1 = key;
			if (PolyCompare(p1, ((Polygon) records.get(mid).get(0))) <= 0) {
				ans = mid;

				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
		return ans;
	}

	public int LowerBound(String key) throws ParseException // Binary Search to get the index of a primary key
	{

		int high = noRows - 1; // length -1
		int low = 0;
		int ans = -1;
		while (high >= low) {
			int mid = (high + low) / 2;
			if (type == 0) {

				String s = records.get(mid).get(0) + "";
				if (s.compareTo(key) >= 0) {
					high = mid - 1;
					ans = mid;

				} else {
					low = mid + 1;
				}

			} else if (type == 1) {

				if ((int) (records.get(mid).get(0)) >= Integer.parseInt(key)) {
					high = mid - 1;
					ans = mid;

				} else {
					low = mid + 1;
				}
			} else if (type == 2) {
				if ((int) (records.get(mid).get(0)) >= Double.parseDouble(key)) {
					ans = mid;

					high = mid - 1;
				} else {
					low = mid + 1;
				}
			} else if (type == 3) {

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
				Date s = (Date) records.get(mid).get(0);
				Date key1 = sdf.parse(key);
				if (s.compareTo(key1) >= 0) {
					high = mid - 1;
					ans = mid;

				} else {
					low = mid + 1;
				}
			} else {
//				System.out.println(key);
				StringTokenizer st = new StringTokenizer(key);
				ArrayList xx = new ArrayList();
				ArrayList yy = new ArrayList();
				while (st.hasMoreTokens()) {
					xx.add(st.nextToken("(,)"));
					yy.add(st.nextToken("(,)"));
				}

				int[] x = new int[xx.size()];
				int[] y = new int[xx.size()];
				for (int i = 0; i < xx.size(); i++) {
					x[i] = Integer.parseInt((String) xx.get(i));
					y[i] = Integer.parseInt((String) yy.get(i));
				}
				Polygon p1 = new Polygon(x, y, xx.size());
				if (PolyCompare(p1, ((Polygon) records.get(mid).get(0))) <= 0) {
					ans = mid;

					high = mid - 1;
				} else {
					low = mid + 1;
				}

			}

		}
		return ans;
	}

	public void Update(String key, Hashtable x) throws DBAppException {
		Vector tuple = new Vector(); // get new tuple
		tuple.setSize(column);
		int index;
		for (Object i : x.keySet()) {
			index = (int) columnNameIndex.get(i);
			tuple.set(index, x.get(i));
		}
		int k;
		try {
			k = LowerBound(key);

		} catch (ParseException e) {
			throw new DBAppException("Wrong date format");
		}
		if (k == -1) {
			throw new DBAppException("The key you are searching for doesn't exsits");
		}

		Vector old = records.get(k);

		for (int j = k; j < records.size() && old.get(0).equals(records.get(j).get(0)); j++) {
			for (int i = 0; i < tuple.size(); i++) {
				if (tuple.get(i) == null) {
					tuple.set(i, old.get(i));
				}
			}

			records.set(j, tuple);

		}

	}

	public void UpdateIndex(String keyValue, Hashtable x, int index_row) throws DBAppException {
		Vector tuple = new Vector(); // get old tuple
		tuple.setSize(column);
		int index;
		for (Object i : x.keySet()) {
			index = (int) columnNameIndex.get(i);
			tuple.set(index, x.get(i));
		}
		int keyIndex = (int) columnNameIndex.get(Key);
		Vector new_tuple = records.get(index_row);

		for (int i = 0; i < tuple.size(); i++) {
			if (tuple.get(i) == null) {
				tuple.set(i, new_tuple.get(i));
			}
		}
		/// fixed
//		if (type == 0) {
//			tuple.set(keyIndex, keyValue);
//		} else if (type == 1) {
//			int right_value = Integer.parseInt(keyValue);
//			tuple.set(keyIndex, right_value);
//
//		} else if (type == 2) {
//			double right_value = Double.parseDouble(keyValue);
//			tuple.set(keyIndex, right_value);
//		} else if (type == 3) {
//			                       // kindly check date and polygon don't forget
//
//		} else if (type == 4) {
//			// polygon
//
//		}

		records.set(index_row, tuple);

	}

	public boolean isFull() {
		return noRows == pSize;
	}

	public int PolyCompare(Polygon p1, Polygon p2) {
		Dimension dim = p1.getBounds().getSize();
		int area1 = dim.width * dim.height;
		Dimension dim2 = p2.getBounds().getSize();
		int area2 = dim2.width * dim2.height;
		return area1 - area2;
	}

	static class Sort implements Comparator<Vector> {
		// Used for sorting in ascending order of
		// roll number
		// will be updated soon.
		@Override
		public int compare(Vector o1, Vector o2) {
//         
			String x = o1.get(0).getClass().toString();
			if (x.equals("class java.lang.Integer")) {
				return (int) o1.get(0) - (int) o2.get(0);
			} else if (x.equals("class java.util.Date")) {
				return ((Date) (o1.get(0))).compareTo((Date) o2.get(0));
			} else if (x.equals("class java.lang.String")) {
				return ((String) (o1.get(0) + "")).compareTo(o2.get(0) + "");
			} else if (x.equals("class java.lang.Double")) {
				return ((Double) (o1.get(0))).compareTo((Double) o2.get(0));
			} else {
				Dimension dim = ((Polygon) (o1.get(0))).getBounds().getSize();
				int area1 = dim.width * dim.height;
				Dimension dim2 = ((Polygon) (o2.get(0))).getBounds().getSize();
				int area2 = dim2.width * dim2.height;
				return area1 - area2;

			}

		}
	}
}
