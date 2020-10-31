package ManGaH;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;
import BpTree.*;
import RTree.RTree;
import RTree.RTreeLeafNode;

import java.util.Iterator;

public class Table implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String tableName;
	int NumOfPages = 0;
	ArrayList<String> pages;

	Hashtable ColNameType; // COLNAME -> COLTYPE FETCHED FROM metadata
	int TreeOrder = DBApp.TreeOrder;
	String key; // clustering key
	int type;
	Hashtable<String, String> colNameIndex; // col->Tree
	Hashtable<String, String> R_colNameIndex; // ** for R_TREE R_TREE -> (R+TableName+ColName);

	public Table(String tableName, String key, Hashtable<String, String> htblColNameType) {
		pages = new ArrayList<String>();
		this.tableName = tableName;
		this.key = key;
		this.colNameIndex = new Hashtable<String, String>();
		this.R_colNameIndex = new Hashtable<String, String>();// 8yrt

		ColNameType = htblColNameType;
		ColNameType.put("TouchDate", "java.util.Date");

		for (Object i : htblColNameType.keySet()) {
			if (key.equals(i)) {
				String x = htblColNameType.get(i);
				if (x.equals("java.lang.String")) {
					type = 0;
				} else if (x.equals("java.lang.Integer")) {
					type = 1;
				} else if (x.equals("java.lang.Double")) {
					type = 2;
				} else if (x.equals("java.util.Date")) {
					type = 3;
				} else {
					type = 4;
				}
			}

		}

	}

	public void RTree_createIndeX(String strColName) throws DBAppException {
		// TODO handle R TREE
		try {
			if (!ColNameType.get(strColName).equals("java.awt.Polygon")) {// 8yrt
				throw new DBAppException("Wrong data type for RTree");
			} else {
				String tree_name = tableName + "" + strColName;
				R_colNameIndex.put(strColName, tree_name);
				createIndeX(strColName);

			}
		} catch (Exception e) {
			throw new DBAppException("column no exist");
		}
	}

	public ArrayList selectIndex(SQLTerm arrSQLTerms) {
		ArrayList<Object> result = new ArrayList<>();
//		System.out.println(ColNameType.get(arrSQLTerms._strColumnName));
		if(!ColNameType.get(arrSQLTerms._strColumnName).equals("java.awt.Polygon")) {
		BPTree tree = loadTree(tableName + arrSQLTerms._strColumnName);
		Comparable Value = (Comparable) arrSQLTerms._objValue;
		Page p1 = loadPage(pages.get(0));
		int key_index = (int) p1.columnNameIndex.get(arrSQLTerms._strColumnName);
		Comparable first_record = (Comparable) p1.records.get(0).get(key_index); // first record value in the table
		if (arrSQLTerms._strOperator.equals("=")) {
			Ref ref = tree.search(Value);
			if (ref != null) {

				for (int z = 0; z < ref.Refs.size(); z++) {
					int page_no = ref.Refs.get(z).getPage();
					int index = ref.Refs.get(z).getIndexInPage();
					Page p = loadPage(tableName + "" + page_no);
					result.add(p.records.get(index));
					p = null;

				}

			}

		} else if (arrSQLTerms._strOperator.equals("!=")) {
			BPTreeLeafNode node = tree.searchmanga(first_record);
			while (node != null) {
				for (int i = 0; i < node.numberOfKeys; i++) {
					if (node.keys[i].equals(Value)) {
						continue;
					}
					int target_index = node.findIndex(node.keys[i]) - 1;
					for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
						int page_no = node.getRecord(target_index).Refs.get(z).getPage();
						int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
						Page p = loadPage(tableName + "" + page_no);
						result.add(p.records.get(index));
						p = null;

					}

				}

				node = node.getNext();

			}

		} else if (arrSQLTerms._strOperator.equals(">")) {
			BPTreeLeafNode node = tree.searchmanga(Value);
			// BPTreeLeafNode node = tree.searchmanga(first_record);
			if (node == null) {

				node = tree.searchmanga(first_record);
			}
			while (node != null) {

				for (int i = 0; i < node.numberOfKeys; i++) {
					if (node.keys[i].compareTo(Value) == 1) {

						// find_index -> btgblk l index ele fl node bta3 l key
						// getRecord(index) -> get the ref of the record
						int target_index = node.findIndex(node.keys[i]) - 1;

						for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
							int page_no = node.getRecord(target_index).Refs.get(z).getPage();
							int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
							Page p = loadPage(tableName + "" + page_no);
							result.add(p.records.get(index));
							p = null;

						}

					}

				}
				node = node.getNext();
			}

		} else if (arrSQLTerms._strOperator.equals(">=")) {
			BPTreeLeafNode node = tree.searchmanga(Value);
			if (node == null) {

				node = tree.searchmanga(first_record);
			}
			while (node != null) {
				for (int i = 0; i < node.numberOfKeys; i++) {
					if (node.keys[i].compareTo(Value) == 1 | node.keys[i].compareTo(Value) == 0) {
						int target_index = node.findIndex(node.keys[i]) - 1;
						for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
							int page_no = node.getRecord(target_index).Refs.get(z).getPage();
							int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
							Page p = loadPage(tableName + "" + page_no);
							result.add(p.records.get(index));
							p = null;

						}
					}

				}
				node = node.getNext();
			}

		} else if (arrSQLTerms._strOperator.equals("<")) {
			BPTreeLeafNode node = tree.searchmanga(first_record);

			while (node != null) {
				for (int i = 0; i < node.numberOfKeys; i++) {
					if (node.keys[i].equals(Value)) {
						return result;
					}
					if (node.keys[i].compareTo(Value) == -1) {
						int target_index = node.findIndex(node.keys[i]) - 1;
						for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
							int page_no = node.getRecord(target_index).Refs.get(z).getPage();
							int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
							Page p = loadPage(tableName + "" + page_no);
							result.add(p.records.get(index));
							p = null;

						}
					}

				}

				node = node.getNext();

			}

		} else if (arrSQLTerms._strOperator.equals("<=")) {
			BPTreeLeafNode node = tree.searchmanga(first_record);
			while (node != null) {
				for (int i = 0; i < node.numberOfKeys; i++) {
					if (node.keys[i].equals(Value)) {
						int target_index = node.findIndex(node.keys[i]) - 1;
						for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
							int page_no = node.getRecord(target_index).Refs.get(z).getPage();
							int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
							Page p = loadPage(tableName + "" + page_no);
							result.add(p.records.get(index));
							p = null;

						}
						return result;
					}
					if (node.keys[i].compareTo(Value) == -1) {

						int target_index = node.findIndex(node.keys[i]) - 1;
						for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
							int page_no = node.getRecord(target_index).Refs.get(z).getPage();
							int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
							Page p = loadPage(tableName + "" + page_no);
							result.add(p.records.get(index));
							p = null;

						}
					}
				}

				node = node.getNext();
			}

		}
		}
		else {
			RTree tree = R_loadTree(tableName + arrSQLTerms._strColumnName);
		
			Page p1 = loadPage(pages.get(0));
			int key_index = (int) p1.columnNameIndex.get(arrSQLTerms._strColumnName);
			if (arrSQLTerms._strOperator.equals("=")) {
				Polygon poly = (Polygon) arrSQLTerms._objValue;
				int area=poly.getBounds().height*poly.getBounds().width;
				Ref ref = tree.search(area);
				if (ref != null) {

					for (int z = 0; z < ref.Refs.size(); z++) {
						int page_no = ref.Refs.get(z).getPage();
						int index = ref.Refs.get(z).getIndexInPage();
						Page p = loadPage(tableName + "" + page_no);
						Polygon polysearch=(Polygon) p.records.get(index).get(key_index);
						if(Arrays.equals(poly.xpoints, polysearch.xpoints)&&Arrays.equals(poly.ypoints, polysearch.ypoints))
								result.add(p.records.get(index));
						p = null;

					}

				}

			} else if (arrSQLTerms._strOperator.equals("!=")) {//llolzz mind fuck
				Polygon first_record_ = (Polygon) p1.records.get(0).get(key_index);
				Polygon Value = (Polygon) arrSQLTerms._objValue;
				Integer valuearea=Value.getBounds().height*Value.getBounds().width;

				int firstarea=first_record_.getBounds().height*first_record_.getBounds().width;
				RTreeLeafNode node = tree.searchmanga(firstarea);
//				System.out.println(node.keys[0]);
				while (node != null) {
					for (int i = 0; i < node.numberOfKeys; i++) {
						if (((Integer)node.keys[i]).equals(valuearea)) {
//							System.out.println(valuearea);
							Ref ref=tree.search((Integer)node.keys[i]);
							for(Ref r:ref.Refs) {
								Page p=loadPage(tableName+r.getPage());
								int ind=r.getIndexInPage();
								Polygon polytest=(Polygon) p.records.get(ind).get(key_index);
								if (!(Arrays.equals(Value.xpoints, polytest.xpoints)&& Arrays.equals(Value.ypoints, polytest.ypoints))) {
									result.add(p.records.get(ind));
								}
								
							}	
							continue;
						}
						int target_index = node.findIndex((int) node.keys[i]) - 1;
						for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
							int page_no = node.getRecord(target_index).Refs.get(z).getPage();
							int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
							Page p = loadPage(tableName + "" + page_no);
							result.add(p.records.get(index));
							p = null;

						}

					}

					node = node.getNext();

				}

			} 
			else if (arrSQLTerms._strOperator.equals(">")) {
				Polygon first_record_ = (Polygon) p1.records.get(0).get(key_index);
				int firstarea=first_record_.getBounds().height*first_record_.getBounds().width;
				
				Integer value=(Integer) arrSQLTerms._objValue;
				RTreeLeafNode node = tree.searchmanga(value);
				// BPTreeLeafNode node = tree.searchmanga(first_record);
				if (node == null) {

					node = tree.searchmanga(firstarea);
				}
				while (node != null) {

					for (int i = 0; i < node.numberOfKeys; i++) {
						if (((Integer) node.keys[i]).compareTo(value) == 1) {

							// find_index -> btgblk l index ele fl node bta3 l key
							// getRecord(index) -> get the ref of the record
							int target_index = node.findIndex((int) node.keys[i]) - 1;

							for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
								int page_no = node.getRecord(target_index).Refs.get(z).getPage();
								int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
								Page p = loadPage(tableName + "" + page_no);
								result.add(p.records.get(index));
								p = null;

							}

						}

					}
					node = node.getNext();
				}

			} else if (arrSQLTerms._strOperator.equals(">=")) {
				Polygon first_record_ = (Polygon) p1.records.get(0).get(key_index);
				int firstarea=first_record_.getBounds().height*first_record_.getBounds().width;
				
				Integer value=(Integer) arrSQLTerms._objValue;
				RTreeLeafNode node = tree.searchmanga(value);
				if (node == null) {

					node = tree.searchmanga(firstarea);
				}
				while (node != null) {
					for (int i = 0; i < node.numberOfKeys; i++) {
						if (((Integer) node.keys[i]).compareTo(value) == 1 | ((Integer) node.keys[i]).compareTo(value) == 0) {
							int target_index = node.findIndex((int) node.keys[i]) - 1;
							for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
								int page_no = node.getRecord(target_index).Refs.get(z).getPage();
								int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
								Page p = loadPage(tableName + "" + page_no);
								result.add(p.records.get(index));
								p = null;

							}
						}

					}
					node = node.getNext();
				}

			} else if (arrSQLTerms._strOperator.equals("<")) {
				Polygon first_record_ = (Polygon) p1.records.get(0).get(key_index);
				int firstarea=first_record_.getBounds().height*first_record_.getBounds().width;
				
				Integer value=(Integer) arrSQLTerms._objValue;
				RTreeLeafNode node = tree.searchmanga(firstarea);

				while (node != null) {
					for (int i = 0; i < node.numberOfKeys; i++) {
						if (((Integer) node.keys[i]).compareTo(value) == 0) {
							return result;
						}
						if (((Integer) node.keys[i]).compareTo(value) == -1) {
							int target_index = node.findIndex((int) node.keys[i]) - 1;
							for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
								int page_no = node.getRecord(target_index).Refs.get(z).getPage();
								int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
								Page p = loadPage(tableName + "" + page_no);
								result.add(p.records.get(index));
								p = null;

							}
						}

					}

					node = node.getNext();

				}

			} else if (arrSQLTerms._strOperator.equals("<=")) {
				Polygon first_record_ = (Polygon) p1.records.get(0).get(key_index);
				int firstarea=first_record_.getBounds().height*first_record_.getBounds().width;
				
				Integer value=(Integer) arrSQLTerms._objValue;
				RTreeLeafNode node = tree.searchmanga(firstarea);
				while (node != null) {
					for (int i = 0; i < node.numberOfKeys; i++) {
						if (((Integer) node.keys[i]).compareTo(value) == 0) {
							int target_index = node.findIndex((int) node.keys[i]) - 1;
							for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
								int page_no = node.getRecord(target_index).Refs.get(z).getPage();
								int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
								Page p = loadPage(tableName + "" + page_no);
								result.add(p.records.get(index));
								p = null;

							}
							return result;
						}
						if (((Integer) node.keys[i]).compareTo(value) == -1) {

							int target_index = node.findIndex((int) node.keys[i]) - 1;
							for (int z = 0; z < node.getRecord(target_index).Refs.size(); z++) {
								int page_no = node.getRecord(target_index).Refs.get(z).getPage();
								int index = node.getRecord(target_index).Refs.get(z).getIndexInPage();
								Page p = loadPage(tableName + "" + page_no);
								result.add(p.records.get(index));
								p = null;

							}
						}
					}

					node = node.getNext();
				}

			}
			
		}
		return result;

	}

	public String convert_Date_String(Object _objValue) {
		/*
		 * b convert el date -> String 2020/05/2;
		 */
		String Value = "";
		SimpleDateFormat formatNowDay = new SimpleDateFormat("dd");
		SimpleDateFormat formatNowMonth = new SimpleDateFormat("MM");
		SimpleDateFormat formatNowYear = new SimpleDateFormat("YYYY");
		String currentDay = formatNowDay.format(_objValue);
		String currentMonth = formatNowMonth.format(_objValue);
		String currentYear = formatNowYear.format(_objValue);
		Value = currentYear + "/" + currentMonth + "/" + currentDay;
		return Value;
	}
	public String convert_poly_String(Object _objValue) {
		/*
		 * b convert el date -> String 2020/05/2;
		 */
		String Value="";
		Polygon p=(Polygon) _objValue;
		int[] x=p.xpoints;
		int[] y=p.xpoints;
		for(int i=0;i<x.length;i++) {
			Value=Value+"("+x[i]+",";
			Value=Value+y[i]+"),";	
		}
		Value=Value.substring(0,Value.length()-1);
		return Value;
	}

	public ArrayList selectnoIndexHelper_unC(Page p, Object value, String colName, String op) {
		String gettype = (String) ColNameType.get(colName);
		ArrayList result = new ArrayList();
		int tuple_index = (int) p.columnNameIndex.get(colName); // get index fl colum to comparison
		if (op.equals("=")) {
			// polygon ??? 
			if(!gettype.equals("java.awt.Polygon")) {
			for (int i = 0; i < p.records.size(); i++) {{
				if (p.records.get(i).get(tuple_index).equals(value)) {
					result.add(p.records.get(i));
				}
			}}
			
			}
			else {
				for (int i = 0; i < p.records.size(); i++) {{
					Polygon poly=(Polygon) p.records.get(i).get(tuple_index);
					Polygon polyvalue=(Polygon) value;
					if (Arrays.equals(poly.xpoints,polyvalue.xpoints)&&Arrays.equals(poly.ypoints,polyvalue.ypoints)) {
						result.add(p.records.get(i));
					}
				}}
				
			}
		} else if (op.equals("!=")) {
			// polygon ???
//			System.out.println("hena");
			if(!gettype.equals("java.awt.Polygon")) {
			for (int i = 0; i < p.records.size(); i++) {
				if (!p.records.get(i).get(tuple_index).equals(value)) {
					result.add(p.records.get(i));
				}
			}
		}
			else {
				for (int i = 0; i < p.records.size(); i++) {{
					Polygon poly=(Polygon) p.records.get(i).get(tuple_index);
					Polygon polyvalue=(Polygon) value;
					if (!(Arrays.equals(poly.xpoints,polyvalue.xpoints)&&Arrays.equals(poly.ypoints,polyvalue.ypoints))) {
//						System.out.println("++++");
						result.add(p.records.get(i));
					}
				}}
				
			}
		} else if (op.equals(">")) {
//			System.out.println(gettype);
			if (value instanceof Integer && !gettype.equals("java.awt.Polygon")) {
				Integer val = (Integer) value;

				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Integer) p.records.get(i).get(tuple_index)) == -1) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof Double&&!gettype.equals("java.awt.Polygon")) {
				Double val = (Double) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Double) p.records.get(i).get(tuple_index)) == -1) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof String) {
				String val = (String) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((String) p.records.get(i).get(tuple_index)) == -1) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof Date) {
				Date val = (Date) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Date) p.records.get(i).get(tuple_index)) == -1) {
						result.add(p.records.get(i));
					}

				}

			} else if(gettype.equals("java.awt.Polygon")) {
				//handle polygon ysta 
				Integer val = (Integer) value;

				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					Polygon poly=(Polygon) p.records.get(i).get(tuple_index);
					Integer area =(Integer)poly.getBounds().height*poly.getBounds().width;
					if (val.compareTo(area) == -1) {
						result.add(p.records.get(i));
					}

				}

			}

		} else if (op.equals(">=")) {
			if (value instanceof Integer&&!gettype.equals("java.awt.Polygon")) {
				Integer val = (Integer) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Integer) p.records.get(i).get(tuple_index)) == -1
							| val.compareTo((Integer) p.records.get(i).get(tuple_index)) == 0) {

						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof Double&&!gettype.equals("java.awt.Polygon")) {
				Double val = (Double) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Double) p.records.get(i).get(tuple_index)) == -1
							| val.compareTo((Double) p.records.get(i).get(tuple_index)) == 0) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof String) {
				String val = (String) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((String) p.records.get(i).get(tuple_index)) == -1
							| val.compareTo((String) p.records.get(i).get(tuple_index)) == 0) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof Date) {
				Date val = (Date) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Date) p.records.get(i).get(tuple_index)) == -1
							| val.compareTo((Date) p.records.get(i).get(tuple_index)) == 0) {
						result.add(p.records.get(i));
					}

				}

			}  else if(gettype.equals("java.awt.Polygon")) {
				//handle polygon ysta 
				Integer val = (Integer) value;

				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					Polygon poly=(Polygon) p.records.get(i).get(tuple_index);
					Integer area =(Integer)poly.getBounds().height*poly.getBounds().width;
					if (val.compareTo((Integer) area) == -1
							| val.compareTo((Integer) area) == 0) {
						result.add(p.records.get(i));
					}

				}

			}


		} else if (op.equals("<")) {
			if (value instanceof Integer&&!gettype.equals("java.awt.Polygon")) {
				Integer val = (Integer) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Integer) p.records.get(i).get(tuple_index)) == 1) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof Double&&!gettype.equals("java.awt.Polygon")) {
				Double val = (Double) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Double) p.records.get(i).get(tuple_index)) == 1) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof String) {
				String val = (String) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((String) p.records.get(i).get(tuple_index)) == 1) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof Date) {
				Date val = (Date) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Date) p.records.get(i).get(tuple_index)) == 1) {
						result.add(p.records.get(i));
					}

				}

			} else if(gettype.equals("java.awt.Polygon")) {
				//handle polygon ysta 
				Integer val = (Integer) value;

				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					Polygon poly=(Polygon) p.records.get(i).get(tuple_index);
					Integer area =(Integer)poly.getBounds().height*poly.getBounds().width;
					if (val.compareTo(area) == 1) {
						result.add(p.records.get(i));
					}

				}

			}

		} else if (op.equals("<=")) {
			if (value instanceof Integer&&!gettype.equals("java.awt.Polygon")) {
				Integer val = (Integer) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Integer) p.records.get(i).get(tuple_index)) == 1
							| val.compareTo((Integer) p.records.get(i).get(tuple_index)) == 0) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof Double&&!gettype.equals("java.awt.Polygon")) {
				Double val = (Double) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Double) p.records.get(i).get(tuple_index)) == 1
							| val.compareTo((Double) p.records.get(i).get(tuple_index)) == 0) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof String) {
				String val = (String) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((String) p.records.get(i).get(tuple_index)) == 1
							| val.compareTo((String) p.records.get(i).get(tuple_index)) == 0) {
						result.add(p.records.get(i));
					}

				}

			} else if (value instanceof Date) {
				Date val = (Date) value;
				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					if (val.compareTo((Date) p.records.get(i).get(tuple_index)) == 1
							| val.compareTo((Date) p.records.get(i).get(tuple_index)) == 0) {
						result.add(p.records.get(i));
					}

				}

			} else if(gettype.equals("java.awt.Polygon")) {
				//handle polygon ysta 
				Integer val = (Integer) value;

				for (int i = 0; i < p.records.size(); i++) {
					// val > el bta3 = 1 , -1 < , 0 =
					Polygon poly=(Polygon) p.records.get(i).get(tuple_index);
					Integer area =(Integer)poly.getBounds().height*poly.getBounds().width;
					if (val.compareTo(area) == 1||val.compareTo((Integer) area) == 0) {
						result.add(p.records.get(i));
					}

				}

			}

		}

		return result;

	}

///>, >=, <, <=, != or =  OPERATORS 10
//  AND OR XOR OPERATORS
	public ArrayList selectnoIndex(SQLTerm arrSQLTerms) throws DBAppException {
		System.out.println("no");
		String colName = arrSQLTerms._strColumnName;
		String type = arrSQLTerms._objValue.getClass() + "";
		String Value = "";
		Polygon polyvalue=null;
		int tellyou_Date_poly = 0; // 1->date , 2->Poly
		if (type.equalsIgnoreCase("class java.util.date")) {
			tellyou_Date_poly = 1;
			Value = convert_Date_String(arrSQLTerms._objValue);
		} 
		else if(type.equalsIgnoreCase("class java.awt.polygon")){
			tellyou_Date_poly=2;
			Value = convert_poly_String(arrSQLTerms._objValue);
		}
		else {
			Value = arrSQLTerms._objValue + "";
		}

		boolean Clusterd = colName.equals(key);
		ArrayList result = new ArrayList(); // result that will be fetched
		if (arrSQLTerms._strOperator.equals("=")) {
			if (Clusterd) {
				for (String i : pages) {

					Page p = loadPage(i);
					int index;
					try {
						index = p.LowerBound(Value);
					} catch (ParseException e) {
						throw new DBAppException(e.getMessage());
					}

					if (index != -1) {
						if (this.type == 4) {
							StringTokenizer st = new StringTokenizer(Value);
							ArrayList xx = new ArrayList();
							ArrayList yy = new ArrayList();
							while (st.hasMoreTokens()) {
								xx.add(st.nextToken("(,)"));
								yy.add(st.nextToken("(,)"));
							}

							int[] x = new int[xx.size()];
							int[] y = new int[xx.size()];
							for (int axes = 0; axes < xx.size(); axes++) {
								x[axes] = Integer.parseInt((String) xx.get(axes));
								y[axes] = Integer.parseInt((String) yy.get(axes));
							}
							Polygon pvalue = new Polygon(x, y, xx.size());
							int area = pvalue.getBounds().height*pvalue.getBounds().width;
							
							for (int z = index ; z < p.records.size(); z++) {
								Polygon ptest = (Polygon) p.records.get(z).get(0);
								int areatest=ptest.getBounds().height*ptest.getBounds().width;
								if(areatest==area) {
								if(Arrays.equals(ptest.xpoints,pvalue.xpoints)&&Arrays.equals(ptest.ypoints,pvalue.ypoints)) {
									result.add(p.records.get(z));
								}
								}
								else {
									break;
								}

							}
						} else {
							Vector r = p.records.get(index);
							String s = "";
							if (tellyou_Date_poly == 1) {
								s = convert_Date_String(r.get(0));
							} else {
								s = "" + r.get(0); // what if polygon? // date

							}
							for (int j = index; j < p.records.size() && r.get(0).equals(p.records.get(j).get(0))
									&& s.equals(Value); j++) {

								result.add(p.records.get(j));
							}
						}
					}

					p = null;
				}

			} else {
				ArrayList<Object> r = new ArrayList<>();
				for (String i : pages) {
					Page p = loadPage(i);
					r = selectnoIndexHelper_unC(p, arrSQLTerms._objValue, colName, "=");
					for (Object z : r) {
						result.add(z);
					}

				}

			}

		} else if (arrSQLTerms._strOperator.equals("!=")) {
			ArrayList<Object> r = new ArrayList<>();
			for (String i : pages) {
				Page p = loadPage(i);
				r = selectnoIndexHelper_unC(p, arrSQLTerms._objValue, colName, "!=");
				for (Object z : r) {
					result.add(z);
				}

			}

		} else if (arrSQLTerms._strOperator.equals(">")) {//poly >20

			if (Clusterd) {
				for (String i : pages) {
					Page p = loadPage(i);
					int index;
					try {
						index = p.LowerBound(Value); //

					} catch (ParseException e) {
						throw new DBAppException(e.getMessage());
					}
					if (index != -1) {
						if (this.type == 3) {
							int pt = index;
							String s = convert_Date_String(p.records.get(index).get(0));
							for (int j = index + 1; j < p.records.size(); j++) {
								if (s.equals(Value)) {
									pt = j;
									s = convert_Date_String(p.records.get(j).get(0));
								} else {
									break;
								}
							}
							if (s.equals(Value)) {
								continue;
							}

							for (int x = pt; x < p.records.size(); x++) {

								result.add(p.records.get(x));
							}

						} else if(this.type!=4) {
							Vector r = p.records.get(index);
							int pt = index;
							String s = "" + p.records.get(index).get(0);
							for (int j = index + 1; j < p.records.size(); j++) {
								if (s.equals("" + Value)) {
									pt = j;
									s = "" + p.records.get(j).get(0);
								} else {
									break;
								}
							}
							if (s.equals(Value)) {
								continue;
							}

							for (int x = pt; x < p.records.size(); x++) {

								result.add(p.records.get(x));
							}

						}else {
							Vector r = p.records.get(index);
							int pt = index;
							Polygon poly=(Polygon) p.records.get(index).get(0);
							String s = "" + poly.getBounds().height*poly.getBounds().width;
							for (int j = index + 1; j < p.records.size(); j++) {
								if (s.equals("" + Value)) {
									pt = j;
									poly=(Polygon) p.records.get(j).get(0);
									s = "" + poly.getBounds().height*poly.getBounds().width;
								} else {
									break;
								}
							}
							if (s.equals(Value)) {
								continue;
							}

							for (int x = pt; x < p.records.size(); x++) {

								result.add(p.records.get(x));
							}
							
							
						}
						
							
						
					}
				}

			} else {

				ArrayList<Object> r = new ArrayList<>();
				for (String i : pages) {
					Page p = loadPage(i);
					r = selectnoIndexHelper_unC(p, arrSQLTerms._objValue, colName, ">");
					for (Object z : r) {
						result.add(z);
					}

				}
			}

		} 
		else if (arrSQLTerms._strOperator.equals(">=")) {
			if (Clusterd) {
				for (String i : pages) {
					Page p = loadPage(i);
					int index;
					try {
						index = p.LowerBound(Value);
					} catch (ParseException e) {
						throw new DBAppException(e.getMessage());
					}
					if (index != -1) {
						if(this.type!=4) {
						Vector r = p.records.get(index);
						for (int x = index; x < p.records.size(); x++) {
							result.add(p.records.get(x));
						}
						}
						else {
							Vector r = p.records.get(index);
//							Polygon poly=(Polygon) p.records.get(index).get(0);
//							String s = "" + poly.getBounds().height*poly.getBounds().width;
							for (int j = index + 1; j < p.records.size(); j++) {
								result.add(p.records.get(j));
							}
							
							
						}
					}
				}

			} 
			else {
				ArrayList<Object> r = new ArrayList<>();
				for (String i : pages) {
					Page p = loadPage(i);
					r = selectnoIndexHelper_unC(p, arrSQLTerms._objValue, colName, ">=");
					for (Object z : r) {
						result.add(z);
					}

				}
			}

		} else if (arrSQLTerms._strOperator.equals("<")) {
			if (Clusterd) {
				for (String i : pages) {
					Page p = loadPage(i);
					int index;
					try {
						index = p.UpperBound(Value);
					} catch (ParseException e) {
						throw new DBAppException(e.getMessage());
					}

					if (index != -1) {
						if(this.type!=4) {
						Vector r = p.records.get(index);
						String s = ""; // what if polygon? // date

						if (tellyou_Date_poly == 1) {
							s = convert_Date_String(r.get(0));
						} else {
							s = "" + r.get(0); // what if polygon? // date

						}
						while (s.equals(Value) && index >= 0) {

							index--;
							if (index != -1) {
								r = p.records.get(index);
								if (tellyou_Date_poly == 1) {
									s = convert_Date_String(r.get(0));
								} else {
									s = "" + r.get(0); // what if polygon? // date

								}
							}
						}
						for (int x = index; x >= 0; x--) {
							result.add(p.records.get(x));
						}
						}
						else {
							Vector r = p.records.get(index);
							Polygon poly=(Polygon) r.get(0);
							Integer area =poly.getBounds().height*poly.getBounds().width;
							String s = ""; // what if polygon? // date
							
							while (s.equals(""+area) && index >= 0) {

								index--;
								if (index != -1) {
									r = p.records.get(index);
									poly=(Polygon) r.get(0);
									area =poly.getBounds().height*poly.getBounds().width;
									s = "" + area; // what if polygon? // date
								}
							}
							for (int x = index; x >= 0; x--) {
								result.add(p.records.get(x));
							}
						}
						
					}
				}

			} else {
				ArrayList<Object> r = new ArrayList<>();
				for (String i : pages) {
					Page p = loadPage(i);
					r = selectnoIndexHelper_unC(p, arrSQLTerms._objValue, colName, "<");
					for (Object z : r) {
						result.add(z);
					}

				}
			}
		} else {
			if (Clusterd) {
				for (String i : pages) {
					Page p = loadPage(i);
					int index;
					try {
						index = p.UpperBound(Value);
					} catch (ParseException e) {
						throw new DBAppException(e.getMessage());
					}
					if (index != -1) {
						Vector r = p.records.get(index);

						for (int x = index; x >= 0; x--) {
							result.add(p.records.get(x));
						}
					}
				}

			} else {
				ArrayList<Object> r = new ArrayList<>();
				for (String i : pages) {
					Page p = loadPage(i);
					r = selectnoIndexHelper_unC(p, arrSQLTerms._objValue, colName, "<=");
					for (Object z : r) {
						result.add(z);
					}

				}
			}

		}

		return result;

	}

	public ArrayList OperationsHandler(ArrayList records, SQLTerm[] arrSQLTerms, String[] strarrOperators) {

		ArrayList<Object> Final_Result = new ArrayList<>();
		ArrayList result1 = (ArrayList) records.get(0); // get first query to start compare and or xor

		Page p = loadPage(pages.get(0));

		for (int i = 1; i < records.size(); i++) {
			ArrayList<Object> query_result = new ArrayList<>();
			ArrayList r2 = (ArrayList) records.get(i); // 2nd , 3rd ,.. query
			int index_r2 = (int) p.columnNameIndex.get(arrSQLTerms[i]._strColumnName);

			if (i == 1) {
				if (strarrOperators[i - 1].equals("OR")) {

					for (Object k : result1) {
						query_result.add(k);
					}
					for (Object k : r2) {

						query_result.add(k);

					}
					Final_Result.add(query_result);
					continue;

				} else if (strarrOperators[i - 1].equals("XOR")) {

					for (int z = 0; z < result1.size(); z++) {

						Vector m = (Vector) result1.get(z);
						if (!r2.contains(m)) {
							query_result.add(m);
						}
					}

					for (int z = 0; z < r2.size(); z++) {

						Vector m = (Vector) r2.get(z);
						if (!result1.contains(m)) {
							query_result.add(m);
						}
					}

				}

				for (int z = 0; z < result1.size(); z++) {
					Vector m = (Vector) result1.get(z); // to cast because exception here we get the row

					for (int h = 0; h < r2.size(); h++) {
						Vector m2 = (Vector) r2.get(h);
						if (strarrOperators[i - 1].equals("AND")) {
							if (m.get(index_r2).equals(m2.get(index_r2))) {

								query_result.add(m);
								break;
							}
						}

					}
				}

			} else {

				for (int z = 0; z < Final_Result.size(); z++) {
					ArrayList s1 = (ArrayList) Final_Result.get(z); // get first query data

					// r2 el query ele h3mlha m3 s1

					if (strarrOperators[i - 1].equals("OR")) {
						// System.out.println("myr2" + r2);
						// System.out.println("s1" + s1);
						for (Object k : s1) {
							query_result.add(k);
						}
						for (Object k : r2) {
							query_result.add(k);

						}
						Final_Result = new ArrayList<>();
						Final_Result.add(query_result);
						continue;

					} else if (strarrOperators[i - 1].equals("XOR")) {
						// System.out.println("myr2 el gdeda" + r2.size() + " " + r2);
						// System.out.println("s1(ba2y el result) ->" + "size" + s1.size() + " " + s1);

						for (int zz = 0; zz < s1.size(); zz++) {

							Vector m = (Vector) s1.get(zz);
							if (!r2.contains(m)) {
								query_result.add(m);
							}
						}

						for (int zzz = 0; zzz < r2.size(); zzz++) {

							Vector m = (Vector) r2.get(zzz);
							if (!s1.contains(m)) {
								query_result.add(m);
							}
						}

					}

					for (int k = 0; k < s1.size(); k++) {
						Vector m = (Vector) s1.get(k); // to cast because exception here we get the row

						for (int h = 0; h < r2.size(); h++) {
							Vector m2 = (Vector) r2.get(h);
							if (strarrOperators[i - 1].equals("AND")) {
								if (m.get(index_r2).equals(m2.get(index_r2))) {

									query_result.add(m);
									break;
								}
							}

						}
					}

					Final_Result = new ArrayList<>();

				}
			}
			Final_Result.add(query_result);
			// query_result.clear(); // new

		}
		p = null;
		return Final_Result;

	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		ArrayList Result = new ArrayList();
		Iterator ans;
		for (int i = 0; i < arrSQLTerms.length; i++) {
			if (colNameIndex.containsKey(arrSQLTerms[i]._strColumnName)||R_colNameIndex.containsKey(arrSQLTerms[i]._strColumnName)) {
				// System.out.println("enterrr");
				ArrayList r = selectIndex(arrSQLTerms[i]);
				Result.add(r);
			} else {
				ArrayList r = selectnoIndex(arrSQLTerms[i]);

				Result.add(r);
			}
		}

		// ArrayList z = (ArrayList) Result.get(0);
//		for (Object a : z) {
//			System.out.println(a);
//		}
//		ArrayList h = (ArrayList) Result.get(1);
//		for (Object a : h) {
//			System.out.println(a);
//		}

		// System.out.println("Result->" + ((ArrayList) (Result.get(0))).size() +
		// Result.get(0));

		ArrayList Final_Result = OperationsHandler(Result, arrSQLTerms, strarrOperators);
		if (Final_Result.isEmpty()) {
			ans = Result.iterator();
			return ans;
		}
		// System.out.println("Final_Result->" + ((ArrayList)
		// (Final_Result.get(0))).size() + Final_Result.get(0));

		ans = Final_Result.iterator();
		return ans;

	}

	public int getColIndex(Page page, String strColName) {
		int colIndex = (int) page.columnNameIndex.get(strColName);
		return colIndex;
	}

	public void createIndeX(String strColName) throws DBAppException {
		if (pages.isEmpty()) {
			for (String i : colNameIndex.keySet()) {
				File f = new File("data/" + colNameIndex.get(i) + ".class");
				f.delete();
			}
			for (String i : R_colNameIndex.keySet()) {
				File f = new File("data/" + R_colNameIndex.get(i) + ".class");
				f.delete();
			}
			colNameIndex.clear();
			R_colNameIndex.clear();
			return;
		}
		Page p1 = loadPage(pages.get(0)); // map strColName ->Index in records // Column
		int col_Index = getColIndex(p1, strColName); // index el strColName fl records
		if (!ColNameType.containsKey(strColName)) {
			throw new DBAppException("column doesnt exist");
		}

		p1 = null;
		String type = (String) ColNameType.get(strColName);
		String tree_name = tableName + "" + strColName;
		BPTree tree = null;
		RTree rtree = null;
		if (type.equals("java.lang.String")) {
			tree = new BPTree<String>(TreeOrder);
		} else if (type.equals("java.lang.Integer")) {
			tree = new BPTree<Integer>(TreeOrder);
		} else if (type.equals("java.lang.Double")) {
			tree = new BPTree<Double>(TreeOrder);
		} else if (type.equals("java.util.Date")) {

			tree = new BPTree<Date>(TreeOrder);
		} else {
			rtree = new RTree(TreeOrder);
		}
		if (rtree == null) {
			colNameIndex.put(strColName, tree_name);
		} else {
			R_colNameIndex.put(strColName, tree_name);
		}
		int page_index = 0;
		for (String name : pages) {
			Page page = loadPage(name);
			String p_name = name.replace(tableName, "");
			page_index = Integer.parseInt(p_name);
			for (int i = 0; i < page.records.size(); i++) {
				if (tree != null) {
					Comparable key = (Comparable) page.records.get(i).get(col_Index);

					if (tree.search(key) == null) {

						Ref ref = new Ref(page_index, i); // i refers to makan el tuple gwa el records
						tree.insert(key, ref);
						tree.search(key).Refs.add(ref);
					} else {
						Ref target_ref = tree.search(key);
						Ref new_ref = new Ref(page_index, i);
						target_ref.Refs.add(new_ref);
					}

				} else {
					Polygon key = (Polygon) page.records.get(i).get(col_Index);
					int keyarea = key.getBounds().height * key.getBounds().width;
					if (rtree.search(keyarea) == null) {
						Ref ref = new Ref(page_index, i); // i refers to makan el tuple gwa el records
						rtree.insert(keyarea, ref);
						rtree.search(keyarea).Refs.add(ref);
					} else {
						Ref target_ref = rtree.search(keyarea);
						Ref new_ref = new Ref(page_index, i);
						target_ref.Refs.add(new_ref);
					}
				}
			}

		}
		if (tree != null)
			tree.saveTree(tableName + strColName);
		else
			rtree.R_saveTree(tableName + strColName);

	}

	public void checkType(Hashtable values) throws DBAppException {

		try {
			Hashtable z = loadCSV("data/metadata.csv", tableName);
			// System.out.println(z.containsKey("id"));
			// System.out.println(z);

			for (Object i : values.keySet()) {

				if (z.containsKey(i)) {

					String x = values.get(i).getClass().toString().toLowerCase();
					String y = "class" + " " + (String) z.get(i).toString().toLowerCase();

					if (y.equals(x)) {
						continue;
					} else {
						// return false;

						throw new DBAppException("Incompatitable Type Exception");

					}
				} else {
					// return false;
					throw new DBAppException("Invalid column Exception");

				}
			}

		} catch (Exception e) {

			throw new DBAppException(e.getMessage());
		}

		// return true;

	}

	public Object getKey(Hashtable<String, Object> htblColNameValue) {
		return htblColNameValue.get(key);
	}

// id->1111 , gpa ->2 m
	public String searchPage(Object key) {

		for (String i : pages) {
			Page p1 = loadPage(i);
			Object k = p1.getLast(); // last key fl page

			if (type == 0) {
				String x = (String) k;
				String y = (String) key;
				if (y.compareTo(x) < 0) {
					return i;
				}

			} else if (type == 1) {

				int x = (int) k; // last fl page

				int y = (int) key; // key

				if (y < x) {

					return i;
				}

			} else if (type == 2) {

				Double x = (Double) k; // last fl page

				Double y = (Double) key; // key

				if (y < x) {

					return i;
				}
			} else if (type == 3) {
				Date x = (Date) k;
				Date y = (Date) key;
				if (y.compareTo(x) < 0) {
					return i;
				}

			} else if (type == 4) {
				Polygon x = (Polygon) k;
				Polygon y = (Polygon) key;
				if (p1.PolyCompare(y, x) < 0) {
					return i;
				}
			}

		}
		return null;
	}

	public void createpage(Hashtable<String, Object> htblColNameValue) {
		String[] colName = new String[htblColNameValue.size()];
		colName[0] = key;
		int c = 1;
		for (Object i : htblColNameValue.keySet()) {
			if (!i.equals(key)) {
				colName[c++] = (String) i;
			}
		}
		String name = tableName + NumOfPages;
		Page p1 = new Page(name, key, tableName, colName, type);
		p1.insert(htblColNameValue);

		p1.savePage(name);
		p1 = null;

		pages.add(name);
		NumOfPages++;

	}

//	public int getdupliindex(Page p2, Hashtable<String, Object> htblColNameValue) {
//		Vector tuple = p2.getTuple(htblColNameValue);
//		int ind = p2.records.indexOf(tuple);
//		int record_size = p2.records.size();
//		int dupli = 0;
//		for (int i = ind; i < record_size - 1; i++) {
//			if (p2.records.get(i + 1).equals(p2.records.get(i))) {
//				dupli++;
//			} else {
//				break;
//			}
//
//		}
//		return record_size + dupli;
//	}

	public void insert(Hashtable<String, Object> htblColNameValue) throws DBAppException, ParseException {
		if (!htblColNameValue.containsKey(key))

		{
			throw new DBAppException("Clustring key not found");
		}

		checkType(htblColNameValue);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		Date date = new Date();
		String y = sdf.format(date);
		Date zz = sdf.parse(y);
		htblColNameValue.put("TouchDate", zz);
		int page_index = 0;
		int record_index = 0;
		if (pages.isEmpty()) {
			createpage(htblColNameValue);
		} else {

			Object key = getKey(htblColNameValue);

			String pagename = searchPage(key);

			Page p1;
			if (pagename == null) {
				p1 = null;
			} else {
				p1 = loadPage(pagename);

			}
			// p1 -- null key > last key in all pages
			if (p1 == null && !loadPage(pages.get(pages.size() - 1)).isFull()) {
				Page p2 = loadPage(pages.get(pages.size() - 1));
				int n = pages.size() - 1;
				p2.insert(htblColNameValue);
//				record_index = getdupliindex(p2, htblColNameValue);
				String pname = p2.name;
				String intValue = pname.replaceAll("[^0-9]", "");
				page_index = Integer.parseInt(intValue);
//				String x = pages.size() - 1 + "";
				p2.savePage(p2.name);///////////////////// deletion problem
				p2 = null;
			} else if (p1 == null && loadPage(pages.get(pages.size() - 1)).isFull()) {

				createpage(htblColNameValue);
				page_index = pages.size() - 1;
			} else if (p1 != null && p1.isFull()) {
				Vector tuple = p1.getLastTuple();
				p1.records.remove(p1.records.lastElement());
				///////////// $$/////////$$$$$$$$$$$$///////////////////////////////////////
				Hashtable x = p1.getHashtable(p1.records.lastElement());
//				if (!colNameIndex.isEmpty()) {
//					for (Object i : colNameIndex.keySet()) // i -> key , get(i) -> value
//					{
//						if (!ColNameType.get(i).equals("java.awt.Polygon")) {// 8yrt
//							Comparable value = (Comparable) x.get(i);
//							// BPTree tree = colNameIndex.get(i);
//
//							BPTree tree = loadTree(colNameIndex.get(i));
//							tree.delete(value);
//						} else {
//							Polygon value = (Polygon) x.get(i);
//							// BPTree tree = colNameIndex.get(i);
//
//							RTree tree = R_loadTree(colNameIndex.get(i));
//							tree.delete(value);
//
//						}
//
//					}
//
//				}
				p1.noRows--;
				Hashtable temp = new Hashtable();

				temp = p1.getHashtable(tuple);

				p1.insert(htblColNameValue);
				// record_index = getdupliindex(p1, htblColNameValue);

				String pname = p1.name;
				String intValue = pname.replaceAll("[^0-9]", "");
				page_index = Integer.parseInt(intValue);
				p1.savePage(pagename);
				p1 = null;
				insert(temp);
			} else {
				p1.insert(htblColNameValue);
				// record_index = getdupliindex(p1, htblColNameValue);
				String pname = p1.name;
				String intValue = pname.replaceAll("[^0-9]", "");
				page_index = Integer.parseInt(intValue);
				p1.savePage(pagename);
				p1 = null;

			}

		}
//		if (!colNameIndex.isEmpty()) {
//			for (String i : colNameIndex.keySet()) // i -> key , get(i) -> value
//			{
//				BPTree tree=loadTree(tableName+i);
//				Comparable value = (Comparable) htblColNameValue.get(i);
////				BPTree tree = colNameIndex.get(i);
//				Ref ref = new Ref(page_index, record_index);
//				if(tree.search(value)!=null) {
//					tree.search(value).Refs.add(ref);
//				}
//				else {
//					tree.insert(value, ref);
//					tree.search(value).Refs.add(ref);
//				}
//				tree.saveTree(tableName+i);
//				}
//		}
		if (!colNameIndex.isEmpty()) {
			for (String i : colNameIndex.keySet()) {
				createIndeX(i);
				// colNameIndex.get(i).saveTree(tableName + i);
				if (!ColNameType.get(i).equals("java.awt.Polygon")) {// 8yrt
					BPTree tree = loadTree(colNameIndex.get(i));
					tree.saveTree(tableName + i);
				}
			}

		}
		if (!R_colNameIndex.isEmpty()) {
			for (String i : R_colNameIndex.keySet()) {
				createIndeX(i);
				// colNameIndex.get(i).saveTree(tableName + i);

				RTree rtree = R_loadTree(R_colNameIndex.get(i));
				rtree.R_saveTree(tableName + i);

			}

		}

	}

	@SuppressWarnings("unchecked")
	public void deleteFromTable(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		checkType(htblColNameValue);
		boolean first_time = false;
		if (!colNameIndex.isEmpty()) {
			ArrayList<String> to_deleted_Page = new ArrayList<String>();
			ArrayList<Ref> tobeDeleted = new ArrayList<Ref>();
			ArrayList<Integer> done_page = new ArrayList<Integer>();
			for (String i : colNameIndex.keySet()) {
				@SuppressWarnings("rawtypes")
				// BPTree tree = colNameIndex.get(i); // first random
				BPTree tree = loadTree(colNameIndex.get(i));
				@SuppressWarnings("rawtypes")
				Comparable value = (Comparable) htblColNameValue.get(i);
				Ref ref = tree.search(value); // ref-> m3ah array b kol el 7agat ele zyo
				if (!first_time) {

					@SuppressWarnings("rawtypes")
					ArrayList<Ref> target_ref = new ArrayList();
					// target_ref.add(ref); // we will remove that as the ref in the array
					if (ref.Refs.size() != 0) {
						for (int z = 0; z < ref.Refs.size(); z++) {
							target_ref.add((Ref) ref.Refs.get(z));
						}
					}
					// System.out.println(target_ref.get(0).getIndexInPage());
					for (int h = 0; h < target_ref.size(); h++) {
						@SuppressWarnings("new_Update") // due to shift problem i will use normal delete
						int page_no = target_ref.get(h).getPage();
						int index = target_ref.get(h).getIndexInPage();
						Ref newRef = new Ref(page_no, index);
						String pagename = tableName + "" + page_no;
						done_page.add(page_no);
						Page p1 = loadPage(pagename);
						boolean isDeleted = p1.deleteIndex(htblColNameValue, index);
						// System.out.println("index->" + index + " " + "\n" + "pagenum ->" + page_no);
						if (isDeleted) {
							// its mean en el index da7 sa7
							tobeDeleted.add(newRef);
							if (p1.records.size() == 0) {
								File f = new File("data/" + pagename + ".class");
								f.delete();
								pages.remove(pagename);
								continue;
							}
						}
						p1.savePage(pagename);
						p1 = null;
					}
				}

				first_time = true;
				for (int z = 0; z < ref.Refs.size(); z++) {

//					if (tobeDeleted.contains(ref.Refs.get(z))) {
//
//						ref.Refs.remove(ref.Refs.get(z));
//
//					}
					for (Ref r : tobeDeleted) {
						if (r.check(ref.Refs.get(z))) {
							ref.Refs.remove(ref.Refs.get(z));
						}
					}

				}
				tree.delete(value);
				tree.insert(value, ref);
				for (Ref r : tobeDeleted) {
					if (r.check(ref) && ref.Refs.isEmpty()) {
						tree.delete(value);
					}
					if (r.check(ref) && !ref.Refs.isEmpty()) {
						Ref finalref = new Ref(ref.Refs.get(0).getPage(), ref.Refs.get(0).getIndexInPage());
						for (int m = 1; m < ref.Refs.size(); m++) {
							finalref.Refs.add(ref.Refs.get(m));

						}
						tree.delete(value);
						tree.insert(value, finalref);
					}
				}
				System.out.println("indexed !");
				tree.saveTree(tableName + i);
				if (!colNameIndex.isEmpty()) {
					for (String z : colNameIndex.keySet()) {
						createIndeX(z);
						// colNameIndex.get(i).saveTree(tableName + z);
						@SuppressWarnings("rawtypes")
						BPTree treee = loadTree(colNameIndex.get(i));
						treee.saveTree(tableName + i);
					}

				}

//				if (tobeDeleted.contains(ref) && !ref.Refs.isEmpty()) {
//					Ref finalref = new Ref(ref.Refs.get(0).getPage(), ref.Refs.get(0).getIndexInPage());
//					for (int m = 1; m < ref.Refs.size(); m++) {
//						finalref.Refs.add(ref.Refs.get(m));
//
//					}
//					tree.delete(value);
//					tree.insert(value, finalref);
//
//				}

			}

		} else {
			System.out.println(" non - indexed !");
			Vector tuple = new Vector();
			tuple.setSize(htblColNameValue.size());
			int index;
			ArrayList<String> helper = new ArrayList();
			for (String i : pages) {
				Page p1 = loadPage(i);
				p1.delete(htblColNameValue);

				if (p1.records.size() == 0) {

					// delete the page
					File f = new File("data/" + i + ".class");
					f.delete();
					// pages.remove(i);
					helper.add(i);

				} else {
					p1.savePage(i);
					p1 = null;
				}

			}
			pages.removeAll(helper);
		}
	}

	public Hashtable redistribute_HashTable(Hashtable<String, Object> htblColNameValue) {
		Hashtable<String, String> result = new Hashtable();
		ArrayList<String> check = new ArrayList<String>();
		for (String i : colNameIndex.keySet()) {
			check.add(i);
		}

		for (String i : htblColNameValue.keySet()) {
			if (colNameIndex.containsKey(i)) {
				result.put(i, colNameIndex.get(i));
			}
			if (R_colNameIndex.containsKey(i)) {
				result.put(i, R_colNameIndex.get(i));
			}
		}
		for (String i : check) {
			if (!result.containsKey(i)) {
				result.put(i, colNameIndex.get(i));
			}
		}
		return result;
	}

	/**
	 * yasta el function deh :- btshof lw el input ele mdholk leh index wla la2
	 * malosh m3naha ank htdelete 3ady w t3ml create index tany
	 * 
	 * @param htblColNameValue
	 * @return
	 * @throws DBAppException
	 */
	public boolean delete_Helper_Index(Hashtable<String, Object> htblColNameValue) throws DBAppException {

		if (colNameIndex.isEmpty()) {
			throw new DBAppException("i will never reach to this excpetion isA");
		}
		for (String i : htblColNameValue.keySet()) {
			if (colNameIndex.containsKey(i)) {
				return true;
			}
		}
		return false;
	}

	public void deleteFromTabl_v2(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		checkType(htblColNameValue);
		boolean first_time = false;
		if (!colNameIndex.isEmpty() && delete_Helper_Index(htblColNameValue)) {
			ArrayList<String> to_deleted_Page = new ArrayList<String>();
			ArrayList<Ref> tobeDeleted = new ArrayList<Ref>();
			ArrayList<Integer> done_page = new ArrayList<Integer>();
			Hashtable<String, String> redistibuted_hash = redistribute_HashTable(htblColNameValue); // to be sure that//
																									// awl 7aga
			for (String i : redistibuted_hash.keySet()) {
				if (!ColNameType.get(i).equals("java.awt.Polygon")) {
					// BPTree tree = colNameIndex.get(i); // first random
					BPTree tree = loadTree(redistibuted_hash.get(i));
					@SuppressWarnings("rawtypes")
					Comparable value = (Comparable) htblColNameValue.get(i);
					Ref ref = tree.search(value); // ref-> m3ah array b kol el 7agat ele zyo
					if (!first_time) {
						@SuppressWarnings("rawtypes")
						ArrayList<Ref> target_ref = new ArrayList();
						// target_ref.add(ref); // we will remove that as the ref in the array
						if (ref.Refs.size() != 0) {
							for (int z = 0; z < ref.Refs.size(); z++) {
								target_ref.add((Ref) ref.Refs.get(z));
							}
						}
						// System.out.println(target_ref.get(0).getIndexInPage());
						for (int h = 0; h < target_ref.size(); h++) {
							@SuppressWarnings("new_Update") // due to shift problem i will use normal delete
							int page_no = target_ref.get(h).getPage();
							int index = target_ref.get(h).getIndexInPage();
							Ref newRef = new Ref(page_no, index);
							String pagename = tableName + "" + page_no;
							if (done_page.contains(page_no)) {
								continue;
							} else {
								done_page.add(page_no);
								Page p1 = loadPage(pagename);
								p1.delete(htblColNameValue);
								if (p1.records.size() == 0) {
									File f = new File("data/" + pagename + ".class");
									f.delete();
									pages.remove(pagename);
									continue;
								}
								p1.savePage(pagename);
								p1 = null;
							}
						}

						first_time = true;
						System.out.println("indexed !");
						tree.saveTree(tableName + i);

						break;
					}
				} else {
					RTree tree = R_loadTree(R_colNameIndex.get(i));
					@SuppressWarnings("rawtypes")
					Polygon value = (Polygon) htblColNameValue.get(i);
					int area = value.getBounds().height * value.getBounds().width;
					Ref ref = tree.search(area); // ref-> m3ah array b kol el 7agat ele zyo

					if (!first_time) {
						@SuppressWarnings("rawtypes")
						ArrayList<Ref> target_ref = new ArrayList();
						// target_ref.add(ref); // we will remove that as the ref in the array
						for (Ref refloop : ref.Refs) {
							Page p = loadPage(tableName + refloop.getPage() + ".class");
							Vector v = p.records.get(refloop.getIndexInPage());
							for (Object o : v) {
								if (o.getClass().toString().equals("class java.awt.Polygon")) {
									Polygon pcheck = (Polygon) o;
									if (Arrays.equals(value.xpoints, pcheck.xpoints)
											&& Arrays.equals(value.ypoints, pcheck.ypoints)) {
										target_ref.add((Ref) refloop);
									}
								}
							}

						}
//						if (ref.Refs.size() != 0) {
//							for (int z = 0; z < ref.Refs.size(); z++) {
//								target_ref.add((Ref) ref.Refs.get(z));
//							}
//						}
						if (target_ref.size() == 0) {
							throw new DBAppException("no keys");
						}
						// System.out.println(target_ref.get(0).getIndexInPage());
						for (int h = 0; h < target_ref.size(); h++) {
							@SuppressWarnings("new_Update") // due to shift problem i will use normal delete
							int page_no = target_ref.get(h).getPage();
							int index = target_ref.get(h).getIndexInPage();
							Ref newRef = new Ref(page_no, index);
							String pagename = tableName + "" + page_no;
							if (done_page.contains(page_no)) {
								continue;
							} else {
								done_page.add(page_no);
								Page p1 = loadPage(pagename);
								p1.delete(htblColNameValue);
								if (p1.records.size() == 0) {
									File f = new File("data/" + pagename + ".class");
									f.delete();
									pages.remove(pagename);
									continue;
								}
								p1.savePage(pagename);
								p1 = null;
							}
						}

						first_time = true;
						System.out.println("indexed !");
						tree.R_saveTree(tableName + i);

					}

					break;
				}
			}
			if (!colNameIndex.isEmpty() && first_time) {

				for (String z : colNameIndex.keySet()) {
					createIndeX(z);

				}

			}
			if ((!R_colNameIndex.isEmpty()) && first_time) {

				for (String z : R_colNameIndex.keySet()) {

					RTree_createIndeX(z);
				}

			}
		}

		else {
			System.out.println(" non - indexed !");
			Vector tuple = new Vector();
			tuple.setSize(htblColNameValue.size());
			int index;
			ArrayList<String> helper = new ArrayList();
			for (String i : pages) {
				Page p1 = loadPage(i);
				p1.delete(htblColNameValue);

				if (p1.records.size() == 0) {

					// delete the page
					File f = new File("data/" + i + ".class");
					f.delete();
					// pages.remove(i);
					helper.add(i);

				} else {
					p1.savePage(i);
					p1 = null;
				}

			}
			pages.removeAll(helper);
			if (!colNameIndex.isEmpty()) {
				for (String z : colNameIndex.keySet()) {
					createIndeX(z);

				}

			}
			if (!R_colNameIndex.isEmpty()) {
				for (String z : R_colNameIndex.keySet()) {
					createIndeX(z);

				}

			}

		}
	}

	public void youTryUpdateKey(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Object key_V = htblColNameValue.get(key);
		if (key_V != null) {
			throw new DBAppException("You cant update Key please remove it from hashtable !");
		}
	}

	public void Update(String keyValue, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ParseException {
		checkType(htblColNameValue);
		youTryUpdateKey(htblColNameValue);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		Date date = new Date();
		String y = sdf.format(date);
		Date zz = sdf.parse(y);
		htblColNameValue.put("TouchDate", zz);

		if ((colNameIndex.isEmpty() == false && colNameIndex.containsKey(key))
				|| (R_colNameIndex.isEmpty() == false && R_colNameIndex.containsKey(key))) {
			/// there is index lol take care
			// BPTree tree = colNameIndex.get(key);
			Comparable new_KeyValue = (Comparable) keyValue;
			if (type != 4) {
				BPTree tree = loadTree(colNameIndex.get(key));
				if (tree.getType().equalsIgnoreCase("class java.lang.integer")) {
					Integer key1 = Integer.parseInt(keyValue);
					new_KeyValue = (Comparable) key1;
				} else if (tree.getType().equalsIgnoreCase("class java.lang.string")) {
					new_KeyValue = (Comparable) keyValue;
				} else if (tree.getType().equalsIgnoreCase("class java.lang.double")) {
					Double key1 = Double.parseDouble(keyValue);
					new_KeyValue = (Comparable) key1;
				} else {
					StringTokenizer st = new StringTokenizer(keyValue);
					String year = st.nextToken("-");
					String month = st.nextToken("-");
					String day = st.nextToken("-");
					SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd");
					String datee = year + "/" + month + "/" + day;
					Date key1 = sdf2.parse(datee);
					new_KeyValue = (Comparable) key1;
				}
				Ref ref = tree.search(new_KeyValue);

				if (ref == null) {
					throw new DBAppException("the key not found ! ");
				} else {
					ArrayList<Ref> target_ref = new ArrayList();
					// target_ref.add(ref);
					if (ref.Refs.size() != 0) {
						for (int i = 0; i < ref.Refs.size(); i++) {
							target_ref.add((Ref) ref.Refs.get(i));
						}
					}
					for (int i = 0; i < target_ref.size(); i++) {

						int page_no = target_ref.get(i).getPage();
						int index = target_ref.get(i).getIndexInPage();
						String pagename = tableName + "" + page_no;
						Page p1 = loadPage(pagename);
						p1.UpdateIndex(keyValue, htblColNameValue, index);
						p1.savePage(pagename);
						p1 = null;
					}

				}
				System.out.println("Indexed Update");
			} else {

				RTree tree = R_loadTree(R_colNameIndex.get(key));
				StringTokenizer st = new StringTokenizer(keyValue);
				ArrayList xx = new ArrayList();
				ArrayList yy = new ArrayList();
				while (st.hasMoreTokens()) {
					xx.add(st.nextToken("(,)"));
					yy.add(st.nextToken("(,)"));
				}

				int[] x = new int[xx.size()];
				int[] ypoints = new int[xx.size()];
				for (int i = 0; i < xx.size(); i++) {
					x[i] = Integer.parseInt((String) xx.get(i));
					ypoints[i] = Integer.parseInt((String) yy.get(i));
				}
				Polygon polygon = new Polygon(x, ypoints, xx.size());
				int area = polygon.getBounds().height * polygon.getBounds().width;
				Ref ref = tree.search(area);

				if (ref == null) {
					throw new DBAppException("the key not found ! ");
				} else {
					ArrayList<Ref> target_ref = new ArrayList();
					for (Ref refloop : ref.Refs) {
//						System.out.println(tableName+refloop.getPage()+".class");
						Page p = loadPage(tableName + refloop.getPage());
						Vector v = p.records.get(refloop.getIndexInPage());
						for (Object o : v) {
							if (o.getClass().toString().equals("class java.awt.Polygon")) {
								Polygon pcheck = (Polygon) o;
//								System.out.println("here");
								if (Arrays.equals(polygon.xpoints, pcheck.xpoints)
										&& Arrays.equals(polygon.ypoints, pcheck.ypoints)) {
									target_ref.add((Ref) refloop);
								}
							}
						}

					}
					// target_ref.add(ref);
					if (target_ref.size() == 0) {
						throw new DBAppException("the key not found ! ");

					}
					for (int i = 0; i < target_ref.size(); i++) {

						int page_no = target_ref.get(i).getPage();
						int index = target_ref.get(i).getIndexInPage();
						String pagename = tableName + "" + page_no;
						Page p1 = loadPage(pagename);
						p1.UpdateIndex(keyValue, htblColNameValue, index);
						p1.savePage(pagename);
						p1 = null;
					}

				}

			}

		} else {
			System.out.println("non-Indexed Update");
			boolean flag = false;
			if (type == 3) {
				StringTokenizer st = new StringTokenizer(keyValue);
				String year = st.nextToken("-");
				String month = st.nextToken("-");
				String day = st.nextToken("-");
				String datee = year + "/" + month + "/" + day;
				keyValue = datee;

			}
			for (String i : pages) {
				Page p1 = loadPage(i);

				if (p1.getIndex(keyValue) != -1) {
					p1.Update(keyValue, htblColNameValue);
					p1.savePage(i);
					p1 = null;
					flag = true;
				}
			}
			if (!flag) {
				throw new DBAppException("this key not exisits ");
			}
		}
		if (!colNameIndex.isEmpty()) {
			for (String name : htblColNameValue.keySet()) {
				if (colNameIndex.containsKey(name)) {
					createIndeX(name);
				}
			}
		}
		if (!R_colNameIndex.isEmpty()) {
			for (String name : htblColNameValue.keySet()) {
				if (R_colNameIndex.containsKey(name)) {
					createIndeX(name);
				}
			}
		}
	}

	public void saveTable(String path) // path = tablename
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

	public String getType(String key) {
		return (String) ColNameType.get(key);
	}

	public static Hashtable loadCSV(String path, String table) throws Exception {
		Hashtable result = new Hashtable();
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		boolean flag = false;

		while (line != null) {
			String[] info = line.split(",");
			line = br.readLine();
			String x = info[2];
			String z = info[1];
			String tablename = info[0];
			if (tablename.equals(table)) {
				result.put(z, x);
			}

		}

		br.close();
		return result;

	}

	public Page loadPage(String path) { // path=tablename
		FileInputStream fs;
		try {
			fs = new FileInputStream("data/" + path + ".class");
			ObjectInputStream os = new ObjectInputStream(fs);
			Page p1 = (Page) os.readObject();

			os.close();
			return p1;

		} catch (FileNotFoundException e) {

			System.out.println("OPS this file is deleted yasta aw msh mawgod");
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	public BPTree loadTree(String path) { // path=tablename
		FileInputStream fs;
		try {
			fs = new FileInputStream("data/" + path + ".class");
			ObjectInputStream os = new ObjectInputStream(fs);
			BPTree p1 = (BPTree) os.readObject();

			os.close();
			return p1;

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	public RTree R_loadTree(String path) { // path=tablename
		FileInputStream fs;
		try {
			fs = new FileInputStream("data/" + path + ".class");
			ObjectInputStream os = new ObjectInputStream(fs);
			RTree p1 = (RTree) os.readObject();

			os.close();
			return p1;

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

}
