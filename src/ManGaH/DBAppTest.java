package ManGaH;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;
import java.util.Iterator;
import java.util.Random;

import BpTree.Ref;

public class DBAppTest {
	private static final Random rand = new Random();

	public static void main(String[] args) throws DBAppException, IOException, ParseException {

		String strTableName = "testStringtwo";

		DBApp dbApp = new DBApp();
//		5. Create table "Course"
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("Poly", "java.awt.Polygon");
		htblColNameType.put("Name", "java.lang.String");
		htblColNameType.put("Semester", "java.lang.Integer");
		htblColNameType.put("date", "java.util.Date");
//		dbApp.createTable(strTableName,"Name", htblColNameType ); //create table
		// exception
		int[] x = { 1, 2, 3 };
		int[] y = { 1, 2, 3 };
		Polygon p = new Polygon(x, y, 3);
		int[] x1 = { 1, 5, 3 };
		int[] y1 = { 10, 2, 8 };
		Polygon p1 = new Polygon(x1, y1, 3);
		int[] x2 = { 100, 28, 3 };
		int[] y2 = { 1, 0, 3 };
		Polygon p2 = new Polygon(x2, y2, 3);
		int[] x3 = { 1, 89, 3 };
		int[] y3 = { 1, 92, 73 };
		Polygon p3 = new Polygon(x3, y3, 3);
		int[] x4 = { 1, 9, 9 };
		int[] y4 = { 1, 4, 9 };
		Polygon p4 = new Polygon(x4, y4, 3);
		int[] x5 = { 8, 9, 10 };
		int[] y5 = { 8, 9, 10 };
		Polygon p5 = new Polygon(x5, y5, 3);
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		Date d=sdf.parse("1998-08-07");
//		System.out.println(d.toString());
		Hashtable<String, Object> ctblColNameValue1 = new Hashtable<String, Object>();
		ctblColNameValue1.put("Poly", p);
		ctblColNameValue1.put("Name", "p");
		ctblColNameValue1.put("Semester", 39);
		ctblColNameValue1.put("date",d  );
		
		
	
//		
//	dbApp.insertIntoTable(strTableName, ctblColNameValue1);
//		 dbApp.createRTreeIndex(strTableName, "Poly");
//		 dbApp.createBTreeIndex(strTableName, "date");
//		 dbApp.deleteFromTable(strTableName, ctblColNameValue1);
//		 dbApp.updateTable(strTableName,"a", ctblColNameValue1);
//		 dbApp.loadTable(strTableName).loadPage(strTableName+0).getRecords();
		 dbApp.loadTable(strTableName).loadTree(strTableName+"date").toString();		
		 dbApp.loadTable(strTableName).R_loadTree(strTableName+"Poly").toString();

//		System.out.println(dbApp.loadTable(strTableName).R_loadTree(strTableName+"poly").toString());
//		dbApp.deleteFromTable(strTableName, ctblColNameValue1);
// 		dbApp.createBTreeIndex(strTableName, "date");

//	 # Insert in table "Course"

		// System.out.println(dbApp.loadTable(strTableName).loadTree("MohamedSlimandate"));
//		Hashtable<String, Object> ctblColNameValue1 = new Hashtable<String, Object>();
		// ctblColNameValue1.put("date", dx3);
		// ctblColNameValue1.put("Name", "asasa");
		// ctblColNameValue1.put("Code", "asaa");
//	     ctblColNameValue1.put("Hours", 555);
//		ctblColNameValue1.put("Semester", 555);
//		ctblColNameValue1.put("Major_ID", 5555);
		// dbApp.insertIntoTable(strTableName, ctblColNameValue1);
		// Polygon pp = (Polygon) (dbApp.loadTable(strTableName).loadPage(strTableName +
		// "" + 0).records.get(0)).get(0);
		// dbApp.updateTable(strTableName, "2010-01-01", ctblColNameValue1);

//		 dbApp.deleteFromTable(strTableName, ctblColNameValue1);
//		Date dx = new Date("2010/1/1");
//		Date dx1 = new Date("2020/1/2");
//
//		Date dx2 = new Date("2010/1/20");
//
//		Date dx3 = new Date("2011/4/4");
//
//		Date dx4 = new Date("2021/05/02");

// 
		// long startTime = System.currentTimeMillis(); // for testing purpose

//	for(int i=170;i<1000;i++)
//	{
//		Hashtable<String,Object> ctblColNameValueI = new Hashtable<String,Object>();
//		ctblColNameValueI.put("ID", i+2);
//		ctblColNameValueI.put("Name", "c"+(i+2));
//		ctblColNameValueI.put("Code", "co "+(i+2));
//		ctblColNameValueI.put("Hours", rand.nextInt(7) + 2);
//		ctblColNameValueI.put("Semester", rand.nextInt(10) + 1);
//		ctblColNameValueI.put("Major_ID", rand.nextInt(1001) + 1);
//		dbApp.insertIntoTable("Course", ctblColNameValueI);
//	}
		// long endTime = System.currentTimeMillis(); // for testing purpose
//	System.out.printf("Time for query = %d ms\n", endTime - startTime);

// 
//		dbApp.createTable(strTableName,"ID", htblColNameType  );
		// Create table "Student"
//		Hashtable htblColNameType = new Hashtable();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		htblColNameType.put("SSID", "java.lang.Integer");
// dbApp.createTable(strTableName, "id", htblColNameType);
//	
//		Hashtable htblColNameValue = new Hashtable();
//		htblColNameValue.put("id", new Integer(150000));
//		htblColNameValue.put("name", new String("shady"));
//		htblColNameValue.put("gpa", 4.0);
//		htblColNameValue.put("SSID", new Integer(600));
//
//		 dbApp.insertIntoTable(strTableName, htblColNameValue);
		// dbApp.updateTable(strTableName, "303", htblColNameValue);
		// dbApp.createBTreeIndex(strTableName, "ID");
		// dbApp.deleteFromTable(strTableName, htblColNameValue);
//		 dbApp.loadTable("Student").createIndeX("id");
// System.out.println( dbApp.loadTable("Student").loadTree("Studentid"));

//		String[] strarrOperators = new String[0];
//  	strarrOperators[0] = "AND"; 
// 		strarrOperators[1] = "AND";
//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[1];
//		for (int i = 0; i < arrSQLTerms.length; i++)
//			arrSQLTerms[i] = new SQLTerm();
//		arrSQLTerms[0]._strTableName = "manga";
//		arrSQLTerms[0]._strColumnName = "id";
//		arrSQLTerms[0]._strOperator = "<=";
//		arrSQLTerms[0]._objValue = new Integer(300);
//		arrSQLTerms[1]._strTableName = "Student";
//		arrSQLTerms[1]._strColumnName = "name";
//		arrSQLTerms[1]._strOperator = "=";
//		arrSQLTerms[1]._objValue = new String("shady");
//		arrSQLTerms[2]._strTableName = "Student";	
//		arrSQLTerms[2]._strColumnName = "gpa";
//		arrSQLTerms[2]._strOperator = "=";
//		arrSQLTerms[2]._objValue = new Double(3.7);

		// -------------------------------------------------------------------------------
//
		String[] strarrOperators = new String[0];
//		strarrOperators[0] = "AND";
////		strarrOperators[1] = "AND";
////		strarrOperators[2] = "XOR";
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[1];
		for (int i = 0; i < arrSQLTerms.length; i++)
			arrSQLTerms[i] = new SQLTerm();
		arrSQLTerms[0]._strTableName = strTableName;
		arrSQLTerms[0]._strColumnName = "Poly";
		arrSQLTerms[0]._strOperator = ">=";
		arrSQLTerms[0]._objValue = 8008;
//		arrSQLTerms[1]._strTableName = strTableName;
//		arrSQLTerms[1]._strColumnName = "Name";
//		arrSQLTerms[1]._strOperator = "=";
//		arrSQLTerms[1]._objValue = new String("p5");
//		arrSQLTerms[2]._strTableName = "Student";
//		arrSQLTerms[2]._strColumnName = "name";
//		arrSQLTerms[2]._strOperator = "=";
//		arrSQLTerms[2]._objValue = new String("bebo");
//		arrSQLTerms[3]._strTableName = "Student";
//		arrSQLTerms[3]._strColumnName = "id";
//		arrSQLTerms[3]._strOperator = ">";
//		arrSQLTerms[3]._objValue = new Integer(9224);

//		/*
//		 * to print the table dat
//		 */
//		System.out.println("-----------------myDB-----------------------------");
//		for (String i : dbApp.loadTable(strTableName).pages) {
//			if (dbApp.loadTable(strTableName).loadPage(i) == null) {
//				continue;
//			}
//			System.out.print(i);
//			dbApp.loadTable(strTableName).loadPage(i).getRecords();
//		}
//		System.out.println("----------------myDB------------------------------");
//		System.out.println(
//				"--*&*&*&&*&*&**********************&*&*&*& TESTING &*&*&*&*********&********************************************************");
		System.out.println("select");
		System.out.println("-----------------myDB---------POLYYYYYYY-------");
		for (String i : dbApp.loadTable(strTableName).pages) {
			
			if (dbApp.loadTable(strTableName).loadPage(i) == null) {
				continue;
			}
			for(int  z=0; z<dbApp.loadTable(strTableName).loadPage(i).records.size(); z++ )
            {
//                System.out.print("x points > "+Arrays.toString(((Polygon)(dbApp.loadTable(strTableName).loadPage(i).records.get(z).get(0))).xpoints) );
//                System.out.print(" ,  y points ->"+Arrays.toString(((Polygon)(dbApp.loadTable(strTableName).loadPage(i).records.get(z).get(0))).ypoints));
                System.out.println("");
            }
			System.out.println(i);
			dbApp.loadTable(strTableName).loadPage(i).getRecords();
		}
		System.out.println("----------------myDB----POLYYY----------------------");
		System.out.println(
				"--*&*&*&&*&*&**********************&*&*&*& TESTING &*&*&*&*********&********************************************************");
		Iterator i = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//		int si=0;
		while (i.hasNext()) {
			System.out.println(i.next());
//			si++;
		}
		

		// System.out.println(dbApp.loadTable(strTableName).loadPage(strTableName+""+"0").records.get(0));
		// ArrayList result = new ArrayList ();
		// result.add(dbApp.loadTable(strTableName).loadPage(strTableName+""+"0").records.get(0));
		// System.out.println(result);
		// dbApp.loadTable(strTableName).loadPage(strTableName+""+"2").getRecords();

	}

}
