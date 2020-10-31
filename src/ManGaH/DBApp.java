package ManGaH;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Iterator;

public class DBApp {
	static String metadata = "data/metadata.csv";
	static HashSet<String> tables = new HashSet<String>();
	static int PSize;
	static File meta = new File(metadata);
	static Properties prop;
	static int TreeOrder;
	private ArrayList<String> Operators = new ArrayList<String>(); // avaiable operators
	private ArrayList<String> Operations = new ArrayList<String>(); // Operations AND OR XOR

	public DBApp() throws DBAppException, IOException {

		prop = new Properties();
		if (!meta.exists()) {
			makeMetadata();
		}

		getAllTables();
		try {
			prop.load(new FileInputStream("config/DBApp.properties"));
			this.PSize = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
			this.TreeOrder = Integer.parseInt(prop.getProperty("NodeSize"));
		} catch (Exception e) {
			throw new DBAppException(e.getMessage());

		}
		Operators.add(">");
		Operators.add(">=");
		Operators.add("<");
		Operators.add("<=");
		Operators.add("!=");
		Operators.add("=");
		Operations.add("AND");
		Operations.add("OR");
		Operations.add("XOR");

	}

	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException {
		if (tables.contains(strTableName)) {
			Table t1 = loadTable(strTableName);
			t1.createIndeX(strColName);
			try {
				CSVIndex(strTableName, strColName);
			} catch (IOException e) {
				e.printStackTrace();
			}
			t1.saveTable(strTableName);
			t1 = null;
		} else {
			throw new DBAppException("This table does not exist.");

		}

	}

	public void QueryValid(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		ArrayList<String> sameTable = new ArrayList<String>();
		sameTable.add(arrSQLTerms[0]._strTableName);
		if (strarrOperators.length >= arrSQLTerms.length) {
			throw new DBAppException("Invalid Query !");

		}
		for (SQLTerm i : arrSQLTerms) {
			if (!sameTable.contains(i._strTableName)) {
				throw new DBAppException("Invalid Query !");
			}
			if (!tables.contains(i._strTableName)) {
				throw new DBAppException("This table does not exist.");
			}
			if (!Operators.contains(i._strOperator)) {
				throw new DBAppException("Invalid Query !");

			}
		}
		for (String i : strarrOperators) {
			if (!Operations.contains(i)) {
				throw new DBAppException("Invalid Query !");
			}
		}
		try {
			@SuppressWarnings("rawtypes")
			Hashtable columns = Table.loadCSV(metadata, sameTable.get(0));
			for (SQLTerm i : arrSQLTerms) {
				if (!columns.containsKey(i._strColumnName)) {
					throw new DBAppException("Invalid Query (Wrong Columns) !");

				}

				String y = i._objValue.getClass().toString().toLowerCase();
				String ColName = i._strColumnName;
				String type_Col = "class" + " " + (String) columns.get(ColName);
				type_Col = type_Col.toString().toLowerCase();
				if (!type_Col.equals(y) && !type_Col.equalsIgnoreCase("class java.awt.polygon")) {
					throw new DBAppException("Incompatitable Type Exception");

				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new DBAppException(e.getMessage());
		}

	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		QueryValid(arrSQLTerms, strarrOperators);
		Table t1 = loadTable(arrSQLTerms[0]._strTableName);

		// System.out.println(t1.loadPage(arrSQLTerms[0]._strTableName + "" +
		// "0").columnNameIndex);
		return t1.selectFromTable(arrSQLTerms, strarrOperators);

	}

	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException {

		if (tables.contains(strTableName)) {
			Table t1 = loadTable(strTableName);
			t1.RTree_createIndeX(strColName);
			try {
				CSVIndex(strTableName, strColName);
			} catch (IOException e) {
				e.printStackTrace();
			}
			t1.saveTable(strTableName);
			t1 = null;
		} else {
			throw new DBAppException("This table does not exist.");

		}

	}

	public void init() {

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
		if (!tables.contains(strTableName)) {
			if (!htblColNameType.containsKey(strClusteringKeyColumn)) {
				throw new DBAppException("Invalid Creation ");
			}
			Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);

			t.saveTable(strTableName);
			t = null;
			this.addToMetaData(strTableName, htblColNameType, strClusteringKeyColumn, "false");

		} else {
			throw new DBAppException("this Table already exists");
		}

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ParseException {
		if (tables.contains(strTableName)) {
			Table t1 = loadTable(strTableName);
			t1.insert(htblColNameValue);
			t1.saveTable(strTableName);
			t1 = null;

		} else {
			throw new DBAppException("This table does not exist.");
		}
	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ParseException {
		if (tables.contains(strTableName)) {
			Table t1 = loadTable(strTableName);
			t1.Update(strClusteringKey, htblColNameValue);
			t1.saveTable(strTableName);
			t1 = null;
		} else {
			throw new DBAppException("This table does not exist.");

		}
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (tables.contains(strTableName)) {
			Table t1 = loadTable(strTableName);
			if (t1.pages.isEmpty()) {
				throw new DBAppException("MAMA MIA IT IS EMPTY");
			}
			t1.deleteFromTabl_v2(htblColNameValue);
			t1.saveTable(strTableName);
			t1 = null;
		} else {
			throw new DBAppException("This table does not exist.");

		}
	}

	public Table loadTable(String path) { // path=tablename
		FileInputStream fs;
		try {
			fs = new FileInputStream("data/" + path + ".class");
			ObjectInputStream os = new ObjectInputStream(fs);
			Table t1 = (Table) os.readObject();

			os.close();
			return t1;

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

	public void getAllTables() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(metadata)));
			while (br.ready()) {
				String[] line = br.readLine().split(",");
				tables.add(line[0]);
			}
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void makeMetadata() throws DBAppException {
		PrintWriter out;
		try {
			out = new PrintWriter(new FileWriter(metadata, true));
			out.println("Table Name, Column Name, Column Type, ClusteringKey, Indexed ");
			out.flush();
			out.close();
		} catch (Exception e) {
			throw new DBAppException("File not found !");

		}

	}

	public void addToMetaData(String strTableName, Hashtable<String, String> htblColNameType, String ClusteringKey,
			String Indexed) throws IOException {
		PrintWriter pr = new PrintWriter(new FileWriter(metadata, true));
		for (Entry<String, String> entry : htblColNameType.entrySet()) {
			String colName = entry.getKey();
			String colType = entry.getValue();
			boolean key = (colName == ClusteringKey);
			pr.append(strTableName + "," + colName + "," + colType + "," + key + "," + Indexed + "\n");
		}
		pr.flush();
		pr.close();
	}

	public ArrayList loadallCsv() throws IOException {
		ArrayList result = new ArrayList<>();
		BufferedReader br = new BufferedReader(new FileReader(metadata));
		String line = br.readLine();
		while (line != null) {
			ArrayList addit = new ArrayList();
			addit.add(line);
			result.add(addit);
			line = br.readLine();
		}

		br.close();
		return result;

	}

	public void WriteCSVupdated(ArrayList data) throws IOException {
		try (PrintWriter writer = new PrintWriter(new FileWriter(metadata))) {
			for (int i = 0; i < data.size(); i++) {
				ArrayList x = (ArrayList) data.get(i);
				for (Object obj : x) {
					StringBuilder sb = new StringBuilder();
					String info = (String) obj;
					sb.append(info);
					sb.append('\n');
					writer.write(sb.toString());
				}
			}

		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}
	}

	public void CSVIndex(String strTableName, String strColName) throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(metadata));
		String line = br.readLine();
		ArrayList data = loadallCsv();
		while (line != null) {
			String[] info = line.split(",");
			String colName = info[1];
			String tablename = info[0];
			if (tablename.equals(strTableName) && colName.equals(strColName)) {
				ArrayList newinfo = new ArrayList<>();
				String infoz = info[0] + "," + info[1] + "," + info[2] + "," + info[3].toUpperCase() + "," + "TRUE";
				newinfo.add(infoz);
				ArrayList help = new ArrayList<>();
				help.add(line);
				data.remove(help);
				data.add(newinfo);

				break;

			}
			line = br.readLine();

		}
		WriteCSVupdated(data);
		br.close();

	}

}
