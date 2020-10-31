package BpTree;

import java.io.ObjectInputStream.GetField;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;
import java.util.Vector;
import java.util.Iterator;

public class TestBPTree {
	public static boolean check(Ref ref1, Ref ref2) {
		return ref1.getPage() == ref2.getPage() && ref2.getIndexInPage() == ref2.getIndexInPage();
	}

	public static void main(String[] args) throws ParseException {
		BPTree<Date> tree = new BPTree<Date>(3);
		Ref ref = new Ref(500,100);
		Ref ref1 = new Ref(31,101);
		Ref ref2 = new Ref(32,102);
	//  tree.insert(30, ref);
	 
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		Date date = new Date();
		String y = sdf.format(date);
		Date zz = sdf.parse(y);
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd");
         String test =  "2020/05/18";
      //   System.out.println(sdf2.parse(test).getClass());
		Date x = new Date ("2020/05/18");
 ;
	     Date date1 = new Date("2020/05/18");


	         SimpleDateFormat formatNowDay = new SimpleDateFormat("dd");
	         SimpleDateFormat formatNowMonth = new SimpleDateFormat("MM");
	         SimpleDateFormat formatNowYear = new SimpleDateFormat("YYYY");

	         String currentDay = formatNowDay.format(date1);
	         String currentMonth = formatNowMonth.format(date1);
	         String currentYear = formatNowYear.format(date1);
	 

	 System.out.println(currentDay);
	 System.out.println(currentMonth);
	 System.out.println(currentYear);

		
		
	}
}
