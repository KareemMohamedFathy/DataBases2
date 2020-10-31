package RTree;

import java.awt.Dimension;
import java.awt.Polygon;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Vector;

import BpTree.Ref;

import java.util.Iterator;


public class TestBPTree {
	public static boolean check(Ref ref1, Ref ref2) {
		return ref1.getPage() == ref2.getPage() && ref2.getIndexInPage() == ref2.getIndexInPage();
	}

	public static void main(String[] args) {
		RTree tree = new RTree(3);
	   ArrayList <Integer> s = new ArrayList <Integer >  ();
	   int []x= {1,2,3};
	   int []y= {1,2,3};
	   Polygon p=new Polygon(x, y, 3) ;
	   int []x1= {1,5,3};
	   int []y1= {10,2,8};
	   Polygon p1=new Polygon(x1, y1, 3) ;
	   int []x2= {100,28,3};
	   int []y2= {1,0,3};
	   Polygon p2=new Polygon(x2, y2, 3) ;
	   int []x3= {1,89,3};
	   int []y3= {1,92,73};
	   Polygon p3=new Polygon(x3, y3, 3) ;
	   int []x4= {1,9,9};
	   int []y4= {1,4,9};
	   Polygon p4=new Polygon(x4, y4, 3) ;
	   int []x5= {1,2,0};
	   int []y5= {1,2,7};
	   Polygon p5=new Polygon(x5, y5, 3) ;
	   tree.insert(p.getBoundingBox().height*p.getBoundingBox().width, null);
	   tree.insert(p1.getBoundingBox().height*p1.getBoundingBox().width, null);
	   tree.insert(p2.getBoundingBox().height*p2.getBoundingBox().width, null);
	   tree.insert(p3.getBoundingBox().height*p3.getBoundingBox().width, null);
	   tree.insert(p4.getBoundingBox().height*p4.getBoundingBox().width, null);
	   tree.insert(p5.getBoundingBox().height*p5.getBoundingBox().width, null);
	   Dimension d=p.getBounds().getSize();
	   Dimension d1=p1.getBounds().getSize();
	   Dimension d2=p2.getBounds().getSize();
	   Dimension d3=p3.getBounds().getSize();
	   Dimension d4=p4.getBounds().getSize();
	   Dimension d5=p5.getBounds().getSize();
	   System.out.println(d.width*d.height);
	   System.out.println(d1.width*d1.height);
	   System.out.println(d2.width*d2.height);
	   System.out.println(d3.width*d3.height);
	   System.out.println(d4.width*d4.height);
	   System.out.println(d5.width*d5.height);

	   System.out.println(p.toString());
	   tree.toString();
	   
	  

	}
}
