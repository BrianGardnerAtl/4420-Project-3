import simpledb.server.*;
import simpledb.index.*;
import simpledb.index.hash.*;
import simpledb.query.*;
import simpledb.materialize.SortPlan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;
import java.util.*;

public class MergeDriver{

   
	public static void main(String[] args) {
	  SimpleDB.init("studentdb"); 
	  Transaction tx = new Transaction();
	  Plan p = new TablePlan("messy", tx);
	  List<String> fields = new ArrayList<String>();
	  fields.add("col1");
	  SortPlan sp = new SortPlan(p, fields, tx, 3);
	  sp.open();
	}
}
