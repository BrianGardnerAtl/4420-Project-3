package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

/**
 * The Plan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
public class SortPlan implements Plan {
   private Plan p;
   private Transaction tx;
   private Schema sch;
   private RecordComparator comp;
   private int recordsPerBlock;
   
   /**
    * Creates a sort plan for the specified query.
    * @param p the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx the calling transaction
    */
   public SortPlan(Plan p, List<String> sortfields, Transaction tx) {
      this.p = p;
      recordsPerBlock = p.recordsOutput() / p.blocksAccessed();
      this.tx = tx;
      sch = p.schema();
      comp = new RecordComparator(sortfields);
   }
   
   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SortScan for final merging.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      Scan src = p.open();
      List<TempTable> runs = splitIntoRuns(src);
      src.close();
      // Print the contents of the initial runs
      while (runs.size() > 2)
         runs = doAMergeIteration(runs);
      return new SortScan(runs, comp);
   }
   
   /**
    * Returns the number of blocks in the sorted table,
    * which is the same as it would be in a
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // does not include the one-time cost of sorting
      Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
      return mp.blocksAccessed();
   }
   
   /**
    * Returns the number of records in the sorted table,
    * which is the same as in the underlying query.
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }
   
   /**
    * Returns the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the sorted table, which
    * is the same as in the underlying query.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
   
   private List<TempTable> splitIntoRuns(Scan src) {
      List<TempTable> temps = new ArrayList<TempTable>();
      src.beforeFirst();
      if (!src.next())
         return temps;
      TempTable currenttemp = new TempTable(sch, tx);
      temps.add(currenttemp);
      UpdateScan currentscan = currenttemp.open();
      int recordsInserted = 0;
      while (copy(src, currentscan)){
        recordsInserted++;
        if(recordsInserted==recordsPerBlock){
          recordsInserted = 0;
          // Sort the records in the currentscan
          sortScan(currentscan);
          // close the scan and open another
          currentscan.close();
          currenttemp = new TempTable(sch, tx);
          temps.add(currenttemp);
          currentscan = (UpdateScan) currenttemp.open();
        }
      }
      currentscan.close();
      return temps;
   }
   
   private List<TempTable> doAMergeIteration(List<TempTable> runs) {
      List<TempTable> result = new ArrayList<TempTable>();
      while (runs.size() > 1) {
         TempTable p1 = runs.remove(0);
         TempTable p2 = runs.remove(0);
         result.add(mergeTwoRuns(p1, p2));
      }
      if (runs.size() == 1)
         result.add(runs.get(0));
      return result;
   }
   
   private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
      Scan src1 = p1.open();
      Scan src2 = p2.open();
      TempTable result = new TempTable(sch, tx);
      UpdateScan dest = result.open();
      
      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      while (hasmore1 && hasmore2)
         if (comp.compare(src1, src2) < 0)
         hasmore1 = copy(src1, dest);
      else
         hasmore2 = copy(src2, dest);
      
      if (hasmore1)
         while (hasmore1)
         hasmore1 = copy(src1, dest);
      else
         while (hasmore2)
         hasmore2 = copy(src2, dest);
      src1.close();
      src2.close();
      dest.close();
      return result;
   }
   
   private boolean copy(Scan src, UpdateScan dest) {
      dest.insert();
      for (String fldname : sch.fields())
         dest.setVal(fldname, src.getVal(fldname));
      return src.next();
   }

   // Sort the records in the update scan
   private void sortScan(UpdateScan scan){
     // make temporary update scans to hold the sorted records
     UpdateScan tempScan = new TempTable(sch, tx).open();
     UpdateScan newScan = new TempTable(sch, tx).open();
     boolean more;
     int i;
     RID minRid;
     for(i=0; i<recordsPerBlock; i++){
       scan.beforeFirst();
       minRid = scan.getRid();
       more = copy(scan, tempScan);
       while(more){
         // find minimum record in scan
         if(comp.compare(scan, tempScan) < 0){
           // record in scan is smaller so switch
           tempScan.delete();
           minRid = scan.getRid();
           more = copy(scan, tempScan);
         }
         else{
           more = scan.next();
         }
       }
       // copy min record in tempScan to newScan
       copy(tempScan, newScan);
       // delete min record from scan
       scan.moveToRid(minRid);
       scan.delete();
     }
     scan = newScan;
   }
}
