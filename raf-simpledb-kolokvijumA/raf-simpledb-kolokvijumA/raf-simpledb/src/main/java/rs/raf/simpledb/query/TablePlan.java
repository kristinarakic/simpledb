package rs.raf.simpledb.query;

import rs.raf.simpledb.SimpleDBEngine;
import rs.raf.simpledb.tx.Transaction;
import rs.raf.simpledb.metadata.*;
import rs.raf.simpledb.query.operators.Scan;
import rs.raf.simpledb.query.operators.TableScan;
import rs.raf.simpledb.record.*;

/** The Plan class corresponding to a table.
  * @author Edward Sciore
  */
public class TablePlan implements Plan {
   private Transaction tx;
   private TableInfo ti;
   private StatInfo si;
   
   /**
    * Creates a leaf node in the query tree corresponding
    * to the specified table.
    * @param tblname the name of the table
    * @param tx the calling transaction
    */
   public TablePlan(String tblname, Transaction tx) {
      this.tx = tx;
      ti = SimpleDBEngine.catalogMgr().getTableInfo(tblname, tx);
      si = SimpleDBEngine.catalogMgr().getStatInfo(tblname, ti, tx);
   }
   
   /**
    * Creates a table scan for this query.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      return new TableScan(ti, tx);
   }
   
   /**
    * Estimates the number of block accesses for the table,
    * which is obtainable from the statistics manager.
    * @see simpledb.query.Plan#blocksAccessed()
    */ 
   public int blocksAccessed() {
      return si.blocksAccessed();
   }
   
   /**
    * Estimates the number of records in the table,
    * which is obtainable from the statistics manager.
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return si.recordsOutput();
   }
   
   /**
    * Estimates the number of distinct field values in the table,
    * which is obtainable from the statistics manager.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return si.distinctValues(fldname);
   }
   
   /**
    * Determines the schema of the table,
    * which is obtainable from the catalog manager.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return ti.schema();
   }
   
   @Override
	public void printPlan(int indentLevel) {
		
		System.out.println("-".repeat(indentLevel)+"-> TABLE SCAN OF -->("+ ti.fileName()+")");
	}
}
