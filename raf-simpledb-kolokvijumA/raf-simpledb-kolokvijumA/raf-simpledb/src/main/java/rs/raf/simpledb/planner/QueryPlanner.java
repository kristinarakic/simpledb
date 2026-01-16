package rs.raf.simpledb.planner;

import net.sf.jsqlparser.JSQLParserException;
import rs.raf.simpledb.tx.Transaction;
import rs.raf.simpledb.query.Plan;
import rs.raf.simpledb.parse.QueryData;

/**
 * The interface implemented by planners for 
 * the SQL select statement.
 * @author Edward Sciore
 *
 */
public interface QueryPlanner {
   
   /**
    * Creates a plan for the parsed query.
    * @param data the parsed representation of the query
    * @param tx the calling transaction
    * @return a plan for that query
    */
   public Plan createPlan(QueryData data, Transaction tx) throws JSQLParserException;
}
