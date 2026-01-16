package rs.raf.simpledb.planner;

import net.sf.jsqlparser.JSQLParserException;
import rs.raf.simpledb.tx.Transaction;
import rs.raf.simpledb.parse.*;
import rs.raf.simpledb.query.*;

/**
 * The object that executes SQL statements.
 * @author sciore
 */
public class Planner {
   private QueryPlanner qplanner;
   private UpdatePlanner uplanner;

   public Planner(QueryPlanner qplanner, UpdatePlanner uplanner) {
      this.qplanner = qplanner;
      this.uplanner = uplanner;
   }

   /**
    * Creates a plan for an SQL select statement, using the supplied planner.
    * @param qry the SQL query string
    * @param tx the transaction
    * @return the scan corresponding to the query plan
    */
   public Plan createQueryPlan(String qry, Transaction tx) throws JSQLParserException {
      JSQLParser JSQLParser = new JSQLParser();
      QueryData data = JSQLParser.query(qry);

      return qplanner.createPlan(data, tx);
   }

   /**
    * Vraca rezultat parsiranja
    * @param sqlQuery
    * @return
    */
   public QueryData getParsingResult(String sqlQuery) throws JSQLParserException {
	   JSQLParser JSQLParser = new JSQLParser();
	   QueryData data = JSQLParser.query(sqlQuery);

	   return data;
   }

   /**
    * Vraca logicki plan upita od rezultata parsiranja
    * @param queryData
    * @param tx
    * @return
    */
   public Plan createQueryPlanFromParsingResult(QueryData queryData, Transaction tx) throws JSQLParserException {

	   return qplanner.createPlan(queryData, tx);
   }
   /**
    * Executes an SQL insert, delete, modify, or
    * create statement.
    * The method dispatches to the appropriate method of the
    * supplied update planner,
    * depending on what the JSQLParser returns.
    * @param cmd the SQL update string
    * @param tx the transaction
    * @return an integer denoting the number of affected records
    */
   public int executeUpdate(String cmd, Transaction tx) throws JSQLParserException {
      JSQLParser JSQLParser = new JSQLParser();
      Object obj = JSQLParser.updateCmd(cmd);
      if (obj instanceof InsertData)
         return uplanner.executeInsert((InsertData)obj, tx);
      else if (obj instanceof DeleteData)
         return uplanner.executeDelete((DeleteData)obj, tx);
      else if (obj instanceof ModifyData)
         return uplanner.executeModify((ModifyData)obj, tx);
      else if (obj instanceof CreateTableData)
         return uplanner.executeCreateTable((CreateTableData)obj, tx);
      else if (obj instanceof CreateViewData)
         return uplanner.executeCreateView((CreateViewData)obj, tx);
      else if (obj instanceof CreateIndexData)
         return uplanner.executeCreateIndex((CreateIndexData)obj, tx);
      else
         return 0;
   }
}
