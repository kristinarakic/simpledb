package rs.raf.simpledb.planner;

import net.sf.jsqlparser.JSQLParserException;
import rs.raf.simpledb.query.aggregation.AggregationFn;
import rs.raf.simpledb.query.aggregation.CountFn;
import rs.raf.simpledb.query.aggregation.GroupByPlan;
import rs.raf.simpledb.tx.Transaction;
import rs.raf.simpledb.query.*;
import rs.raf.simpledb.parse.*;
import rs.raf.simpledb.SimpleDBEngine;
import java.util.*;

/**
 * The simplest, most naive query planner possible.
 * @author Edward Sciore
 */
public class BasicQueryPlanner implements QueryPlanner {
   
   /**
    * Creates a query plan as follows.  It first takes
    * the product of all tables and views; it then selects on the predicate;
    * and finally it projects on the field list. 
    */
   public Plan createPlan(QueryData data, Transaction tx) throws JSQLParserException {
      //Step 1: Create a plan for each mentioned table or view
      List<Plan> plans = new ArrayList<Plan>();
      for (String tblname : data.tables()) {
         String viewdef = SimpleDBEngine.catalogMgr().getViewDef(tblname, tx);
         if (viewdef != null)
            plans.add(SimpleDBEngine.planner().createQueryPlan(viewdef, tx));
         else
            plans.add(new TablePlan(tblname, tx));
      }
      
      //Step 2: Create the product of all table plans
       Plan p = plans.remove(0);
       for (Plan next : plans)
           p = new CrossProductPlan(p, next);

       /* STEP 3: WHERE */
       if (data.pred() != null)
           p = new SelectionPlan(p, data.pred());

       /* STEP 4: GROUP BY */
       if (!data.groupBy().isEmpty()) {

           Collection<AggregationFn> aggs = new ArrayList<>();
           aggs.add(new CountFn("ocena")); // OVDJE JE COUNT

           p = new GroupByPlan(
                   p,
                   data.groupBy(),
                   aggs,   // agregacije
                   tx
           );
       }
       List<String> orderByList = new ArrayList<>(data.orderBy());
       /* STEP 5: ORDER BY */
       if (!data.orderBy().isEmpty()) {
           p = new SortPlan(
                   p,
                   orderByList,
                   tx);
       }

       /* STEP 6: PROJECTION */
       p = new ProjectionPlan(p, data.fields());

       return p;
   }
}
