package rs.raf.simpledb.parse;

import rs.raf.simpledb.query.*;
import rs.raf.simpledb.query.aggregation.AggregationFn;

import java.util.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private Collection<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private Collection<String> groupBy;
   private Collection<String> orderBy;
   private Collection<AggregationFn> aggfns;

    public Collection<AggregationFn> aggFns() {
        return aggfns;
    }
   
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(Collection<String> fields, Collection<String> tables, Collection<String> groupBy, Collection<String> orderBy, Predicate pred, Collection<AggregationFn> aggfns) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.groupBy = groupBy;
      this.orderBy = orderBy;
      this.aggfns = aggfns;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a collection of field names
    */
   public Collection<String> fields() {
      return fields;
   }
    public Collection<String> groupBy() {
        return groupBy;
    }
    public Collection<String> orderBy() {
        return orderBy;
    }

   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }
}
