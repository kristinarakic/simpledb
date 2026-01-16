package rs.raf.simpledb.query;

import rs.raf.simpledb.query.operators.SortMergeJoinScan;
import rs.raf.simpledb.query.Plan;
import rs.raf.simpledb.query.SortPlan;
import rs.raf.simpledb.tx.Transaction;
import rs.raf.simpledb.query.operators.Scan;
import rs.raf.simpledb.record.Schema;

import java.util.List;

public class SortMergeJoinPlan implements Plan {
    private Plan p1, p2;
    private String fld1, fld2;
    private Transaction tx;
    private Schema schema = new Schema();

    public SortMergeJoinPlan(Plan p1, Plan p2, String fld1, String fld2, Transaction tx) {
        this.p1 = p1;
        this.p2 = p2;
        this.fld1 = fld1;
        this.fld2 = fld2;
        this.tx = tx;

        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Plan sp1 = new SortPlan(p1, List.of(fld1), tx);
        Plan sp2 = new SortPlan(p2, List.of(fld2), tx);

        Scan s1 = sp1.open();
        Scan s2 = sp2.open();

        return new SortMergeJoinScan(s1, s2, fld1, fld2);
    }

    @Override
    public int blocksAccessed() {
        // gruba procena (SortPlan.blocksAccessed ne uključuje cenu sortiranja)
        return p1.blocksAccessed() + p2.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        int v1 = p1.distinctValues(fld1);
        int v2 = p2.distinctValues(fld2);
        int maxV = Math.max(1, Math.max(v1, v2));
        return (p1.recordsOutput() * p2.recordsOutput()) / maxV;
    }

    @Override
    public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname)) return p1.distinctValues(fldname);
        return p2.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public void printPlan(int indentLevel) {
        System.out.println("-".repeat(indentLevel) + "SORT-MERGE JOIN (" + fld1 + "=" + fld2 + ") OF");
        p1.printPlan(indentLevel + 3);
        p2.printPlan(indentLevel + 3);
    }
}
