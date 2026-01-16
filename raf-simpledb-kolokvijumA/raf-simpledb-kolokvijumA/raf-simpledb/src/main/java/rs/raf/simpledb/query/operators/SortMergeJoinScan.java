package rs.raf.simpledb.query.operators;

import rs.raf.simpledb.operators.SortScan;
import rs.raf.simpledb.query.Constant;
import rs.raf.simpledb.query.operators.Scan;

public class SortMergeJoinScan implements Scan {
    private final Scan s1;
    private final Scan s2;
    private final String fld1;
    private final String fld2;

    private boolean hasmore1;
    private boolean hasmore2;

    // group state
    private boolean inGroup = false;
    private Constant joinval = null;

    // desni mora biti SortScan da bi save/restore radio
    private final SortScan right;

    public SortMergeJoinScan(Scan s1, Scan s2, String fld1, String fld2) {
        this.s1 = s1;
        this.s2 = s2;
        this.fld1 = fld1;
        this.fld2 = fld2;

        this.right = (s2 instanceof SortScan) ? (SortScan) s2 : null;

        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
        hasmore1 = s1.next();
        hasmore2 = s2.next();
        inGroup = false;
        joinval = null;
    }

    @Override
    public boolean next() {
        if (!hasmore1 || !hasmore2) return false;

        while (true) {

            // 1) Ako smo u grupi (isti joinval), prvo iscrpljuj desnu grupu
            if (inGroup) {
                hasmore2 = s2.next();

                if (hasmore2 && s2.getVal(fld2).equals(joinval)) {
                    // isti levi, sledeći desni u istoj grupi
                    return true;
                }

                // završili smo desnu grupu za trenutni levi
                hasmore1 = s1.next();

                if (hasmore1 && s1.getVal(fld1).equals(joinval)) {
                    // levi duplikat -> vrati desnu na početak grupe i vrati prvi par
                    if (right == null) throw new RuntimeException("Right input must be SortScan for Sort-Merge Join.");
                    right.restorePosition();
                    return true;
                }

                // levi ključ se promenio -> izađi iz group mode i nastavi merge
                inGroup = false;

                if (!hasmore1 || !hasmore2) return false;
                // nastavljamo u merge delu
            }

            // 2) Normalan merge (poređenje ključeva)
            Constant v1 = s1.getVal(fld1);
            Constant v2 = s2.getVal(fld2);

            int cmp = v1.compareTo(v2);

            if (cmp < 0) {
                hasmore1 = s1.next();
                if (!hasmore1) return false;
            } else if (cmp > 0) {
                hasmore2 = s2.next();
                if (!hasmore2) return false;
            } else {
                // MATCH: uđi u grupu
                joinval = v1;
                inGroup = true;

                if (right == null) throw new RuntimeException("Right input must be SortScan for Sort-Merge Join.");
                right.savePosition(); // mark početak desne grupe za joinval

                return true; // trenutni (s1,s2) je prvi rezultat
            }
        }
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }

    @Override
    public Constant getVal(String fldname) {
        if (s1.hasField(fldname)) return s1.getVal(fldname);
        return s2.getVal(fldname);
    }

    @Override
    public int getInt(String fldname) {
        if (s1.hasField(fldname)) return s1.getInt(fldname);
        return s2.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        if (s1.hasField(fldname)) return s1.getString(fldname);
        return s2.getString(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }
}
