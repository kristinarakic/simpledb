package rs.raf.simpledb.index;

import rs.raf.simpledb.query.Constant;
import rs.raf.simpledb.record.RID;

public class HashIndex implements Index {

	@Override
	public void beforeFirst(Constant searchkey) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean next() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RID getDataRid() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insert(Constant dataval, RID datarid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(Constant dataval, RID datarid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
