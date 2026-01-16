package rs.raf.simpledb.utils;

import rs.raf.simpledb.SimpleDBEngine;

public class LogMgr {
	/**
	    * The location where the pointer to the last integer in the page is.
	    * A value of 0 means that the pointer is the first value in the page.
	    */
	   public static final int LAST_POS = 0;

	   private String logfile;
	   private Page mypage = new Page();
	   private Block currentblk;
	   private int currentpos;

	   /**
	    * Creates the manager for the specified log file.
	    * If the log file does not yet exist, it is created
	    * with an empty first block.
	    * This constructor depends on a {@link FileMgr} object
	    * that it gets from the method
	    * {@link simpledb.server.SimpleDB#fileMgr()}.
	    * That object is created during system initialization.
	    * Thus this constructor cannot be called until
	    * {@link simpledb.server.SimpleDB#initFileMgr(String)}
	    * is called first.
	    * @param logfile the name of the log file
	    */
	   
	   public LogMgr(String logfile) {
	      this.logfile = logfile;
	      int logsize = SimpleDBEngine.fileMgr().size(logfile);
	      if (logsize == 0)
	         appendNewBlock();
	      else {
	         currentblk = new Block(logfile, logsize-1);
	         mypage.read(currentblk);
	         currentpos = getLastRecordPosition() + Page.INT_SIZE;
	      }
	   }
	   
	   /**
	    * Returns the LSN of the most recent log record.
	    * As implemented, the LSN is the block number where the record is stored.
	    * Thus every log record in a block has the same LSN.
	    * @return the LSN of the most recent log record
	    */
	   private int currentLSN() {
	      return currentblk.number();
	   }

	   /**
	    * Ensures that the log records corresponding to the
	    * specified LSN has been written to disk.
	    * All earlier log records will also be written to disk.
	    * @param lsn the LSN of a log record
	    */
	   public void flush(int lsn) {
	      if (lsn >= currentLSN())
	         flush();
	   }

	   
	   /**
	    * Writes the current page to the log file.
	    */
	   private void flush() {
	      mypage.write(currentblk);
	   }

	   /**
	    * Clear the current page, and append it to the log file.
	    */
	   private void appendNewBlock() {
	      setLastRecordPosition(0);
	      currentpos = Page.INT_SIZE;
	      currentblk = mypage.append(logfile);
	   }
	   
	   private int getLastRecordPosition() {
	      return mypage.getInt(LAST_POS);
	   }

	   private void setLastRecordPosition(int pos) {
	      mypage.setInt(LAST_POS, pos);
	   }
		   
}
