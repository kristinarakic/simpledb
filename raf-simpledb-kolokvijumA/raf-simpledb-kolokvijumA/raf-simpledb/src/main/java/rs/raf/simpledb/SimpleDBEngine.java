package rs.raf.simpledb;


import rs.raf.simpledb.tx.Transaction;
import rs.raf.simpledb.utils.BufferMgr;
import rs.raf.simpledb.utils.FileMgr;
import rs.raf.simpledb.utils.LogMgr;
import rs.raf.simpledb.metadata.CatalogMgr;
import rs.raf.simpledb.planner.*;
//import simpledb.opt.HeuristicQueryPlanner;
//import simpledb.index.planner.IndexUpdatePlanner;

/**
 * The class that provides system-wide static global values.
 * These values must be initialized by the method
 * {@link #init(String) init} before use.
 * The methods {@link #initFileMgr(String) initFileMgr},
 * {@link #initFileAndLogMgr(String) initFileAndLogMgr},
 * {@link #initFileLogAndBufferMgr(String) initFileLogAndBufferMgr},
 * and {@link #initCatalogMgr(boolean, Transaction) initMetadataMgr}
 * provide limited initialization, and are useful for 
 * debugging purposes.
 * 
 * @author Edward Sciore
 */

public class SimpleDBEngine {
   public static int BUFFER_SIZE = 8;
   public static String LOG_FILE = "raf_simpledb.log";
   
   private static FileMgr     fm;
   private static BufferMgr   bm;
   private static LogMgr      logm;
   private static CatalogMgr ctlg;
   
   /**
    * Initializes the system.
    * This method is called during system startup.
    * @param dirname the name of the database directory
    */
   public static boolean init(String dirname) {
      initFileLogAndBufferMgr(dirname);
      Transaction tx = new Transaction();
      boolean isnew = fm.isNew();
      if (isnew)
         System.out.println("creating new database");
      else {
         System.out.println("recovering existing database");
         tx.recover();
      }
      initCatalogMgr(isnew, tx);
      tx.commit();
      
      return isnew;
   }
   
   // The following initialization methods are useful for 
   // testing the lower-level components of the system 
   // without having to initialize everything.
   
   /**
    * Initializes only the file manager.
    * @param dirname the name of the database directory
    */
   public static void initFileMgr(String dirname) {
      fm = new FileMgr(dirname);
   }
   
   /**
    * Initializes the file and log managers.
    * @param dirname the name of the database directory
    */
   public static void initFileAndLogMgr(String dirname) {
      initFileMgr(dirname);
      logm = new LogMgr(LOG_FILE);
   }
   
   /**
    * Initializes the file, log, and buffer managers.
    * @param dirname the name of the database directory
    */
   public static void initFileLogAndBufferMgr(String dirname) {
      initFileAndLogMgr(dirname);
      bm = new BufferMgr(BUFFER_SIZE);
   }
   
   /**
    * Initializes Catalog manager.
    * @param isnew an indication of whether a new
    * database needs to be created.
    * @param tx the transaction performing the initialization
    */
   public static void initCatalogMgr(boolean isnew, Transaction tx) {
      ctlg = new CatalogMgr(isnew, tx);
   }
   
   public static FileMgr     fileMgr()   { return fm; }
   public static BufferMgr   bufferMgr() { return bm; }
   public static LogMgr      logMgr()    { return logm; }
   public static CatalogMgr catalogMgr()     { return ctlg; }
   
   /**
    * Creates a planner for SQL commands.
    * To change how the planner works, modify this method.
    * @return the system's planner for SQL commands
    */
   public static Planner planner() {
      QueryPlanner  qplanner = new BasicQueryPlanner();
      UpdatePlanner uplanner = new BasicUpdatePlanner();
      return new Planner(qplanner, uplanner);
   }
}
