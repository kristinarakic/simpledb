package rs.raf.simpledb.utils;

import rs.raf.simpledb.SimpleDBEngine;
import rs.raf.simpledb.utils.Page.PageFormatter;

import java.util.*;

/**
 * Manages the transaction's currently-pinned buffers. 
 * @author Edward Sciore
 */
public class BufferList {
   private Map<Block,Buffer> buffers = new HashMap<Block,Buffer>();
   private List<Block> pins = new ArrayList<Block>();
   private BufferMgr bufferMgr = SimpleDBEngine.bufferMgr();
   
   /**
    * Returns the buffer pinned to the specified block.
    * The method returns null if the transaction has not
    * pinned the block.
    * @param blk a reference to the disk block
    * @return the buffer pinned to that block
    */
   //Transaction.getInt/getString koriste ovo da dodju do podataka.
   public Buffer getBuffer(Block blk) {
      return buffers.get(blk);
   }
   
   /**
    * Pins the block and keeps track of the buffer internally.
    * @param blk a reference to the disk block
    */
   // trazi od globalnog menadzera da dovede blok u RAM, zapamti, doda u pins
   public void pin(Block blk) {
      Buffer buff = bufferMgr.pin(blk);
      buffers.put(blk, buff);
      pins.add(blk);
   }
   
   /**
    * Appends a new block to the specified file
    * and pins it.
    * @param filename the name of the file
    * @param fmtr the formatter used to initialize the new page
    * @return a reference to the newly-created block
    */
   public Block pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = bufferMgr.pinNew(filename, fmtr);
      Block blk = buff.block();
      buffers.put(blk, buff);
      pins.add(blk);
      return blk;
   }
   
   /**
    * Unpins the specified block.
    * @param blk a reference to the disk block
    */
   // govori globalnom menadzeru da unpinuje block, ako vise nema pinova, brise ga iz liste
   public void unpin(Block blk) {
      Buffer buff = buffers.get(blk);
      bufferMgr.unpin(buff);
      pins.remove(blk);
      if (!pins.contains(blk))
         buffers.remove(blk);
   }
   
   /**
    * Unpins any buffers still pinned by this transaction.
    */
   public void unpinAll() {
      for (Block blk : pins) {
         Buffer buff = buffers.get(blk);
         bufferMgr.unpin(buff);
      }
      buffers.clear();
      pins.clear();
   }
}