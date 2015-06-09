/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package threading;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Sachin
 */
public class BlockingQueues {
    protected LinkedBlockingQueue<Integer> blockingQueue;
    private Queue<Integer>  AQueue = new LinkedList<>();
    byte[] bytes = new byte[400*1024*1024];
    
    public BlockingQueues() {
            this.blockingQueue = new LinkedBlockingQueue<Integer>();
    }

   class PutThread implements Runnable
   {
    @Override
    public void run() {
            while (true) {
                            try {
                                synchronized(this){
                                AQueue.add(1);
                                AQueue.remove();
                                blockingQueue.remove();
                                blockingQueue.add(1);
                                notifyAll();
                                }
                                RandomAccessFile fp;
                                fp = new RandomAccessFile("F:\\2002MB.txt", "r");
                                Stopwatch sw = new Stopwatch();
                                fp.read(bytes);
                                System.out.println(sw.elapsedTime());
                                //blockingQueue.;
                            } catch (FileNotFoundException ex) {
                                Logger.getLogger(BlockingQueues.class.getName()).log(Level.SEVERE, null, ex);
                            } catch (IOException ex) {
                            Logger.getLogger(BlockingQueues.class.getName()).log(Level.SEVERE, null, ex);
                            }
                    } 
                    }
            }
   }


