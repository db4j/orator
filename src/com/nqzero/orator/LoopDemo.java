// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class LoopDemo {
    public static class Stress {
        ConcurrentLinkedQueue<byte []> que = new ConcurrentLinkedQueue();
        AtomicInteger sum = new AtomicInteger();
        AtomicInteger pcount = new AtomicInteger();

        public Kryo make() {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(true);
            init(kryo);
            return kryo;
        }
    
        public class Producer extends Thread {
            Kryo kryo = make();
            public void run() {
                while (sum.get() < nn)
                    runOnce();
            }
            public void runOnce() {
                int cnt = pcount.incrementAndGet();
                if (cnt > tp)
                    org.srlutils.Simple.sleep( 10 );
                byte [] data = new byte[64];
                Output buffer = new Output(data);
                buffer.clear();
                Example.MyObj orig = new Example.MyObj( "hello world", -1, 9.4, 66, "brody" );
                kryo.writeObject(buffer, orig);
                buffer.close();
                que.offer(data);
            }
        }
        public class Consumer extends Thread {
            Kryo kryo = make();
            public void run() {
                int total=0, ii=0;
                for (; true; ii++) {
                    int cnt = pcount.decrementAndGet();
                    byte [] data = null;
                    while ((data = que.poll())==null) {
                        if (sum.get() >= nn) return;
                        org.srlutils.Simple.sleep(0);
                    }
                    Input buffer = new Input(data);
                    Example.MyObj orig = kryo.readObject(buffer,Example.MyObj.class);
                    buffer.close();
                    total += orig.val;
                    int num = sum.incrementAndGet();
                    if (num % nprint==0)
                        System.out.format("%d %d %d %d: %s\n",sum.get(),pcount.get(),ii,total,orig);
                    if (num >= nn && pcount.get()==0) break;
                }
            }
        }


        public void init(Kryo kryo) {
            kryo.register(Example.Other.class);
            kryo.register(Example.MyObj.class);
        }
        
        
        int nc=2, np=2;
        int nn = 1<<22;
        int tp = 1<<10;
        int nprint = nn/16;
        Producer [] producers = new Producer[np];
        Consumer [] consumers = new Consumer[nc];
        
        public void start() throws InterruptedException {
            
            for (int ii=0; ii < np; ii++) producers[ii] = new Producer();
            for (int ii=0; ii < nc; ii++) consumers[ii] = new Consumer();

            for (Producer pp : producers) pp.start();
            for (Consumer pp : consumers) pp.start();
            for (Producer pp : producers) pp.join();
            for (Consumer pp : consumers) pp.join();
        }
        
    }
    public static void main(String [] args) throws Exception {
        Stress stress = new Stress();
        stress.start();
    }
    
}
