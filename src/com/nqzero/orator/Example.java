// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.ReferenceResolver;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import java.io.Serializable;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class Example {



    public static class Other {
        public int kcar;
        public String model;
        public MyObj obj;
        public String toString() { return String.format( "Other: %d, %s\n", kcar, model ); }
    }

    public static class MyObj {
        public Other other;
        public String name;
        public int val;
        public double xo;

        public MyObj() {}
        public MyObj(String name, int val, double xo,int kcar,String model) {
            this.name = name;
            this.val = val;
            this.xo = xo;
            other = new Other();
            other.kcar = kcar;
            other.model = model;
            other.obj = this;
        }
        public String toString() {
            String res = String.format( "MyObj: %s, %d, %8.3f\n", name, val, xo );
            res += other.toString();
            res += other.obj == this;
            return res;
        }

    }

    public interface Dummyable extends Serializable {
        void dummy();
    }
    Dummyable dummy = () -> {
        System.out.println("dummy.lambda.this: " + this);
    };
    public static class MyContext {
        public HashSet<Integer> klasses = new HashSet();
        public void clear() { klasses.clear(); }
        public int [] getClassIDs() {
            int ii = 0;
            int [] ids = new int[ klasses.size() ];
            for (int id : klasses) ids[ ii++ ] = id;
            return ids;
        }
        public void printIDs(Kryo kryo) {
            for (int id : getClassIDs()) {
                Registration rc = kryo.getRegistration(id);
                System.out.format( "%5d -- %s\n", id, rc.getType() );
            }
        }
    }

    public static class MyKryo extends Kryo {

        MyContext my = new MyContext();
        final ClassResolver resolver;
        final private MyKryo master;

        protected MyKryo(SubResolver $resolver,ReferenceResolver referenceResolver,MyKryo $master) {
            super($resolver,referenceResolver);
            resolver = $resolver;
            master = $master;
        }
        
        public MyKryo dup() {
            MyKryo dup = new MyKryo(new SubResolver(resolver),new MapReferenceResolver(),this);
            dup.init();
            return dup;
        }
        
        public MyKryo(ClassResolver $resolver) {
            super($resolver,new MapReferenceResolver());
            resolver = (Resolver) getClassResolver();
            master = this;
        }

        public MyKryo() {
            super(new Resolver(),new MapReferenceResolver());
            resolver = (Resolver) getClassResolver();
            master = this;
        }

        public void printTypes() {
            System.out.println( "-------------------- kryo types -----------------------------" );
//            Set<Entry<Class, RegisteredClass>> set = classToRegisteredClass.entrySet();
//            for (Entry<Class, RegisteredClass> entry : set)
//                System.out.format( "types: %d --> %s\n", entry.getValue().getID(), entry.getKey() );
            System.out.println();
        }
        public void clearContext() {
            my.clear();
            reset();
        }

        /** add type to the list of tracked classes */
        public void trackClass(Class type) {
            Registration registeredClass;
            if (type==null) return;
            registeredClass = getRegistration(type);
            my.klasses.add( registeredClass.getId());
        }

	public int put(Output buffer, Object object, boolean writeIDs) {
            clearContext();
            writeClassAndObject( buffer, object );
            int idpos = buffer.position();
            if (writeIDs)
                writeClassAndObject(buffer, my.getClassIDs() );
            unpool();
            return idpos;
        }
        public MyKryo pool(KryoPool $pool) { pool = $pool; return this; }
        KryoPool pool;
        public void unpool() {
            if (pool != null) {
                pool.release(this);
                pool = null;
            }
        }

        public Object get(Input buffer) {
            clearContext();
            Object obj;
            obj = readClassAndObject( buffer );
            unpool();
            return obj;
        }
        public int [] getIDs(Input buffer,int idpos) {
            clearContext();
            if (idpos >=0) buffer.setPosition( idpos );
            return (int[]) readClassAndObject( buffer );
        }
        public MyKryo init() {
            register( int[].class );
            register(java.lang.invoke.SerializedLambda.class).setInstantiator(
                    new StdInstantiatorStrategy().
                            newInstantiatorOf(java.lang.invoke.SerializedLambda.class));
            register(ClosureSerializer.Closure.class, new ClosureSerializer());
            return this;
        }
	public int getNextRegistrationId () {
            // gets called during super constructor
            if (master==null)
                return super.getNextRegistrationId();
            synchronized (master.resolver) {
                return master==this
                        ? super.getNextRegistrationId()
                        : master.getNextRegistrationId();
            }
        }
        private int next;
	public Registration register (Class type,Serializer serializer) {
            // gets called during super constructor
            if (master==null)
		return getClassResolver()
                        .register(new Registration(type, serializer, next++));
            return master==this 
                ? super.register(type,serializer)
                : ((SubResolver) resolver).register(type,serializer);
	}
    }

    public static class Resolver extends DefaultClassResolver {
	public synchronized Registration registerImplicit(Class type) {
            return kryo.register(type);
	}
        public Registration writeClass(Output output,Class type) {
            ((MyKryo) kryo).trackClass(type);
            return super.writeClass(output,type);
        }
        // super impl only resets name-based structures - save the lock and skip it
        // already impl dependent
        public void reset() {}
        public synchronized Registration getRegistration(int classID) { return super.getRegistration(classID); }
        public synchronized Registration getRegistration(Class type) { return super.getRegistration(type); }
    }
    public static class SubResolver extends DefaultClassResolver {
        private final ClassResolver proxy;
        private int cache = -1;
	private Registration value;

        public Registration register(Registration reg) {
            Registration pr = proxy.getRegistration(reg.getId());
            return pr==null
                    ? local(proxy.register(reg))
                    : super.register(reg);
        }

        private SubResolver(ClassResolver $proxy) { proxy = $proxy; }

        public void reset() { super.reset(); }
        public Registration writeClass(Output output,Class type) {
            ((MyKryo) kryo).trackClass(type);
            return super.writeClass(output,type);
        }

        public Registration readClass(Input input) {
            int rid = input.readVarInt(true), classID = rid-2;
            if (rid==Kryo.NULL) return null;
            if (rid == cache) return value;
            Registration reg = super.getRegistration(classID);
            if (reg==null)
                reg = local(proxy.getRegistration(classID));
            if (reg==null)
                throw new KryoException("Encountered unregistered class ID: " + classID);
            cache = rid;
            value = reg;
            return reg;
        }
        public Registration registerImplicit(Class type) {
            Registration reg = super.getRegistration(type);
            return reg==null ? local(proxy.registerImplicit(type)) : reg;
        }
        public Registration getRegistration(int classID) {
            Registration reg = super.getRegistration(classID);
            return reg==null ? local(proxy.getRegistration(classID)) : reg;
        }
        public Registration getRegistration(Class type) {
            Registration reg = super.getRegistration(type);
            return reg==null ? local(proxy.getRegistration(type)) : reg;
        }
        public Registration register(Class type,Serializer ser) {
            Registration registration = super.getRegistration(type);
            if (registration != null) {
                registration.setSerializer(ser);
                return registration;
            }
            registration = proxy.getRegistration(type);
            if (registration==null)
		throw new RuntimeException("registration failed - must be registered with proxy first");
            Registration local = new Registration(type,ser,registration.getId());
            return super.register(local);
            
        }
        private Registration local(Registration pr) {
            if (pr==null) return null;
            int id = pr.getId();
            Class type = pr.getType();
            Serializer cpy, src;
            src = pr.getSerializer();
            cpy = ReflectionSerializerFactory.makeSerializer(kryo,src.getClass(),type);
            Registration local = new Registration(type,cpy,id);
            return super.register(local);
        }

    }

    public <TT> TT roundTrip(TT orig) {
        byte [] back = new byte[ 2048 ];
        MyKryo kryo = new MyKryo().init();
        MyKryo k1 = kryo.dup();
        MyKryo k2 = kryo.dup();
        Output buffer = new Output(back);
        System.out.println( "orig:\n" + orig );
        buffer.clear();
        k1.put( buffer, orig , true);
        k1.my.printIDs(k1);
        Input buffer2 = new Input(back,0,buffer.position());
        TT copy = (TT) k2.get(buffer2);
        System.out.println( "copy:\n" + copy );
        return copy;
    }


    public static class Stress {
        ConcurrentLinkedQueue<byte []> que = new ConcurrentLinkedQueue();
        AtomicInteger sum = new AtomicInteger();
        AtomicInteger pcount = new AtomicInteger();
        MyKryo kryo = new MyKryo().init();
        KryoFactory factory = new KryoFactory() {
            public Kryo create() { return kryo.dup(); }
        };

        KryoPool pool = new KryoPool.Builder(factory).build();
        public Example.MyKryo kryo() { return ((Example.MyKryo) pool.borrow()).pool(pool); }
        
    
        public class Producer extends Thread {
            MyKryo kryo = (MyKryo) factory.create();
            public void run() {
                while (sum.get() < nn)
                    runOnce();
            }
            public void runOnce() {
                byte [] data = new byte[64];
                Output buffer = new Output(data);
                buffer.clear();
                MyObj orig = new MyObj( "hello world", -1, 9.4, 66, "revis" );
                kryo.put( buffer, orig , true);
                buffer.close();
                que.offer(data);
                int cnt = pcount.incrementAndGet();
                if (cnt > tp)
                    org.srlutils.Simple.sleep(1);
            }
        }
        public class Consumer extends Thread {
            MyKryo kryo = (MyKryo) factory.create();
            public void run() {
                int total=0, ii=0;
                while (sum.get() < nn || pcount.get() > 0) {
                    byte [] data = que.poll();
                    if (data==null) { org.srlutils.Simple.sleep(0); continue; }
                    ii++;
                    Input buffer = new Input(data);
                    MyObj orig = (MyObj) kryo.get(buffer);
                    buffer.close();
                    total += orig.val;
                    pcount.decrementAndGet();
                    int num = sum.incrementAndGet();
                    if (num % nprint==0)
                        System.out.format("%12d %8d %8d %8d: %s\n",sum.get(),pcount.get(),ii,total,orig);
                }
            }
        }

        int nc=2, np=3;
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

    public void run() {
        MyObj orig = new MyObj( "hello world", -1, 9.4, 66, "revis" );
        roundTrip( orig );
        orig = new MyObj( "goodbye love", -2, 9.4, 88, "island" );
        roundTrip( orig );
    }

    
    public static void main(String [] args) throws Exception {
        Example example = new Example();
        example.roundTrip(example.dummy).dummy();
        
        example.run();
        Stress stress = new Stress();
        stress.start();
    }
}
