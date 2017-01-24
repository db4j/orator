// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.nqzero.orator.Loader.NetLoader;
import com.nqzero.orator.Orator.Logable;
import com.nqzero.orator.Orator.NetClass;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import org.srlutils.DynArray;

public class OratorUtils {
    public static class Multi {
        public Message msg;
        public int offset;
        public boolean live;
        public Message.Sup sup;
        public int supID;
        public MessageSet msgset;
        public void set(Message msg, int supID,MessageSet msgset) {
            live = true;
            offset = 0;
            this.msg = msg;
            this.supID = supID;
            this.msgset = msgset;
        }
        public Message get() {
            int length = msgset.fits();
            Message sub;
            if (offset == 0) {
                sub = sup = new Message.Sup();
                sup.set( msg, supID, offset, length );
                sup.makeSup();
            } else
                sub = new Message.Sub().setSup( sup ).set( msg, supID, offset, length );
            offset += sub.core; // fixme
            live = offset < msg.size;
            if (!live) msg = null;
            return sub;
        }
    }
    public static class MessageSet {
        public Orator orator;
        public int header;
        public static int compositeHeaderSize = new Message.Composite().header;
        int maxSize = 1024, size;
        int almost = (int) (maxSize * .8);
        long startTime;
        DynArray.Objects<Message> list = new DynArray.Objects().init( Message.class );
        Nest.Key key;
        public MessageSet() {}
        public MessageSet set(Orator orator,int header,Nest.Key key) {
            this.orator = orator;
            this.key = key;
            this.header = header;
            return this;
        }
        public void init() { list.size = 0; size = 0; }

        public int headerSize(int additional) {
            return (list.size + additional <= 1) ? header : header + compositeHeaderSize;
        }
        public void add(Message msg) {
            if (size==0) startTime = System.currentTimeMillis();
            list.add( msg );
            size += msg.size;
        }
        /** does the msg fit in this set */
        public int fits() { return maxSize - size - headerSize(1); }
        /** does the msg fit in this set */
        public boolean fits(Message msg) { return headerSize(1) + size + msg.size <= maxSize; }
        /** does the set have space for reqsize bytes */
        public boolean fits(int reqsize) { return reqsize <= maxSize - size - headerSize(0); }

        public Message toMessage() {
            Message msg = list.size == 1
                    ? list.vo[ 0 ]
                    : orator.mutils.prep( new Message.Composite().set( list.trim() ) );
            init();
            return msg;
        }
    }
    // fixme::dry -- Entry and NakedList should get moved to srlutils ...
    // srl-2011.05.13 -- looked at moving this, and it looks prettty similar to Listee/Lister
    //   haven't been running orator lately, so don't want to mess with it, but seems obvious that the 2
    //   should be "merged"
    public static class Entry<TT> {
	public TT obj;
	public Entry<TT> next, prev;
	public Entry set(TT obj) { this.obj = obj; return this; }
    }
    public static class NakedList<TT> implements Iterable<TT> {
        public Entry<TT> base = new Entry();
        public NakedList() {  base.prev = base.next = base; }
        public NakedList(Entry<TT> head, Entry<TT> tail) {
            base.prev = tail;
            base.next = head;
            tail.next = base;
            head.prev = base;
        }

        public boolean empty() { return base.next == base; }
        public Entry<TT> peek() { return base.next; }
        public Entry<TT> pop() { return (base.next == base) ? null : remove( base.next ); }
        public Entry<TT> remove(Entry ee) {
            ee.prev.next = ee.next;
            ee.next.prev = ee.prev;
            ee.prev = ee.next = null;
            return ee;
        }
        /** add object to the tail, and return the added entry */
        public Entry<TT> push(TT obj) { return addBefore( new Entry().set( obj ), base ); }
        public Entry<TT> push(Entry<TT> ee) { return addBefore( ee, base ); }
        /** add ee to the list just before rel */
        public Entry<TT> addBefore(Entry ee,Entry rel) {
            ee.prev = rel.prev;
            ee.next = rel;
            ee.prev.next = ee;
            ee.next.prev = ee;
            return ee;
        }
        /** clear the list and return the head, with the head and tail links nulled */
        public Entry<TT> clear() {
            if (empty()) return null;
            Entry<TT> head = base.next, tail = base.prev;
            head.prev = null;
            tail.next = null;
            base.prev = base.next = base;
            return head;
        }

        public class Iter implements Iterator<TT> {
            public Entry<TT> pi = base;
            public boolean hasNext() { return pi.next != base; }
            public TT next() { pi = pi.next; return pi.obj; }
            public void remove() { NakedList.this.remove( pi ); }
        }
        public Iterator<TT> iterator() { return new Iter(); }
    }
    public static class MultiMap {
        public HashMap<SupidKey,MultiBuffer> map = new HashMap();
        public MultiBuffer get(SupidKey key) {
            MultiBuffer mb = map.get( key );
            if ( mb == null ) map.put( key, mb = new MultiBuffer() );
            return mb;
        }
    }
    public static class MultiBuffer {
        public DynArray.bytes data = new DynArray.bytes();
        public int sum, total;
        public boolean add(ByteBuffer buf,int offset,int length,int msgTotal) {
            if (msgTotal > 0) total = msgTotal;
            data.ensure( Math.max( offset+length, total ) );
            buf.get( data.vo, offset, length );
            sum += length;
            return sum == total;
        }
        public ByteBuffer getBuffer() {
            ByteBuffer buf = ByteBuffer.wrap( data.vo, 0, total );
            buf.limit( total );
            return buf;
        }
    }
    /**
     * a complete ecosystem for processing - a class loader and a registry of class ids (kryo)
     * there is a single kelly for all comm in which we are the upstream
     * there is one kelly per upstream for comm in which we are the downstream
     * fixme::completeness -- handle the case in which a downstream task acts as the upstream with another orator
     */
    public static class Kelly {
        NetLoader loader;
        public Example.MyKryo kryo;
        /** 0 --> never tested, 1 --> requested name, 2 --> present, 3 --> requested bytecode */
        public DynArray.ints have;
        public Orator orator;
        public int id;
        public Logable logger;
        public Key key;
        /** list of user messages that have been deferred, not thread safe - owned by the boss loop */
        public NakedList<Message.User> deferred = new NakedList();
        public Kelly() {}

        /** roa is used only if not upstream */
        public Kelly keyify(int upstreamKID,boolean upstream,Remote roa) {
            key = new Key();
            key.kid = upstreamKID;
            key.up = upstream;
            key.roa = roa;
            return this;
        }

        KryoFactory factory = new KryoFactory() {
            public Kryo create() { return kryo.dup(); }
        };

        KryoPool pool = new KryoPool.Builder(factory).build();
        public Example.MyKryo kryo() { return ((Example.MyKryo) pool.borrow()).pool(pool); }

        public void assertUpstream(boolean status) {
            boolean up = loader==null;
            if (up != status)
                throw new AssertionError();
        }
        
        /**
         * initialize the Kelly for god-mode, ie the most top-level authority. null values are shown
         *    explicitly, since they're part of the "API"
         */
        public Kelly init(Orator orator) {
            id = orator.kellyID++;
            loader = null;
            have = null;
            kryo = null;
            this.orator = orator;
            keyify( id, true, null );
            return this;
        }
        /** initialize the Kelly with an upstream */
        public Kelly init(Orator orator,Nest upstream,Logable logger) {
            id = orator.kellyID++;
            loader = new NetLoader( orator ).init( upstream );
            kryo = new Example.MyKryo().init();
            have = new DynArray.ints();
            this.orator = orator;
            this.logger = logger;
            return this;
        }
        public void assertBoss() {
            org.srlutils.Simple.hardAssert( Thread.currentThread(), orator.boss.thread, null );
        }
        public void regID(Class klass,NetClass nc) {
            assertBoss();
            assertUpstream(false);
            kryo.register( klass, nc.classID ); // boss down
            have.set( nc.classID, 2 );
        }
        /** define the class if defined, return whether we need to request bytecode, and mark the have cache */
        public boolean checkClass(NetClass nc) {
            assertBoss();
            int haveit = nc.classID < have.size ? have.get( nc.classID ) : 0;
            if (haveit <= 1) {
                Class klass = Loader.checkClass( nc.name );
                if ( klass != null ) regID( klass, nc );
                else {
                    have.set( nc.classID, 3 );
                    return loader.requested( nc.name );
                }
            }
            return false;
        }
        public void addCode(NetClass nc) {
            assertUpstream(false);
            assertBoss();
            Class klass = loader.defineClass( nc );
            regID( klass, nc );
        }
        /**
         * verify that all classes needed to deserialize user are registered with kryo
         *   if needs is non-null, add needed IDs to it
         *   if user is deserializable add it to delegateq, otherwise to deferq
         */
        public boolean checkIDs(Message.User user,DynArray.ints needs) {
            assertBoss();
            // fixme:clarity -- this method is pretty simple, but it looks confusing, ifs buried in ifs
            if (have == null) return true;
            // long-term should prolly support reverse-net-class-loading
            // but for now, require all classes to be available on the upstream
            assertUpstream(false);
            boolean doAdd = needs != null;
            int [] cids = user.classIDs;
            boolean ready = true;
            for (int ii = 0; ii < cids.length; ii++) {
                int cid = cids[ ii ];
                int cur = cid < have.size ? have.get( cid ) : 0;
                if ( cur == 0 ) {
                    // boss down
                    Registration rc = user.nest.kelly.kryo.getRegistration(cid);
                    if (rc == null) { have.set( cid, 1 ); if (doAdd) needs.add( cid ); }
                    else            { have.set( cid, 2 );                    cur = 2; }
                }
                ready &= (cur == 2);
            }
            return ready;
        }
        public static class Key {
            public int kid;
            public boolean up;
            /** only used if not up */
            public Remote roa;

            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            public boolean equals(Object obj) {
                Key oo = (Key) obj;
                return kid==oo.kid && up==oo.up && ( up || roa==oo.roa );
            }
            public int hashCode() { return autoCode( kid, up ? 1 : roa.hashCode() ); }
            public String toString() { return "Partner::Lookup::" + kid + "/" + (up ? "" : roa); }
        }
    }

    public static class SupidKey {
        public Remote roa;
        public int supID;
        public SupidKey(Remote roa,int supID) { this.roa = roa; this.supID = supID; }
        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        public boolean equals(Object obj) {
            SupidKey oo = (SupidKey) obj;
            return roa==oo.roa && supID==oo.supID;
        }
        public int hashCode() { return autoCode( supID, roa.hashCode() ); }
        public String toString() { return "SupidKey::" + roa + "/" + supID; }
    }

    /**
     * a (kelly,address) pair ... specifies the processing environment for a message
     * only hashable in homogenous/non-null environs, ie equals doesn't check type
     */
    public static class Nest {
        public Kelly kelly;
        public Remote roa;
        public int nextPacketID = 1, oldest, oldestSent;
        public Nest() {}
        public Nest set(Kelly kelly,Remote roa) {
            this.kelly = kelly;
            this.roa = roa;
            return this;
        }
        public Key key() { return new Key(); }
        public class Key {
            Nest ref = Nest.this;
            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            public boolean equals(Object obj) { Nest oo = ((Nest.Key) obj).ref; return kelly==oo.kelly && roa==oo.roa; }
            public int hashCode() { return kelly.hashCode()*67 + roa.hashCode(); }
        }
    }

    public static class Kiss2 {
        public Nest nest;
        public int id;
        public Kiss2 set(Nest nest, int id) { this.nest = nest; this.id = id; return this; }

        public String toString() { return "Kiss -- " + nest + "/" + id; }

        @SuppressWarnings( { "EqualsWhichDoesntCheckParameterClass" })
        public boolean equals(Object obj) { Kiss2 oo = (Kiss2) obj; return nest == oo.nest && id == oo.id; }
        public int hashCode() { return 67 * nest.hashCode() + id; }
    }

    /**
     * reference to an Orator, ie inet address + port ... the java.net stuff doesn't give a contract for
     * equals and hashcode - the source appears to do what i want, but wrap it in case it's not universally true
     */
    public static class Remote {
        public InetAddress inet;
        public int port;

        public Remote set(InetAddress inet,int port) {
            if (inet==null)
                try { inet = InetAddress.getLocalHost(); } catch (Exception ex) {}
            this.inet = inet;
            this.port = port;
            return this;
        }
        public String txtInfo() { return "" + inet + "/" + port; }
        public static class Key extends Remote {
            @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
            public boolean equals(Object obj) {
                Remote oo = (Remote) obj;
                return port == oo.port && inet.equals( oo.inet );
            }
            public int hashCode() { return inet.hashCode()*67 + port; }
        }
    }


    public static int autoCode(int v1) { int hash = 3; hash = 67*hash + v1; return hash; }
    public static int autoCode(int v1,int v2) {
        int hash = 3;
        hash = 67*hash + v1;
        hash = 67*hash + v2;
        return hash;
    }
    public static int autoCode(int v1,int v2,int v3) {
        int hash = 3;
        hash = 67*hash + v1;
        hash = 67*hash + v2;
        hash = 67*hash + v3;
        return hash;
    }
    public static int autoCode(int ... vals) {
        int hash = 3;
        for (int ii = 0; ii < vals.length; ii++)
            hash = 67 * hash + vals[ ii ];
        return hash;
    }



    /**
     *  info about the partner in an exchange ... one per received packet
     *  and one is needed to originate an exchange
     */
    public static class Partner2 implements Cloneable {
        // fixme::dry -- reqAck appears to be redundant ... packetID == 0 <--> reqAck = false
        public boolean upstream, reqAck;
        public int upstreamKID;
        public int sum, packetID;

        // filled on read only
        public static int header = 3*4;

        public Partner2 set(Nest nest,int _sum,boolean _reqAck,int _packetID) {
            sum = _sum;
            reqAck = _reqAck;
            packetID = _packetID;
            this.upstream = nest.kelly.key.up;
            this.upstreamKID = nest.kelly.key.kid;
            return this;
        }

        public Partner2() {}

        public Partner2 read(DatagramPacket packet,ByteBuffer buf) {
            sum = buf.getInt();
            packetID = buf.getInt();
            int info = buf.getInt();
            upstreamKID = info & 0x0fff;
            upstream = (info & 0x8000) == 0; // from a sender-POV, flip it
            reqAck = (info & 0x4000) != 0;
            return this;
        }
        public void write(ByteBuffer buf) {
            buf.putInt( sum );
            buf.putInt( packetID );
            int info = upstreamKID & 0x0fff;
            if (upstream) info |= 0x8000;
            if (reqAck) info |= 0x4000;
            buf.putInt( info );
        }

    }
    public static void main(String [] args) throws Exception {
        Orator.main( "stuff" );
    }

}
