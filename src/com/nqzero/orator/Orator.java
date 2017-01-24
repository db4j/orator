// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.srlutils.data.LockingQ;
import com.nqzero.orator.Loader.NetLoader;
import com.nqzero.orator.Message.User;
import com.nqzero.orator.OratorUtils.Kelly;
import com.nqzero.orator.OratorUtils.Kiss2;
import com.nqzero.orator.OratorUtils.MessageSet;
import com.nqzero.orator.OratorUtils.NakedList;
import com.nqzero.orator.OratorUtils.Nest;
import com.nqzero.orator.OratorUtils.Partner2;
import com.nqzero.orator.OratorUtils.Remote;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.srlutils.DynArray;

/*
 * TODO:
 *   anti-congestion filter
 *   testing: performance, perm-gen, cleanup on close
 */


public class Orator {
    public static int defaultPort = 6177;
    private static final ThreadLocal<Example.MyKryo> kryos = new ThreadLocal<Example.MyKryo>();

    // socket configuration
    public InetSocketAddress addr;
    public DatagramSocket socket;

    // processing loops and resources
    public Example.MyKryo kryo2;
    public SendLoop send;
    public RecvLoop recv;
    public BossLoop boss;
    public UserLoop [] users;

    public MessageUtils mutils = new MessageUtils().init( Message.classList );
    public ConcurrentHashMap<Kelly.Key,Kelly> kellyMap = new ConcurrentHashMap();
    public ConcurrentHashMap<Remote.Key,Remote> remoteMap = new ConcurrentHashMap();
    public ConcurrentHashMap<Nest.Key,Nest> nestMap = new ConcurrentHashMap();
    public Kelly root;
    public int kellyID = 0;

    public Logger logger = new Logger();
    public long seed;
    public Random rand;

    public StringBuffer sbuf = null;
    public Orator setLog(StringBuffer log) { sbuf = log; return this; }

    /** initialize the net connection to the given port, 0 --> ephemeral port */
    public Orator init(Integer port) {
        try {
            seed = seed==0 ? new Random().nextLong() : seed;
            rand = new Random( seed );
            Remote remote = new Remote().set(null,port);
            addr = new InetSocketAddress(remote.inet, port);
            socket = new DatagramSocket( addr );
            logger.log( "Orator::init -- port: %d --> %s, seed:%d\n", port, socket.getLocalPort(), seed );
            kryo2 = new Example.MyKryo().init();
            for (Class klass : Admin.classList) kryo2.register( klass );
            kryos.set(kryo2);
            send = new SendLoop().init();
            recv = new RecvLoop().init();
            boss = new BossLoop().init();
            users = new UserLoop[ 8 ];
            for (int ii = 0; ii < users.length; ii++) users[ii] = new UserLoop().init();
            root = new Kelly().init( this );
            kellyMap.put( root.key, root );
            return this;
        } catch (Exception ex) { throw new RuntimeException( ex ); }
    }
    public void start() {
        for (Loop loop : new Loop[] { recv, send, boss }) loop.start();
        for (Loop loop : users) loop.start();
    }

    public void shutdown() {
    }




    public interface Logable {
        void log(String fmt,Object... args);
    }


    public Example.MyKryo kryo2() {
        Example.MyKryo k2 = kryos.get();
        if (k2==null) kryos.set(k2 = kryo2.dup());
        return k2;
    }
    public abstract class Loop implements Runnable, Logable {
        Example.MyKryo kryo = kryo2.dup();
        public Orator orator = Orator.this;
        public Thread thread;
        public String roleName;
        public void start() {
            String loc = socket.getLocalPort() + ":" + roleName;
            thread = new Thread( this, loc );
            thread.start();
        }
        public void log(String fmt,Object ... args) {
            if (!virus.dbg) return;
            String comp = thread.getName() + " -- " + fmt + "\n";
            logger.print( String.format( comp, args ) );
        }
        public boolean check() {
            Thread current = Thread.currentThread();
            if (current==thread) return true;
            throw new AssertionError();
        }
    }
    public boolean check() {
        Thread current = Thread.currentThread();
        System.out.println(current);
        return true;
    }

    public static void dumpLog(StringBuffer buf) {
        String txt;
        synchronized( buf ) {
            txt = buf.toString();
            buf.setLength( 0 );
        }
        System.out.print( txt );
        System.out.println( "dumpLog -- log dumped, waiting ..." );
    }
    public class Logger implements Logable {
        public void print(String txt) {
            if (sbuf != null) sbuf.append( txt );
            else System.out.print( txt );
        }
        public void log(String fmt,Object ... args) {
            if (!virus.dbg) return;
            String comp = String.format( "%s:%s -- %s\n", Thread.currentThread().getName(), socket.getLocalPort(), fmt );
            print( String.format( comp, args ) );
        }
    }

    public static class PacketRecord {
        public Message msg;
        public int packetID;

        public String toString() { return (msg==null?"null":msg.toString()) + "/" + packetID; }

        public PacketRecord(Message msg, int packetID) {
            this.msg = msg;
            this.packetID = packetID;
        }

    }

    public void diag() {
        System.out.println( "checking the sent map" );
        for (Kiss2 kiss : send.sentMap.keySet()) {
            System.out.println( kiss );
        }
        System.out.println( "checking the time map" );
        for (Entry<Longm,PacketRecord> entry : send.timeMap.entrySet()) {
            System.out.println( entry.getKey() + " --> " + entry.getValue() );
        }

    }


    public static class Longm implements Comparable<Longm> {
        long time;
        Message msg;

        public Longm set(long time, Message msg) { this.time = time; this.msg = msg; return this; }


        public int compareTo(Longm oo) {
            if (time == oo.time) {
                if (msg == oo.msg) return 0;
                if (msg == null) return -1;
                if (oo.msg == null) return 1;
                int x1 = System.identityHashCode( msg );
                int x2 = System.identityHashCode( oo.msg );
                return x1 - x2;
            }
            long dd = time - oo.time;
            return dd==0 ? 0 : dd > 0 ? 1 : -1;
        }

        @Override
        public boolean equals(Object obj) {
            if ( obj == null ) return false;
            if ( getClass() != obj.getClass() ) return false;
            final Longm other = (Longm) obj;
            if ( this.time != other.time ) return false;
            if ( this.msg != other.msg && (this.msg == null || !this.msg.equals( other.msg )) ) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 79 * hash + (int) (this.time ^ (this.time >>> 32));
            hash = 79 * hash + (this.msg != null ? this.msg.hashCode() : 0);
            return hash;
        }
    }


    public class SendLoop extends Loop {
        public BlockingQueue<Message> sendq;
        public byte [] data;
        public ByteBuffer sendBuffer;
        OratorUtils.Multi multi = new OratorUtils.Multi();
        DatagramPacket packetOut;
        public int packetID = 0, supID = 0, msgID = 1;
        public ConcurrentHashMap<Integer,Callback> callbackMap = new ConcurrentHashMap();
        public NakedList<MessageSet> setq = new NakedList();
        public HashMap<Nest.Key,OratorUtils.Entry<MessageSet>> setMap = new HashMap();
        public ConcurrentHashMap<Kiss2,Boolean> recvMap = new ConcurrentHashMap();
        public ConcurrentHashMap<Kiss2,PacketRecord> sentMap = new ConcurrentHashMap();
        public TreeMap<Longm,PacketRecord> timeMap = new TreeMap();
        // fixme:robustness/security - increase delay when congestion detected
        public int jitter = 100, retryTo = 2*jitter + 100;

        public SendLoop init() {
            sendq = new LinkedBlockingQueue();
            data = new byte[ 2*1024 ];
            packetOut = new DatagramPacket( data, 0 );
            sendBuffer = ByteBuffer.wrap( data );
            roleName = "send";
            return this;
        }
        public void send(OratorUtils.Entry<MessageSet> entry) {
            setq.remove( entry );
            setMap.remove( entry.obj.key );
            Message msg = entry.obj.toMessage();
            int pid = msg.reqAck ? msg.nest.nextPacketID++ : 0;
            send( msg, pid );
        }
        public void send(Message msg,int pid) {
            Remote roa = msg.nest.roa;
            Partner2 partner = new Partner2().set( msg.nest, 0, msg.reqAck, pid );
            sendBuffer.clear();
            partner.write( sendBuffer );
            msg.write( sendBuffer );
            if (msg.reqAck) {
                long time = System.currentTimeMillis() + retryTo;
                Longm stamp = new Longm().set( time, msg );
                PacketRecord tell = new PacketRecord( msg, pid );
                Kiss2 kiss = new Kiss2().set( msg.nest, pid );
                sentMap.put( kiss, tell );
                timeMap.put( stamp, tell );
            }
            sendBuffer.flip();
            int len = sendBuffer.limit();
            log( "sending,%d: id:%d, %d bytes to %s, %s", msg.nest.kelly.id, pid, len, roa.txtInfo(), msg.txt() );
            packetOut.setLength( len );
            InetSocketAddress isa = new InetSocketAddress( roa.inet, roa.port );
            sendPacket( packetOut, isa );
        }
        public void sendPacket(DatagramPacket packetOut,SocketAddress dest) {
            try {
                packetOut.setSocketAddress( dest );
                socket.send( packetOut );
            } catch (Exception ex) { throw new RuntimeException( ex ); }
        }
        /** get a value from the send-queue, waiting at most timeout milliseconds, unless take */
        public Message get(boolean take,long timeout) throws InterruptedException {
            // the docs don't clearly define what happens for timeouts of zero ... source says that it's treated
            // like poll(), but there's an unneeded lock. so just do poll() if that's what we mean ...
            Message msg = take ? sendq.take()
                    : timeout == 0 ? sendq.poll() :
                        sendq.poll( timeout, TimeUnit.MILLISECONDS );
            return msg;
        }
        public void run() {
            OratorUtils.Entry<MessageSet> first, set;
            MessageSet set2;
            Iterator<Entry<Longm, PacketRecord>> iter;
            Entry<Longm, PacketRecord> pre;
            Message saved = null;
            while (! thread.isInterrupted()) {
                try {
                    Message msg = saved;
                    saved = null;
                    set = null;
                    long current = System.currentTimeMillis();
                    iter = timeMap.entrySet().iterator();
                    pre = null;
                    Longm to = null;

                    if (msg==null && multi.live) {
                        set = getSet( multi.msg, true );
                        msg = multi.get();
                    }
                    if (msg==null && iter.hasNext()) {
                        pre = iter.next();
                        to = pre.getKey();
                        PacketRecord pr = pre.getValue();
                        // pr.msg gets nulled on removal, so use a local to keep things consistent
                        Message pastMsg = pr.msg;
//                        log( "sending: checking ... %d", to - current );
                        if (pastMsg == null) {
                            timeMap.remove( to );
                            continue;
                        }
                        if (to.time <= current) {
                            virus.resend++;
                            log( "resending,%d: id:%d", pastMsg.nest.kelly.id, pr.packetID );
                            timeMap.remove( to );
                            send( pastMsg, pr.packetID );
                            continue;
                        }
                    }
                    if (msg == null) {
                        boolean noParts = setq.empty();
                        if (noParts && pre==null) msg = get( true, 0 );
                        else {
                            first = setq.peek();
                            long delta = to==null ? -1 : to.time - current;
                            long timeout = delta, parto = -1;
                            if (! noParts) parto = timeout = first.obj.startTime + jitter - current;
                            if (pre != null) timeout = Math.min( delta, timeout );
                            msg = get( false, timeout );
                            if (msg==null && timeout == parto) { send( first ); continue; }
                            if (msg==null) continue;
                        }
                    }

                    if (set == null) set = getSet( msg, false );

                    set2 = set.obj;

                    mutils.prep( msg );
                    if (set2.fits(msg)) {
                        set2.add( msg );
                        // fixme:opt - never wait to send multi sets, setq.move or another list
                        if (multi.live && almost(set2) || !set2.fits(32))
                            send(set);
                    }
                    else if (almost(set2)) { 
                        send(set);
                        saved = msg;
                    }
                    else
                        multi.set(msg, supID++, set2);
                }
                catch (InterruptedException ex) { thread.interrupt(); return; }
            }
        }
        public boolean almost(MessageSet set) { return set.size > set.almost; }
        public OratorUtils.Entry<MessageSet> getSet(Message msg,boolean live) {
            Nest.Key key = msg.nest.key();
            OratorUtils.Entry<MessageSet> set = setMap.get( key );
            if (set == null) {
                MessageSet nms = new MessageSet().set( orator, Partner2.header, key );
                if (live) multi.msgset = nms;
                set = setq.push( nms );
                setMap.put( key, set );
            }
            return set;
        }

    }

    public static void cmpout(java.util.Collection map, Object key) {
        StringBuilder txt = new StringBuilder();
        txt.append( String.format( "cmp searching for key %s\n", key ) );
        for (Object kk : map) {
            txt.append( String.format( "cmp: %5b -- %s\n", key.equals( kk ), kk ) );
        }
        System.out.println( txt );
    }
    public static void cmpout(java.util.Map map, Object key) {
        StringBuilder txt = new StringBuilder();
        txt.append( String.format( "cmp searching for key %s", key ) );
        for (Object kk : map.keySet()) {
            txt.append( String.format( "cmp: %5b -- %s", key.equals( kk ), kk ) );
        }
        System.out.println( txt );
    }
    public void cmp(ConcurrentHashMap map, Object key) {
        logger.log( "cmp searching for key %s", key );
        for (Object kk : map.keySet()) {
            logger.log( "cmp: %5b -- %s", key.equals( kk ), kk );
        }
    }

    /** uniquify the remote orator address */
    public Remote remotify(InetAddress inet,int port) {
        Remote.Key key = new Remote.Key();
        key.set( inet, port );
        Remote remote = new Remote();
        Remote ret = remoteMap.putIfAbsent( key, remote );
        return (ret==null) ? remote.set( key.inet, key.port ) : ret;
    }

    /** return (create if needed) the unique nest associated with the couple */
    public Nest nestify(Kelly kelly,Remote roa) {
        Nest next = new Nest().set( kelly, roa );
        Nest ret = nestMap.putIfAbsent( next.key(), next );
        if (ret==null)
            logger.log( "nest: %d -> %s", kelly.id, roa.txtInfo());
        return (ret==null) ? next : ret;
    }

    public Nest nestify(Kelly kelly,InetAddress inet,int port) {
        return nestify( kelly, remotify( inet, port ) );
    }



    public Nest kellyify(int upstreamKID,boolean upstream,Remote roa) {
        boolean dump = false;
        Kelly next = new Kelly().keyify( upstreamKID, upstream, roa );
        if (dump) cmp( kellyMap, next.key );
        Kelly kelly = kellyMap.putIfAbsent( next.key, next );
        Nest nest;
        if (kelly == null) {
            kelly = next;
            nest = nestify( kelly, roa );
            kelly.init( this, nest, logger );
        } else nest = nestify( kelly, roa );
        return nest;
    }

    public void doAck(Kiss2 kiss) {
        logger.log( "ack,%d: cleaning id:%d, from %s", kiss.nest.kelly.id, kiss.id, kiss.nest.roa );
        PacketRecord pr = send.sentMap.remove( kiss );
        if (pr != null) pr.msg = null;
    }

    public void ack(Nest nest,int ackID,int oldest) {
        boolean dump = false;
        Kiss2 kiss = new Kiss2().set( nest, ackID );
        if (dump) cmp( send.sentMap, kiss );
        doAck( kiss );
        if (ackID == nest.oldestSent+1) nest.oldestSent++;
        for (int ii = nest.oldestSent+1; ii <= oldest; ii++) {
            nest.oldestSent = ii;
            kiss.set( nest, ii );
            doAck( kiss );
        }
    }

    public class RecvLoop extends Loop {
        public byte[] data;
        public ByteBuffer buffer;
        public DatagramPacket packet;
        public OratorUtils.MultiMap multiMap = new OratorUtils.MultiMap();

        public RecvLoop init() {
            data = new byte[ 1 << 15 ];
            buffer = ByteBuffer.wrap( data );
            packet = new DatagramPacket( data, data.length );
            roleName = "recv";
            return this;
        }
        public void run() {
            while (! thread.isInterrupted()) {
                try {
                    socket.receive( packet );
                    buffer.clear();
                    buffer.limit( packet.getLength() );
                    Partner2 partner = new Partner2().read( packet, buffer );
                    if (rand.nextDouble() < fakeThresh ) {
                        log( "received,up(%b,%d): faking dropped packet from %s/%d/%d",
                                partner.upstream, partner.upstreamKID,
                                packet.getAddress(), packet.getPort(), partner.packetID );
                        continue;
                    }
                    Remote roa = remotify( packet.getAddress(), packet.getPort() );
                    Nest nest = kellyify( partner.upstreamKID, partner.upstream, roa );
                    handle( buffer, nest, partner );
                }
                catch (Exception ex) { throw new RuntimeException( ex ); }
            } 
        }
        public void handle(ByteBuffer buf,Nest nest,Partner2 partner) {
            boolean fresh = ! partner.reqAck || isFresh( nest, partner );
            log( "received,%d: message(%d,%b): %s", nest.kelly.id, partner.packetID, fresh, nest.roa.txtInfo() );
            handle( buf, nest, fresh );
            if (partner.reqAck) {
                Message.Ack ack = new Message.Ack().set( partner.packetID, nest.oldest );
                ack.nest = nest;
                send.sendq.offer( ack );
            }
        }
        public void handle(ByteBuffer buf,Nest nest,boolean fresh) {
            int len = buf.limit();
            int type = buf.getInt();
            Message msg = mutils.alloc( type );
            log( "received,%d: message: %d bytes, cmd %d <-- %s", nest.kelly.id, len, type, msg.getClass() );
            msg.nest = nest;
            if (fresh)
                msg.handle( orator, buf, len, false );
        }
    }

    public boolean isFresh(Nest nest,Partner2 partner) {
        // fixme::robustness -- use TreeMap.subMap to select the per-nest packetIDs ...
        int pid = partner.packetID;
        if ( pid <= nest.oldest ) return false;
        Kiss2 key = new Kiss2().set( nest, pid );
        if ( pid == nest.oldest+1 ) {
            Boolean val = Boolean.TRUE;
            while ( val != null ) {
                nest.oldest = key.id++;
                val = send.recvMap.remove( key );
            }
            return true;
        }
        else return send.recvMap.put( key, Boolean.TRUE )==null;
    }


    
    public class UserLoop extends Loop {
        public UserLoop init() {
            roleName = "user";
            return this;
        }
        public void execTask(Message.User user) {
            user.get( orator );
            log( "message,%d: %s", user.nest.kelly.id, user );
            user.exec( orator, this );
        }
        public void run() {
            kryos.set(kryo);
            while (! thread.isInterrupted()) {
                try {
                    org.srlutils.Simple.sleep( 100 ); // fixme -- don't think this is needed anymore ...
                    Message.User user;
                    if ( ( user = boss.delegateq.take() ) != null )
                        execTask( user );
                }
                catch (Exception ex) { ex.printStackTrace(); }
            }
        }
    }

    public class BossLoop extends Loop {
        public BlockingQueue<Message.User> delegateq;

        public DynArray.ints need = new DynArray.ints(); // kryo classIDs that are undefined, ie need to be loaded
        public byte [] data = new byte[ 1 << 16 ];
        public Output bb = new Output(data);

        public Lock lock;
        public LockingQ.LockSet lockset;
        public LockingQ<Message.User> userq;
        public LockingQ<Message.Admin> defineq;

        public BossLoop init() {
            delegateq = new LinkedBlockingQueue();

            lock = new ReentrantLock();
            lockset = new LockingQ.LockSet( lock );
            userq = new LockingQ( new LinkedList(), lockset );
            defineq = new LockingQ( new LinkedList(), lockset );
            roleName = "boss";
            return this;
        }
        /** boss loop only */
        public void checkIDs(Message.User user,DynArray.ints needs) {
            check();
            Kelly kelly = user.nest.kelly;
            boolean ready = user.nest.kelly.checkIDs( user, needs );
            if (ready) delegateq.offer( user );
            else kelly.deferred.push( user );
            if (needs != null && needs.size > 0) {
                user.nest.kelly.assertUpstream(false);
                int [] ids = org.srlutils.Util.dup( needs.vo, 0, needs.size );
                needs.size = 0;
                Message rids = new Admin.ReqIDs().set( ids ).wrap( bb, kryo ); // boss down
                rids.nest = user.nest;
                try { send.sendq.put( rids ); }
                catch (InterruptedException ex) {
                    throw new RuntimeException( "sendq full ... aborting", ex );
                }
            }
        }
        /** wait for the queues to go notEmpty */
        public void waitNotEmpty() throws InterruptedException {
             lock.lock();
             try {
                 while ( userq.que.peek() == null && defineq.que.peek() == null )
                     lockset.notEmpty.await();
             }
             finally { lock.unlock(); }
        }
        /** boss loop only */
        public void retry(Kelly kelly) {
            OratorUtils.Entry<User> head = kelly.deferred.clear();
            for ( ; head != null; head = head.next) checkIDs( head.obj, need );
        }
        public void run() {
            kryos.set(kryo);
            while (! thread.isInterrupted()) {
                try {
                    waitNotEmpty();
                    Message.User user;
                    Message.Admin admin;
                    need.size = 0;
                    while ( ( user = userq.poll() ) != null ) {
                        user.getIDs( orator );
                        checkIDs( user, need );
                    }
                    while ( ( admin = defineq.poll() ) != null ) {
                        admin.get( orator );
                        admin.payload.nest( admin.nest );
                        log( "admin -- %s", admin.payload.txt() );
                        admin.payload.admin( orator );
                    }
                }
                catch (InterruptedException ex) { thread.interrupt(); return; }
                catch (Exception ex) { ex.printStackTrace(); }
            }
        }
    }

    public static class Scoper {
        public Orator orator;
        public Message.TaskBase msg;
        public Logable logger;

        public Scoper set(Orator orator,Message.TaskBase msg,Logable logger) {
            this.orator = orator;
            this.msg = msg;
            this.logger = logger;
            return this;
        }
        
    }

    public interface Callback<TT> {
        // todo -- probably want to add in the message that resulted in the reply to the args ...
        public abstract void callback(TT obj,Scoper xx);

        /** a callback for synchronous replies - can block on mon until the task reply is received */
        public static class Sync implements Callback<Object> {
            public final Object mon = new Object();
            public boolean done;
            public void callback(Object obj, Scoper xx) {
                synchronized(mon) { done = true; mon.notify(); }
            }
        }
    }

    public interface Adminable {
        /** run the administrative payload - only useable from boss loop */
        public void admin(Orator orator);
        public void nest(Nest nest);
        public String txt();
    }
    public interface Taskable<TT> {
        public abstract TT task(Scoper xx);

        public static abstract class Simple implements Taskable<Void> {
            public Void task(Scoper xx) { task(); return null; }
            public abstract void task();
        }
    }





    public void sendTask2(Taskable taskable,Nest dest,Output buf,Object dummy) {
        Callback.Sync cbs = new Callback.Sync();
        long time = System.currentTimeMillis();
        sendTask( taskable, dest, buf, cbs );
        synchronized (cbs.mon) {
            while( ! cbs.done ) {
                try { cbs.mon.wait(); }
                catch (InterruptedException ex) { throw new RuntimeException( "waitTask interrupted ...", ex ); }
            }
        }
        long delta = System.currentTimeMillis() - time;
        System.out.format( "waitTask completed ...%d ms\n", delta );
    }

    public <TT> void sendTask(Taskable<TT> taskable,Nest dest,Output buf, Callback<TT> cb) {
        Message.Task msg = new Message.Task();
        msg.set( taskable, buf, kryo2() ); // main up
        msg.nest = dest;
        if (cb != null) msg.setCallback( this, cb );
        send.sendq.offer( msg );
    }
    public void sendObj(Object obj,Nest dest,Output buf) {
        Message msg = new Message.Printer().set( obj, buf, kryo2() ); // main up
        msg.nest = dest;
        send.sendq.offer( msg );
    }








    public static class MessageUtils {
        public DynArray.Objects<Class<Message>> list = new DynArray.Objects().init( Class.class );
        public HashMap<Class<? extends Message>,Integer> map = new HashMap();

        public Message alloc(int type) {
            try { return list.get( type ).newInstance(); }
            catch (Exception ex) { throw new RuntimeException( "Unable to create Message", ex ); }
        }
        public void add(Class<Message> klass) { int typeID = list.add( klass ); map.put( klass, typeID ); }
        public Message prep(Message msg) { msg.type = map.get( msg.getClass() ); return msg; }
        public MessageUtils init(Class [] array) { for (Class klass : array) add( klass ); return this; }
    }

    public static class NetClass {
        public String name;
        public int classID;
        public byte [] code;

        public NetClass set(Class klass) { name = klass.getName(); return this; }
    }

    public byte [] read(String name,NetLoader loader) {
        boss.check();
        name = name.replace( '.', '/' ) + ".class";
        logger.log( "Orator::read -- %s", name );
        if (loader != null)
            return loader.getBytecode( name );
        else {
            ClassLoader cl = getClass().getClassLoader();
            InputStream in = cl.getResourceAsStream( name );
            DynArray.bytes bytes = org.srlutils.Files.readStream( in );
            return bytes.trim();
        }
    }


    public static class MyTaskRet implements Taskable<String> {

        public String task(Scoper xx) {
            return String.format( "%s --> %s, msgID:%d", this, xx.orator.addr, xx.msg.id );
        }
    }
    public static class MyObj extends Taskable.Simple {
        public String dummy = "yes we can";
        public String blah() { return "no"; }
        public void task() {
            System.out.format( "%s --> %s --> %s, %s\n", this, dummy, blah(), this.getClass().getClassLoader() );
        }
    }
    public static class MyObj2 extends MyObj {
        public byte [] data = new byte[ 1000 ];
        { data[ 7 ] = 98; }
        public String blah() { return "" + data[7]; }
    }

    public static class Virtual2 implements Runnable {
        Orator net = new Orator();
        public void run() {
            try {
                net.init( 0 );
                net.start();
                Nest nest = net.nestify( net.root, null, Orator.defaultPort );
                Output buf = new Output(new byte[2048]);
                Taskable m1 = new MyObj(), m2 = new MyObj2();
                net.sendTask( m1, nest, buf , null);
//                net.sendTask( m2, nest, buf , null);
            } catch (Exception ex) {
                throw new RuntimeException( "Virtual failed ...", ex );
            }
        }
        public Virtual2 init(Virus virus) {
            net.setLog( virus.sbuf );
            net.seed = virus.seed++;
            net.virus = virus;
            return this;
        }
    }

    public static class V3C implements Callback<String> {
        public void callback(String obj, Scoper xx) {
            System.out.format( "roundtrip completed -- hello world ... %s\n", obj );
        }
    }

    public static class Virtual3 implements Runnable {
        public Class Dummy;
        Orator net = new Orator();
        public void run() {
            try {
                net.init( 0 );
                net.start();
                for (int ii = 0; ii < 40; ii++) {
                    Nest nest = net.nestify( net.root, null, Orator.defaultPort );
                    Output buf = new Output(new byte[2048]);
                    net.sendTask( (Taskable) Dummy.newInstance(), nest, buf , new V3C() );
                }
            } catch (Exception ex) {
                throw new RuntimeException( "Virtual failed ...", ex );
            }
        }
        public void init(Virus virus) {
            net.setLog( virus.sbuf );
            net.seed = virus.seed++;
            net.virus = virus;
            new Thread( this ).start();
        }
        public Virtual3() throws ClassNotFoundException {
            Dummy = this.getClass().getClassLoader().loadClass( "loadee.Obama$Dummy" );
        }
        public Virtual3(boolean val) {
            Dummy = MyObj.class;
        }
    }

    public static class LogLoop extends Thread {
        public Virus virus;
        public LogLoop set(Virus virus) { this.virus = virus; return this; }
        public void run() {
            while (true) {
                org.srlutils.Simple.sleep( 15000 );
                dumpLog( virus.sbuf );
                System.out.format( "dump::count %d, %d\n", virus.count.get(), virus.resend );
            }
        }
    }


    public static class Virus {
        public StringBuffer sbuf;
        public long seed;
        public AtomicInteger count = new AtomicInteger( 0 );
        public boolean dbg = false;
        public int resend;
    }

    public Virus virus = new Virus();

    public static ClassLoader makeLoader(ClassLoader ucl,Virus virus) throws Exception {
        Loader.Proxy loader = new Loader.Proxy( ucl );
        loader.skip( Virus.class );
        loader.loadClass( "com.nqzero.orator.Orator$Virus" );
        Class klass = loader.loadClass( Virtual3.class.getName() );
        Object obj = klass.newInstance();
        org.srlutils.Simple.Reflect.invoke( obj, "init", virus );
        return loader;
    }

    public static void main(String ... args) throws Exception {


        boolean both = args.length == 0;
        boolean onlyNet = args.length > 0 && args[0].equals( "net" );
        Virus virus = new Virus();
//        virus.dbg = false;
        virus.sbuf = new StringBuffer();
        virus.seed = new Random().nextLong() % (1 << 8);
//        virus.seed = 23;
        System.out.format( "virus::seed %d\n", virus.seed );
        int nn = 25;

        Orator host = null;
        if ( both || !onlyNet ) {
            host = new Orator();
            host.seed = virus.seed++;
            host.virus = virus;
            host.setLog( virus.sbuf ).init( defaultPort );
            host.start();
        }
        if ( both || onlyNet ) {

            URL [] urls = new URL[] { new URL( "file:loadee/target/classes/" ) };
            ClassLoader ucl = new URLClassLoader(urls,Orator.class.getClassLoader());

            if (false) {
                Virtual2 v2 = new Virtual2().init( virus );
                new Thread( v2 ).start();
            }
            else if (false) {
                Virtual3 v3 = new Virtual3(true);
                v3.init( virus );
//                v3.run();
            } else {
                for (int ii = 0; ii < nn; ii++) makeLoader( ucl, virus );
            }
        }
        new LogLoop().set( virus ).start();
    }
    // fixme - set to zero for production, move external for testing
    public static double fakeThresh = 0.0;
}



















