package com.nqzero.orator;

import com.esotericsoftware.kryo.io.Output;
import com.nqzero.orator.Admin.ReplyCode;
import com.nqzero.orator.Example.MyKryo;
import com.nqzero.orator.Loader.Oratorable;
import com.nqzero.orator.Message.*;
import com.nqzero.orator.OratorUtils.Taskable;
import com.nqzero.orator.OratorUtils.NetClass;
import com.nqzero.orator.OratorUtils.Logable;
import com.nqzero.orator.OratorUtils.Entry;
import com.nqzero.orator.OratorUtils.Kelly;
import com.nqzero.orator.OratorUtils.Kiss2;
import com.nqzero.orator.OratorUtils.MessageSet;
import com.nqzero.orator.OratorUtils.MultiMap;
import com.nqzero.orator.OratorUtils.NakedList;
import com.nqzero.orator.OratorUtils.Nest;
import com.nqzero.orator.OratorUtils.Partner2;
import com.nqzero.orator.OratorUtils.Remote;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import kilim.Mailbox;
import kilim.Pausable;
import kilim.Scheduler;
import org.srlutils.DynArray;
import org.srlutils.Simple;

public class Orator implements Oratorable {
    public DatagramSocket socket;
    public Message.MessageUtils mutils = new Message.MessageUtils().init( Message.classList );
    Mailbox<Message> outbox = new Mailbox();
    Mailbox<Message.Memo> memos = new Mailbox();
    public ConcurrentHashMap<Remote.Key,Remote> remoteMap = new ConcurrentHashMap();
    public ConcurrentHashMap<Nest.Key,Nest> nestMap = new ConcurrentHashMap();
    public ConcurrentHashMap<Kelly.Key,Kelly> kellyMap = new ConcurrentHashMap();
    public ConcurrentHashMap<Kiss2,Mailbox> sentMap = new ConcurrentHashMap();
    public ConcurrentHashMap<Kiss2,Mailbox> boxMap = new ConcurrentHashMap();
    public MultiMap multiMap = new MultiMap();
    Scheduler bossSched = Scheduler.make(1);
    Scheduler sched = Scheduler.getDefaultScheduler(); // fixme - use fork join ???
    public Example.MyKryo kryo2 = new Example.MyKryo().init();
    { for (Class klass : Admin.classList) kryo2.register( klass ); }
    public int jitter = 100;
    public int retryTo = 2*jitter + 100;
    public ByteBuffer sendBuffer;
    public byte [] data = new byte[ 2*1024 ];
    DatagramPacket packetOut = new DatagramPacket( data, 0 );
    public long seed;
    public Random rand;
    public static double fakeThresh = 0.0;
    public Kelly root;
    public int kellyID = 0;

    public Logable logger() { return logger; }
    public void sendMessage(Message msg) throws InterruptedException {
        outbox.putb(msg);
    }
    public void assertBoss() {}

    static NullLogger logger = new NullLogger();
    static class NullLogger implements Logable {
        static boolean dbg = false;
        int max = 1000;
        AtomicInteger num = new AtomicInteger();
        AtomicInteger read = new AtomicInteger();
        String [] text = new String[max];
        public void log(String fmt,Object... args) {
            if (! dbg) return;
            int line = num.getAndIncrement();
            if (line < max)
                text[line] = args.length==0 ? fmt : String.format(fmt,args);
        }
        void print() {
            int k1 = read.get();
            int k2 = num.get();
            for (int ii=k1; ii < k2; text[ii++]=null)
                System.out.println(text[ii]);
            read.set(k2);
        }
    }
    Log log = new Log();
    public class Log extends kilim.Task {
        public Log() {
            setScheduler(bossSched);
        }
        public void execute() throws Pausable {
            while (true) {
                kilim.Task.sleep(3000);
                logger.print();
            }
        }
    }
    { if (NullLogger.dbg) log.start(); }

    public void init(int port) {
        try {
            root = new Kelly().keyify(0,true,null);
            kellyMap.put( root.key, root );
            root.kryo = kryo2;
            Remote remote = new Remote().set(null,port);
            InetSocketAddress addr = new InetSocketAddress(remote.inet, port);
            seed = seed==0 ? new Random().nextLong() : seed;
            rand = new Random( seed );
            sendBuffer = ByteBuffer.wrap( data );
            socket = new DatagramSocket(addr);
            recv.start();
            boss.start();
            userAdmin.start();
            replyActor.start();
            sender.start();
        }
        catch (Exception ex) {}
    }

    
    RecvLoop recv = new RecvLoop().init();
    public class RecvLoop implements Runnable {
        public byte[] data;
        public ByteBuffer buffer;
        public DatagramPacket packet;
        public HashMap<Kiss2,Boolean> recvMap = new HashMap();

        public Thread thread;
        public String roleName;
        public void start() {
            String loc = socket.getLocalPort() + ":" + roleName;
            thread = new Thread( this, loc );
            thread.start();
        }
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
                    if (fakeThresh > 0 & fakeThresh > rand.nextDouble())
                        continue;
                    Remote roa = remotify( packet.getAddress(), packet.getPort() );
                    Nest nest = kellyify( partner.upstreamKID, partner.upstream, roa );
                    handle( buffer, nest, partner );
                }
                catch (Exception ex) {
                    throw new RuntimeException( ex );
                }
            } 
        }
        public void handle(ByteBuffer buf,Nest nest,Partner2 partner) {
            boolean fresh = ! partner.reqAck || isFresh(nest,partner);
            if (partner.reqAck) {
                // fixme - user messages should putnb() and don't send ack on failure
                Message.Ack ack = new Message.Ack().set(partner.packetID,nest.oldest);
                ack.nest = nest;
                outbox.putb(ack);
            }
            Orator.this.handle(buf,nest,fresh);
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
                    val = recvMap.remove( key );
                }
                return true;
            }
            else return recvMap.put( key, Boolean.TRUE )==null;
        }
    }
    
    public void handle(ByteBuffer buf,Nest nest,boolean fresh) {
        int len = buf.limit();
        int type = buf.getInt();
        if (fresh)
            handle(buf,nest,len,type);
    }
    public void handle(ByteBuffer buf,Nest nest,int len,int type) {
        Message msg = mutils.alloc( type );
        msg.nest = nest;
        msg.handle(this, buf, len, false );
    }
    public void ack(Nest nest,int ackID,int oldest) {
        Kiss2 kiss = new Kiss2().set(nest,ackID);
        Mailbox pr = sentMap.remove( kiss );
        if (pr != null) {
            pr.putnb(kiss);
        }
    }
    // sched loop can't load classes cause they could block waiting on code request, which needs the sched loop
    // to handle admin, add to codeq, start a new task in the kelly space that loads classes, then messages sched
    // sched loop then registers the ids with kryo and retries
    Scheduler userSched = Scheduler.make(1);
    Output userBuf = new Output(new byte[1<<16]);
    public void checkIDs(Entry<MetaActor> entry) {
        Message.Task user = entry.obj.task;
        // can either record a list of all deferred tasks and retry them all when a netclass is resolved
        // or for each deferred netclass, keep a list of all dependent tasks (mailboxen)
        //    and message each one when resolved
        // if there's only a few deferred tasks then nothing matters
        // if there are only a few classes then they're more or less equiv
        // if there are many tasks each with distinct classes, then the mailboxen win
        // if there are many tasks with the same netclasses then retry wins
        Kelly kelly = user.nest.kelly;
        boolean ready = kelly.checkIDs(user,null);
        if (ready)
            entry.obj.resume();
        else
            kelly.deferred.push(entry);
    }
    public DynArray.ints needs = new DynArray.ints();
    public void retry(Kelly kelly) {
        OratorUtils.Entry<MetaActor> head = kelly.deferred.clear();
        for (; head != null; head=head.next) checkIDs(head);
    }
    public static MetaActor meta() throws Pausable {
        return (MetaActor) kilim.Task.getCurrentTask();
    }
    public static void resume(Scheduler scheduler) throws Pausable {
        kilim.Task.getCurrentTask().resumeOnScheduler(scheduler);
    }
    static kilim.PauseReason always = t -> true;
    public <TT> void handleTask(Task<TT> task) throws Pausable {
        resume(userSched);
        OratorUtils.assertBoss(2);
        task.getIDs(task.nest.kelly.kryo);
        Kelly kelly = task.nest.kelly;
        needs.size = 0;
        boolean ready = kelly.checkIDs(task,needs);
        if (needs.size > 0) {
            Message rids = Admin.ReqIDs.make(task.nest,needs);
            outbox.put(rids);
        }
        if (! ready) {
            kelly.deferred.push(meta());
            kilim.Task.pause(always);
        }
        task.get(kelly.kryo);
        resume(sched);
        TT obj = task.payload.task();
        if (task.msgID != 0) {
            TaskReply reply = task.reply(obj);
            resume(userSched);
            OratorUtils.assertBoss(2);
            reply.set(userBuf,kelly.kryo);
            outbox.put(reply);
        }
    }
    public void handleReply(TaskReply reply) throws Pausable {
        MyKryo my = reply.nest.kelly.kryo;
        Simple.softAssert(my==kryo2,"replies should apply to root");
        Kiss2 kiss = new Kiss2().set(reply.nest,reply.msgID);
        Mailbox box = boxMap.remove(kiss);
        if (box==null)
            return;
        OratorUtils.assertBoss(1);
        reply.get(my);
        box.put(reply.payload);
        NetClass [] klasses = my.getNamedClasses();
        if (klasses==null || klasses.length==0)
            return;
        logger().log("reply.klasses:");
        for (NetClass nc : klasses)
            logger().log("  " + nc.name + ": " + nc.classID);
        Memo msg = Admin.Reply.make(reply.nest,klasses);
        outbox.put(msg);
    }

    UserAdmin userAdmin = new UserAdmin();
    public class UserAdmin extends kilim.Task {
        Mailbox<Admin.Reply> tasks = new Mailbox();
        public UserAdmin() {
            setScheduler(userSched);
        }
        public void execute() throws Pausable {
            OratorUtils.threadID.set(2);
            while (true) {
                Admin.Reply task = tasks.get();
                task.doAdmin(Orator.this);
            }
        }
    }
    public class MetaActor<TT> extends kilim.Task {
        Message.Task<TT> task;
        public MetaActor(Message.Task<TT> task) {
            this.task = task;
            setScheduler(userSched);
        }
        public void execute() throws Pausable {
            handleTask(task);
        }
    }
    ReplyActor replyActor = new ReplyActor();
    public class ReplyActor extends kilim.Task {
        Mailbox<TaskReply> tasks = new Mailbox();
        public ReplyActor() {
            setScheduler(bossSched);
        }
        public void execute() throws Pausable {
            while (true) {
                TaskReply task = tasks.get();
                handleReply(task);
            }
        }
    }

    public void handleCode(ReplyCode code) {
        code.nest.kelly.loader.codeq.offer(code);
        new LoadRunner(code).start();
    }
    public class LoadRunner extends kilim.Task {
        ReplyCode code;
        public LoadRunner(ReplyCode code) {
            this.code = code;
            setScheduler(sched);
        }
        public void execute() throws Pausable {
            try {
            for (NetClass nc : code.klasses)
                if (nc != null) code.nest.kelly.loader.loadClass(nc.name);
            }
            catch (ClassNotFoundException ex) {} // fixme - should never happen but ... (need logging i guess)
            Orator.resume(userSched);
            for (NetClass nc : code.klasses)
                if (nc != null) code.nest.kelly.addCode(nc);
            retry(code.nest.kelly);
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
        return (ret==null) ? next : ret;
    }
    public Nest kellyify(int upstreamKID,boolean upstream,Remote roa) {
        boolean dump = false;
        Kelly next = new Kelly().keyify( upstreamKID, upstream, roa );
        Kelly kelly = kellyMap.putIfAbsent( next.key, next );
        Nest nest;
        if (kelly == null) {
            kelly = next;
            nest = nestify( kelly, roa );
            kelly.init(kellyID++, this, nest);
        } else nest = nestify( kelly, roa );
        return nest;
    }
    public class PacketSender extends kilim.Task {
        Message msg;
        int pid;

        public PacketSender(Message msg,int pid) {
            this.msg = msg;
            this.pid = pid;
            setScheduler(bossSched);
        }
        
        public void execute() throws Pausable,Exception {
            Mailbox box = new Mailbox();
            Kiss2 kiss = new Kiss2().set(msg.nest,pid);
            sentMap.put(kiss,box);
            send(msg,pid);
            // fixme::robustness - should route retries through a single entity 
            //                       to allow prioritizing admin messages and acks
            while (box.get(retryTo)==null)
                send(msg,pid);
        }
    }
    public void send(Message msg,int pid) {
        Remote roa = msg.nest.roa;
        Partner2 partner = new Partner2().set( msg.nest, 0, msg.reqAck, pid );
        sendBuffer.clear();
        partner.write( sendBuffer );
        msg.write( sendBuffer );
        sendBuffer.flip();
        int len = sendBuffer.limit();
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
    AtomicInteger nextMsgId = new AtomicInteger(1);
    public <TT> Mailbox<TT> sendTask(Taskable<TT> taskable,Nest dest) {
        Message.Task msg = new Message.Task();
        msg.msgID = nextMsgId.getAndIncrement();
        msg.set(taskable);
        msg.nest = dest;
        Mailbox<TT> box = new Mailbox();
        Kiss2 kiss = new Kiss2().set(msg.nest,msg.msgID);
        Mailbox pre = boxMap.put(kiss,box);
        assert(pre==null);
        outbox.putb(msg);
        return box;
    }


    Boss boss = new Boss();
    public class Boss extends kilim.Task {
        public Boss() {
            setScheduler(bossSched);
        }
        public byte [] data = new byte[ 1 << 16 ];
        public Output scratch = new Output(data);
        public void execute() throws Pausable {
            OratorUtils.threadID.set(1);
            while (true) {
                Memo memo = memos.get();
                memo.get(kryo2);
                memo.payload.nest(memo.nest);
                memo.payload.admin(Orator.this);
            }
        }
    }
    
    Sender sender = new Sender();
    public class Sender extends kilim.Task {
        Sender() {
            setScheduler(bossSched);
        }
        public NakedList<MessageSet> setq = new NakedList();
        public HashMap<Nest.Key,Entry<MessageSet>> setMap = new HashMap();
        OratorUtils.Multi splitter = new OratorUtils.Multi();
        public int packetID = 0;
        public int supID = 0;
        public int msgID = 1;
        public void execute() throws Pausable {
            Entry<MessageSet> first, entry;
            Message saved = null;
            while (true) {
                Message msg = saved;
                saved = null;
                entry = null;
                long current = System.currentTimeMillis();

                if (msg==null && splitter.live) {
                    entry = getMessy(splitter.msg);
                    msg = splitter.get();
                }
                else if (msg == null) {
                    if (setq.empty())
                        msg = outbox.get();
                    else {
                        first = setq.peek();
                        msg = outbox.getnb();
                        if (msg==null) { send( first ); continue; }
                    }
                }
                
                if (msg instanceof Flat && ((Flat) msg).data==null)
                    ((Flat) msg).set(boss.scratch,kryo2);
                if (entry == null) entry = getMessy(msg);

                MessageSet set = entry.obj;

                mutils.prep( msg );
                if (set.fits(msg)) {
                    set.add( msg );
                    // fixme:opt - never wait to send multi sets, setq.move or another list
                    if (splitter.live && almost(set) || !set.fits(32))
                        send(entry);
                }
                else if (almost(set)) { 
                    send(entry);
                    saved = msg;
                }
                else
                    splitter.set(msg, supID++, set);
            }
        }
        public void send(Entry<MessageSet> entry) {
            splitter.msgset = null;
            setq.remove( entry );
            setMap.remove( entry.obj.key );
            Message msg = entry.obj.toMessage();
            if (msg.reqAck)
                new PacketSender(msg,msg.nest.nextPacketID++).start();
            else
                Orator.this.send(msg,0);
        }
        public boolean almost(MessageSet set) { return set.size > set.almost; }

        public Entry<MessageSet> getMessy(Message msg) {
            Nest.Key key = msg.nest.key();
            Entry<MessageSet> set = setMap.get( key );
            if (set == null) {
                MessageSet nms = new MessageSet().set(mutils, Partner2.header, key);
                if (splitter.live) splitter.msgset = nms;
                set = setq.push( nms );
                setMap.put(key, set );
            }
            return set;
        }

    }
    /*
        glossary

        Multi: breaks a single message up into multiple sup/sub portions
        MessageSet: a list of messages that are concatenated into a single Composite message (or the message if solo)
        Remote: inet address + port tuple
        Kelly: kryo wrapper, upstream + id
        Nest: kelly + remote tuple
    */


    public static void main(String [] args) throws Exception {
        if (kilim.tools.Kilim.trampoline(false,args))
            return;

        int port = DemoOrator.defaultPort;
        new Orator().init(port);
    }
}
/*





multicast discovery:
https://stackoverflow.com/questions/3258959/network-discovery-in-java-using-multicasting
https://gist.github.com/kilaka/5799664






*/