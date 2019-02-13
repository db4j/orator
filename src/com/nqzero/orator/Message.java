// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.nqzero.orator.OratorUtils.Taskable;
import com.nqzero.orator.OratorUtils.SupidKey;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.srlutils.DynArray;

public abstract class Message {

    public static Class [] classList = new Class [] {
        User.class, Memo.class, Sup.class, Sub.class, Composite.class,
        Ack.class, TaskReply.class, Task.class
    };
    public static int tracking = 0;
    
    { header = 4*1; }
    int header, type, id, track;
    public int core, size;
    public OratorUtils.Nest nest;
    public boolean reqAck = true;
    public void write(ByteBuffer buf) {
        buf.putInt( type );
    }
    public abstract void handle(Orator orator,ByteBuffer buf,int limit,boolean copy);


    public Message() {
        track = tracking++;
    }
    public String txt() { return toString(); }
    

    public static abstract class Flat<TT,UU> extends Message {
        { header += 4*1; }
        public byte [] data;
        public int idOffset, mark;
        public int [] classIDs;

        /** the object corresponding to data, which is extracted for reads,
         **    but also convenient for debugging writes
         **/
        public UU payload;

        /** only valid for get operations */
        public ByteBuffer buffer;
        public Input input;
        public Input buffer() {
            input.setPosition(buffer.position());
            input.setLimit(buffer.limit());
            return input;
        }

        /** create the message by serializing obj, using kryo and the scratch pad */
        public TT set(UU obj) {
            payload = obj;
            return (TT) this;
        }

        /** create the message by serializing obj, using kryo and the scratch pad */
        public TT set(Output scratch,Example.MyKryo kryo) {
            scratch.clear();
            int idpos = kryo.put( scratch, payload, true );
            byte [] data2 = scratch.toBytes();
            return set( data2, idpos );
        }
        public TT set(byte[] data, int idOffset) {
            this.data = data;
            this.idOffset = idOffset;
            core = data.length;
            size = core + header;
            return (TT) this;
        }
        public void writeTT(ByteBuffer buf) {}
        public void write(ByteBuffer buf) {
            super.write( buf );
            buf.putInt( idOffset );
            writeTT( buf );
            buf.put( data );
        }
        public void read(ByteBuffer buf) {}
        public void handle(Orator orator,ByteBuffer buf,int limit,boolean copy) {
            prep(buf,limit,copy);
        }
        public void prep(ByteBuffer buf,int limit,boolean copy) {
            idOffset = buf.getInt();
            read( buf );
            if (!copy) {
                data = new byte[ limit - buf.position() ];
                int lo = buf.limit();
                buf.limit( limit );
                buffer = ByteBuffer.wrap( data );
                input = new Input(data);
                buffer.put( buf );
                buffer.flip();
                buf.limit( lo );
            }
            else {
                buffer = buf;
                throw new UnsupportedOperationException("direct copy hasn't been ported to kryo v2");
            }
            mark = buffer.position();
        }
        public void getIDs(Example.MyKryo kryo) {
            classIDs = kryo.getIDs(buffer(), mark + idOffset);
        }
        public String txt() {
            return "Flat:" + (payload==null ? "null" : payload.toString());
        }
    }

    public abstract static class User<UU> extends Flat<User,UU> {
        { header += 4; }
        /** id used to identify a reply message, non-zero --> send a reply */
        public int msgID;

        public void get(Example.MyKryo kryo) {
            buffer.position( mark );
            buffer.limit( mark + idOffset );
            payload = (UU) kryo.get( buffer() );
        }
        public void read(ByteBuffer buf) { super.read( buf ); msgID = buf.getInt(); }
        public void writeTT(ByteBuffer buf) { super.writeTT( buf ); buf.putInt( msgID ); }

        public TaskReply reply(Object obj) {
            // assert user
            nest.kelly.assertUpstream(false);
            Message.TaskReply rep = new Message.TaskReply();
            rep.set(obj);
            rep.nest = nest;
            rep.msgID = msgID;
            return rep;
        }
    }

    public static class Task<TT> extends User<Taskable<TT>> {
        { header += 0; }

        public void handle(Orator orator,ByteBuffer buf,int limit,boolean copy) {
            super.handle(orator, buf, limit, copy);
            orator.new MetaActor(this).start();
            // fixme - need to send backpressure to client, tell them task has been rejected
        }
    }
    // created on downstream, sent to upstream
    public static class TaskReply extends User {
        { header += 0; }

        public void handle(Orator orator,ByteBuffer buf,int limit,boolean copy) {
            super.handle( orator, buf, limit, copy );
            boolean ok = orator.replyActor.tasks.putnb(this);
            // fixme - need to send backpressure to client, tell them task has been rejected
        }
    }

    public static class Memo extends Flat<Memo,Admin> {
        { header += 0; }

        public void get(Example.MyKryo kryo) {
            buffer.position( mark );
            buffer.limit( mark + idOffset );
            payload = (Admin) kryo.get(buffer());
        }
        public void handle(Orator orator,ByteBuffer buf,int limit,boolean copy) {
            super.handle( orator, buf, limit, copy );
            orator.memos.putb((Memo) this);
        }
    }

    public static class Multi extends Message {
        { header += 4*1; }
        public Message msg;
        public int offset;
        public int supID;
        public int total;
        /** msg must already be typed */
        public Multi set(Message msg,int supID,int offset,int maxSize) {
            this.msg = msg;
            this.nest = msg.nest;
            this.supID = supID;
            this.offset = offset;
            core = Math.min( msg.size - offset, maxSize - header );
            size = core + header;
            return this;
        }
        public void read(ByteBuffer buf) { supID = buf.getInt(); }
        public void write(ByteBuffer buf) {
            super.write( buf );
            buf.putInt( supID );
        }
        public void handle(Orator orator,ByteBuffer buf,int limit,boolean copy) {
            read( buf );
            SupidKey key = new SupidKey( nest.roa, supID );
            OratorUtils.MultiBuffer mb = orator.multiMap.get( key );
            boolean ready = mb.add( buf, offset, limit - buf.position(), total );
            if (ready) {
                orator.multiMap.map.remove( key );
                orator.handle( mb.getBuffer(), nest, true );
            }
        }
        public String txt() { return String.format( "%s -- %s/%d-%d of %d", super.txt(), msg.txt(), offset, offset+core-1, total ); }
    }


    public static class Sup extends Multi {
        { header += 4; }
        public byte [] data;
        public Sup makeSup() {
            this.total = msg.size;
            data = new byte[ total ];
            msg.write( ByteBuffer.wrap( data ) );
            return this;
        }
        public void read(ByteBuffer buf)  { super.read ( buf ); total = buf.getInt(); }
        public void write(ByteBuffer buf) { super.write( buf ); buf.putInt( total ); buf.put( data, 0, core ); }
    }


    public static class Sub extends Multi {
        { header += 4; }
        Sup sup;
        public Sub  setSup(Sup sup) { this.sup = sup; return this; }
        public void write(ByteBuffer buf) {
            super.write( buf );
            buf.putInt( offset );
            buf.put( sup.data, offset, core );
        }
        public void read(ByteBuffer buf) { super.read(buf); offset = buf.getInt(); }
    }



    public static class Ack extends Message {
        { header += 4; }
        // todo::efficiency -- might make sense to send the (sub)set of received packets
        public int packetID, oldestID;
        public Ack() { reqAck = false; core = 4; size = core + header; }
        public Ack set(int packetID,int oldestID) {
            this.packetID = packetID;
            this.oldestID = oldestID;
            return this;
        }

        public void write(ByteBuffer buf) {
            super.write( buf );
            buf.putInt( packetID );
            buf.putInt( oldestID );
        }
        public void handle(Orator up,ByteBuffer buf,int limit,boolean copy) {
            read(buf);
            up.ack( nest, packetID, oldestID );
        }
        public void read(ByteBuffer buf) {
            packetID = buf.getInt();
            oldestID = buf.getInt();
        }
    }

    public static class Composite extends Message {
        { header += 0; }
        public Message [] msgs;
        public Composite set(Message ... msgs) {
            this.msgs = msgs;
            core = size = 0;
            reqAck = false;
            for (Message msg : msgs) { core += msg.size; reqAck = reqAck || msg.reqAck; }
            size = core + header;
            nest = msgs[0].nest;
            return this;
        }
        public void write(ByteBuffer buf) {
            super.write( buf );
            for (Message msg: msgs) {
                buf.putInt( msg.size );
                msg.write( buf );
            }
        }
        public void handle(Orator orator,ByteBuffer buf,int limit,boolean copy) {
            for (int pos = 0, len = 0; pos+len < limit; ) {
                len = buf.getInt();
                pos = buf.position();
                int subtype = buf.getInt();
                orator.handle(buf,nest,pos+len,subtype);
            }
        }
        public String txt() {
            String ret = super.txt();
            for (Message msg: msgs) {
                ret += "\n\t" + msg.txt();
            }
            return ret;
        }
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



}
