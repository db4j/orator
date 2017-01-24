// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.nqzero.orator.Orator.Adminable;
import com.nqzero.orator.Orator.Taskable;
import com.nqzero.orator.OratorUtils.SupidKey;
import java.nio.ByteBuffer;

public abstract class Message {

    public static Class [] classList = new Class [] {
        User.class, Admin.class, Sup.class, Sub.class, Composite.class, Printer.class, Task.class,
        Ack.class, TaskBase.class, TaskReply.class
    };
    public static int tracking = 0;
    
    { header = 4*1; }
    int header, type, id, track;
    public int core, size;
    public OratorUtils.Nest nest;
    public Orator.Callback cb;
    public boolean reqAck = true;
    public void write(ByteBuffer buf) {
        buf.putInt( type );
    }
    public abstract void handle(Orator orator,ByteBuffer buf,int limit,boolean copy);

    public Message cb(Orator.Callback cb) { this.cb = cb; return this; }

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
        public TT set(UU obj,Output scratch,Example.MyKryo kryo) {
            payload = obj;
            scratch.clear();
            int idpos = kryo.put( scratch, obj, true );
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
        public void getIDs(Orator orator) {
            // fixme:race-condition - if downstream needs to register a class during kryo serialization of
            //   a reply, should request that the upstream does it first and send the ids
            //   eg, use name based resolving, keep a list, include the list in the reply
            //       and upstream sends the ids back
            // boss loop, down+up(TaskReply)
            // could skip on up
            orator.boss.check();
            classIDs = orator.boss.kryo.getIDs(buffer(), mark + idOffset);
        }
        public String txt() {
            return "Flat:" + (payload==null ? "null" : payload.toString());
        }
    }

    public abstract static class User<UU> extends Flat<User,UU> {
        { header += 0; }

        // called by user loop
        public void get(Orator orator) {
            buffer.position( mark );
            buffer.limit( mark + idOffset );
            nest.kelly.assertUpstream(this instanceof TaskReply);
            if (this instanceof TaskReply)
                payload = (UU) orator.kryo2().get( buffer() ); // user up
            else 
                payload = (UU) nest.kelly.kryo().get( buffer() ); // user down
        }
        public void handle(Orator orator,ByteBuffer buf,int limit,boolean copy) {
            super.handle( orator, buf, limit, copy );
            orator.boss.userq.offer( (User) this);
        }
        // run by user loop, ie after being recieved
        public abstract void exec(Orator orator,Orator.Logable logger);
    }

    public static abstract class TaskBase<TT> extends User<TT> {
        { header += 4; }
        /** id used to identify a reply message, non-zero --> send a reply */
        public int msgID;

        public void read(ByteBuffer buf) { super.read( buf ); msgID = buf.getInt(); }
        public void writeTT(ByteBuffer buf) { super.writeTT( buf ); buf.putInt( msgID ); }
    }

    // created on upstream, sent to downstream
    public static class Task extends TaskBase<Taskable> {
        { header += 0; }

        public Task() {}
        
        /** add a callback to the message for the reply - calling more than once will "leak" the callback */
        public Task setCallback(Orator orator,Orator.Callback cb) {
            msgID = orator.send.msgID++;
            orator.send.callbackMap.put( msgID, cb );
            return this;
        }

        public void exec(Orator orator,Orator.Logable logger) {
            Orator.Scoper scoper = new Orator.Scoper().set( orator, this, logger );
            Object obj = payload.task( scoper );
            if (msgID != 0) {
                // assert user
                nest.kelly.assertUpstream(false);
                Message.TaskReply rep = new Message.TaskReply();
                Output buf = new Output(new byte[2048]);
                rep.set( obj, buf, nest.kelly.kryo() ); // user down
                rep.nest = nest;
                rep.msgID = msgID;
                orator.send.sendq.offer( rep );
            }
        }
    }
    // created on downstream, sent to upstream
    public static class TaskReply extends TaskBase {
        { header += 0; }

        public void exec(Orator orator,Orator.Logable logger) {
            // upstream
            Orator.Scoper xx = new Orator.Scoper().set( orator, this, logger );
            Orator.Callback callback = orator.send.callbackMap.get( msgID );
            callback.callback( payload, xx );
        }
    }

    public static class Printer extends User<Object> {
        { header += 0; }
        public void exec(Orator orator,Orator.Logable logger) {
            System.out.println( "Printer: " + payload );
        }
    }

    public static class Admin extends Flat<Admin,Adminable> {
        { header += 0; }

        public void get(Orator orator) {
            orator.boss.check();
            buffer.position( mark );
            buffer.limit( mark + idOffset );
            payload = (Adminable) orator.boss.kryo.get(buffer()); // boss loop, up+down
        }
        public void handle(Orator orator,ByteBuffer buf,int limit,boolean copy) {
            super.handle( orator, buf, limit, copy );
            orator.boss.defineq.offer( this );
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
            OratorUtils.MultiBuffer mb = orator.recv.multiMap.get( key );
            boolean ready = mb.add( buf, offset, limit - buf.position(), total );
            orator.logger.log( "Multi,%d -- %d of %d", nest.kelly.id, mb.sum, mb.total );
            if (ready) {
                orator.recv.multiMap.map.remove( key );
                orator.recv.handle( mb.getBuffer(), nest, true );
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
        public void handle(Orator orator,ByteBuffer buf,int limit,boolean copy) {
            super.handle( orator, buf, limit, copy );
        }
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
        public void handle(Orator orator, ByteBuffer buf, int limit, boolean copy) {
            packetID = buf.getInt();
            oldestID = buf.getInt();
            orator.ack( nest, packetID, oldestID );
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
                Message msg = orator.mutils.alloc( subtype );
                msg.nest = nest;
                msg.handle( orator, buf, pos+len, false );
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

    public static void main(String [] args) throws Exception {
        Orator.main( "net" );
//        new Orator().init( Orator.defaultPort ).start();
    }


}
