// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Output;
import com.nqzero.orator.Orator.Adminable;
import com.nqzero.orator.Orator.NetClass;

public abstract class Admin implements Adminable {

    public static Class [] classList = new Class [] {
        ReqIDs.class, Reply.class, ReqCode.class, ReplyCode.class, NetClass.class, NetClass[].class, byte[].class
    };
    public transient OratorUtils.Nest nest;

    /** thread-safe. scratch is used to construct the message, but no reference to it is kept */
    public Message.Memo wrap(Output scratch,Example.MyKryo kryo) {
        Message.Memo msg = new Message.Memo().set( this, scratch, kryo );
        msg.nest = nest;
        return msg;
    }
    /** boss loop only */
    public void reply(Orator orator,Admin next) {
        orator.boss.check();
        Message msg = next.wrap( orator.boss.bb, orator.boss.kryo ); // boss up+down
        orator.send.sendq.offer( msg );
    }
    public void nest(OratorUtils.Nest nest) { this.nest = nest; }


    // always constructed on downstream (except Reply subclass)
    public static class ReqIDs extends Admin {
        public NetClass [] klasses;

        public void admin(Orator orator) {
            orator.boss.check();
            // boss loop, upstream
            for (NetClass nc : klasses) {
                Registration kryoClass = orator.kryo2().getRegistration(nc.classID);
                nc.set( kryoClass.getType() );
            }
            reply( orator, new Reply().set( this ) );
        }
        public ReqIDs set(ReqIDs req) {
            klasses = req.klasses;
            nest = req.nest;
            return this;
        }
        public ReqIDs set(int [] classIDs) {
            int len = classIDs.length;
            klasses = new NetClass [ len ];
            for (int ii = 0; ii < len; ii++) {
                klasses[ ii ] = new NetClass();
                klasses[ ii ].classID = classIDs[ ii ];
            }
            return this;
        }
        public String txt() {
            String desc = getClass() + " -- ";
            for (NetClass klass : klasses) desc += klass==null ? null : klass.classID + ":" + klass.name;
            return desc;
        }
    }
    public static class Reply extends ReqIDs {
        public void admin(Orator orator) {
            // boss loop, downstream
            boolean more = false, need, anyReady = false;
            for (int ii = 0; ii < klasses.length; ii++) {
                NetClass nc = klasses[ ii ];
                need = nest.kelly.checkClass( nc );
                if (need) more = true;
                else { klasses[ii] = null; anyReady = true; }
            }
            if (more)
                reply( orator, new ReqCode().set( this ) );
            if (anyReady) orator.boss.retry( nest.kelly );
        }
    }
    public static class ReqCode extends ReqIDs {
        public int msgID;
        public ReqCode set(String ... classNames) {
            int len = classNames.length;
            klasses = new NetClass [ len ];
            for (int ii = 0; ii < len; ii++) {
                klasses[ ii ] = new NetClass();
                klasses[ ii ].name = classNames[ ii ];
            }
            return this;
        }
        public void admin(Orator orator) {
            // boss loop, upstream
            orator.boss.check();
            for (NetClass nc : klasses)
                if (nc != null) nc.code = orator.read( nc.name, nest.kelly.loader );
            reply( orator, new ReplyCode().set( this ) );
        }
    }
    public static class ReplyCode extends ReqCode {
        // fixme::maybe -- this should only be exec'd in a downstream ... should i verify that here ???
        public ReplyCode set(ReqCode req) { super.set( req ); msgID = req.msgID; return this; }
        public void admin(Orator orator) {
            orator.boss.check();
            if (msgID > 0)
                nest.kelly.loader.codeq.offer( this );
            else 
                for (NetClass nc : klasses)
                    if (nc != null) nest.kelly.addCode( nc );
            orator.boss.retry( nest.kelly );
        }
    }
    public static class Shutdown extends ReqIDs {
        public void admin(Orator orator) {
            orator.shutdown();
        }
    }
}
