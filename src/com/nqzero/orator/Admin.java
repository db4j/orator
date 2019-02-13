// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.esotericsoftware.kryo.Registration;
import com.nqzero.orator.OratorUtils.NetClass;
import com.nqzero.orator.OratorUtils.Nest;
import java.io.InputStream;
import kilim.Pausable;
import org.srlutils.DynArray;

public abstract class Admin {

    public static Class [] classList = new Class [] {
        ReqIDs.class, Reply.class, ReqCode.class, ReplyCode.class, NetClass.class, NetClass[].class, byte[].class
    };
    public transient OratorUtils.Nest nest;

    /** thread-safe. scratch is used to construct the message, but no reference to it is kept */
    public Message.Memo wrap() {
        Message.Memo msg = new Message.Memo().set(this);
        msg.nest = nest;
        return msg;
    }
    public void reply(Orator kup,Admin next) throws Pausable {
        Message msg = next.wrap();
        kup.outbox.put(msg);
    }
    public Admin nest(OratorUtils.Nest nest) { this.nest = nest; return this; }
    public abstract void admin(Orator kup) throws Pausable;


    // always constructed on downstream (except Reply subclass)
    public static class ReqIDs extends Admin {
        public NetClass [] klasses;

        public void admin(Orator kup) throws Pausable {
            for (NetClass nc : klasses)
                nc.set(kup.kryo2.getRegistration(nc.classID).getType());
            reply(kup,new Reply().set(this));
        }
        public ReqIDs set(ReqIDs req) {
            klasses = req.klasses;
            nest = req.nest;
            return this;
        }
        public static Message make(Nest nest,DynArray.ints classIDs) {
            ReqIDs req = new ReqIDs();
            req.nest = nest;
            int len = classIDs.size;
            req.klasses = new NetClass [ len ];
            for (int ii = 0; ii < len; ii++) {
                req.klasses[ii] = new NetClass();
                req.klasses[ii].classID = classIDs.get(ii);
            }
            return req.wrap();
        }
        public String txt() {
            String desc = getClass() + " -- ";
            for (NetClass klass : klasses) desc += klass==null ? null : klass.classID + ":" + klass.name;
            return desc;
        }
    }
    public static class Reply extends ReqIDs {
        private static int fmore=2;
        private static int fany=1;
        int check() {
            boolean more = false;
            boolean need;
            boolean anyReady = false;
            for (int ii = 0; ii < klasses.length; ii++) {
                NetClass nc = klasses[ ii ];
                need = nest.kelly.registerIfReady( nc );
                if (need) more = true;
                else { klasses[ii] = null; anyReady = true; }
            }
            return (more?fmore:0) + (anyReady?fany:0);
        }
        public void admin(Orator orator) throws Pausable {
            orator.userAdmin.tasks.put(this);
        }
        public void doAdmin(Orator orator) throws Pausable {
            int check = check();
            int more = check&fmore;
            int anyReady = check&fany;
            if (more > 0)
                reply( orator, new ReqCode().set( this ) );
            if (anyReady > 0)
                orator.retry(nest.kelly);
        }
        public static Message.Memo make(Nest nest,NetClass [] klasses) {
            Reply req = new Reply();
            req.nest = nest;
            req.klasses = klasses;
            return req.wrap();
        }
    }
    public static class ReqCode extends ReqIDs {
        public static Message.Memo make(Nest nest,String className) {
            ReqCode req = new ReqCode();
            req.nest = nest;
            NetClass nc = new NetClass();
            nc.name = className;
            req.klasses = new NetClass[]{nc};
            return req.wrap();
        }

        public void admin(Orator kup) throws Pausable {
            for (NetClass nc : klasses)
                if (nc != null) nc.code = readClass(nc.name);
            reply(kup,new ReplyCode().set(this));
        }
    }
    public static class ReplyCode extends ReqCode {
        // fixme::maybe -- this should only be exec'd in a downstream ... should i verify that here ???
        public void admin(Orator kup) throws Pausable {
            kup.handleCode(this);
        }
    }
    public static class Shutdown extends ReqIDs {
        public void admin(Orator kup) throws Pausable {
            throw new AssertionError();
        }
    }


    static public byte [] readClass(String name) {
        return readResource(cname(name));
    }
    static public byte [] readResource(String name) {
        ClassLoader cl = Admin.class.getClassLoader();
        InputStream in = cl.getResourceAsStream( name );
        DynArray.bytes bytes = org.srlutils.Files.readStream( in );
        return bytes.trim();
    }
    static public String cname(String name) { return name.replace( '.', '/' ) + ".class"; }
}
