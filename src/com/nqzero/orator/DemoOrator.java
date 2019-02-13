package com.nqzero.orator;

import com.esotericsoftware.kryo.io.Output;
import com.nqzero.orator.OratorUtils.Taskable;
import kilim.Mailbox;
import kilim.Pausable;

public class DemoOrator {
    public static int defaultPort = 6177;

    public static class Getter3 implements Taskable<String> {
        public String task() throws Pausable {
            System.out.println("task.getter.user: ");
            return "hello world";
        }
    }
    
    public static void main(String[] args) {
        if (kilim.tools.Kilim.trampoline(false,args))
            return;
        int port = defaultPort;

        boolean server = args.length > 0;
        if (server)
            new Orator().init(port);

        Getter3 getter3 = new Getter3();
        
        Orator net = new Orator();
        net.init(0);

        OratorUtils.Remote roa = net.remotify(new OratorUtils.Remote().set(null,0).inet,port);
        OratorUtils.Nest root = net.kellyify(0,true,roa);

        Mailbox box = net.sendTask(getter3,root);
        Object result = box.getb();
        System.out.println("result: " + result);
        System.exit(0);
    }
    
}
