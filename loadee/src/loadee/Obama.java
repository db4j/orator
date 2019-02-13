// copyright 2017 nqzero - see License.txt for terms

package loadee;

import com.nqzero.orator.Orator;
import com.nqzero.orator.DemoOrator;
import com.nqzero.orator.OratorUtils.Taskable;
import com.nqzero.orator.OratorUtils;
import kilim.Mailbox;

public class Obama {

    public static class Biden {
        public String text = "bitter people and their guns";
        public String toString() { return "Biden: " + text; }
    }

    
    public static class Dummy implements Taskable<String> {
        public String dummy = "yes we can";
        public String task() {
            Biden biden = new Biden();
            String ret = String.format( "Obama::stdout:%d -- result: %s, count:%d",
                    0, biden, 0);
            System.out.println( ret );
            return ret;
        }
    }

    static User user = new User("hello","world");
    public static class User {
        public String name, bio;
        public User() {}
        public User(String name,String bio) { this.name=name; this.bio=bio; }
        public String toString() { return "user: " + name + " <" + bio + ">"; }
    }    
    public static class Dummy2 implements Taskable<User> {
        public User task() {
            User user = Obama.user;
            System.out.println("task.getter.user: " + user);
            return user;
        }
    }

    public static void main(String [] args) throws Exception {
        if (kilim.tools.Kilim.trampoline(false,args))
            return;

        Orator net = new Orator();
        net.init(0);

        OratorUtils.Remote roa = net.remotify(new OratorUtils.Remote().set(null,0).inet,DemoOrator.defaultPort);
        OratorUtils.Nest root = net.nestify(net.root,roa);
        for (int ii=0; ii < 20; ii++) {
            Mailbox box = net.sendTask(new Dummy2(),root);

            Object result = box.getb();
            System.out.println("result: " + result);
        }
        System.exit(0);
    }
}



















