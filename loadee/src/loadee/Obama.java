// copyright 2017 nqzero - see License.txt for terms

package loadee;

import com.esotericsoftware.kryo.io.Output;
import com.nqzero.orator.Orator;
import com.nqzero.orator.Orator.Taskable;
import com.nqzero.orator.OratorUtils;
import java.nio.ByteBuffer;

public class Obama {

    public static class Biden {
        public String text = "bitter people and their guns";
        public String toString() { return "Biden: " + text; }
    }

    
    public static class Dummy implements Taskable<String> {
        public String dummy = "yes we can";
        public String task(Orator.Scoper xx) {
            OratorUtils.Kelly kelly = xx.msg.nest.kelly;
            xx.logger.log( "task,%d: %s --> %s, %s\n",
                    kelly.id, this, dummy, this.getClass().getClassLoader() );
            Biden biden = new Biden();
            int cnt = 0;
            cnt = xx.orator.virus.count.incrementAndGet();
            xx.logger.log( "task,%d: result: %s, count:%d", kelly.id, biden, cnt );
            String ret = String.format( "Obama::stdout:%d -- result: %s, count:%d",
                    kelly.id, biden, cnt );
            System.out.println( ret );
            return ret;
        }
    }


    public static void main(String [] args) throws Exception {

        Orator net = new Orator().init( 0 );
        net.start();

        Output buf = new Output(new byte[2048]);
        net.sendTask(new Dummy(), net.nestify(net.root, null, Orator.defaultPort), buf, new Orator.V3C());

        org.srlutils.Simple.sleep( 3000 );
        net.diag();
    }
}



















