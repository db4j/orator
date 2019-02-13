// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.nqzero.orator.OratorUtils.Nest;
import com.nqzero.orator.OratorUtils.Remote;
import com.nqzero.orator.OratorUtils.Taskable;
import java.net.URL;
import java.net.URLClassLoader;

/*
 * TODO:
 *   anti-congestion filter
 *   testing: performance, perm-gen, cleanup on close
 */


public class DemoOrateur {







    

    public static class Virtual3 implements Runnable {
        public Class Dummy;
        Orator net = new Orator();
        public void run() {
            try {
                net.init( 0 );
                for (int ii = 0; ii < 40; ii++) {
                    Remote roa = net.remotify(new Remote().set(null,0).inet,DemoOrator.defaultPort);
                    Nest nest = net.kellyify(0,true,roa);
                    net.sendTask((Taskable) Dummy.newInstance(), nest);
                }
            } catch (Exception ex) {
                throw new RuntimeException( "Virtual failed ...", ex );
            }
        }
        public void init() {
            new Thread( this ).start();
        }
        public Virtual3() throws ClassNotFoundException {
            Dummy = this.getClass().getClassLoader().loadClass( "loadee.Obama$Dummy" );
        }
    }


    public static ClassLoader makeLoader(ClassLoader ucl) throws Exception {
        Loader.Proxy loader = new Loader.Proxy( ucl );
        // fixme - kilim doesn't support getResources yet, so the proxy fails to find the woven class
        Class klass = loader.loadClass( Virtual3.class.getName() );
        Object obj = klass.newInstance();
        org.srlutils.Simple.Reflect.invoke(obj,"init");
        return loader;
    }

    public static void main(String ... args) throws Exception {
        if (kilim.tools.Kilim.trampoline(false,args))
            return;
        boolean both = args.length == 0;
        boolean onlyNet = args.length > 0 && args[0].equals( "net" );
        int nn = 25;

        Orator host = null;
        if ( both || !onlyNet ) {
            host = new Orator();
            host.init( DemoOrator.defaultPort );
        }
        if ( both || onlyNet ) {

            URL [] urls = new URL[] {
                new URL("file:./loadee/target/classes/"),
                new URL("file:../loadee/target/classes/")
            };
            ClassLoader ucl = new URLClassLoader(urls,DemoOrateur.class.getClassLoader());

            for (int ii = 0; ii < nn; ii++) makeLoader(ucl);
        }
    }
}



















