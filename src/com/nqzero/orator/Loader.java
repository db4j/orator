// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.nqzero.orator.OratorUtils.Logable;
import com.nqzero.orator.OratorUtils.NetClass;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import org.srlutils.DynArray;
import org.srlutils.Files;

public class Loader {
    
    public interface Oratorable {
        Logable logger();
        void sendMessage(Message msg) throws InterruptedException;
        void assertBoss();
    }

    public static class NetLoader extends ClassLoader {
        public boolean dbg = true;
        public ConcurrentHashMap<String,byte[]> codeMap = new ConcurrentHashMap();
        public BlockingQueue<Admin.ReplyCode> codeq = new LinkedBlockingQueue();
        /** a non-definitive set of requested class names - could leave requests in the set after definition
         *    or fail to indicate a requested item. Note: iteration may fail
         */
        ConcurrentHashMap.KeySetView<String,Boolean> requests = ConcurrentHashMap.newKeySet();
        public Oratorable orator;
        /**
         * the upstream nest to use for requesting class bytecode
         * the nest.kelly isn't used for serialization
         */
        public OratorUtils.Nest upstream;

        
        public NetLoader(Oratorable orator) { super( NetLoader.class.getClassLoader() ); this.orator = orator; }
        public NetLoader init(OratorUtils.Nest upstream) {
            this.upstream = upstream;
            return this;
        }
        synchronized
        public Class<?> defineClass(NetClass nc) {
            orator.logger().log("NetLoader::define,%s,%d -- %-40s,%8d,%8d bytes",
                    hashCode(), upstream.kelly.id, nc.name, nc.classID, nc.code.length);
            Class klass = null;
            klass = defineClass( nc.name, nc.code, 0, nc.code.length );
            codeMap.put( nc.name, nc.code );
            requests.remove(nc.name);
            return klass;
        }
        public Class<?> loadClass(String name,boolean resolve) throws ClassNotFoundException {
            return loadClass( name, resolve, true );
        }
        
        void slurp() throws InterruptedException {
            Admin.ReplyCode reply = codeq.take();
            for (NetClass nc : reply.klasses)
                if (nc != null) defineClass(nc);
        }

        synchronized
        public Class<?> loadClass(String name,boolean resolve,boolean useNet) throws ClassNotFoundException {
            Integer tid = OratorUtils.threadID.get();
            Class klass = null;
            try {
                while (useNet && ! codeq.isEmpty())
                    slurp();
                try { klass = super.loadClass( name, false ); } catch (ClassNotFoundException ex) {}
                if (klass == null && useNet) {
                    if (tid != null && tid==1)
                        throw new RuntimeException("Orator.Loader.badThread: " + tid + ", " + name);
                    Message msg = Admin.ReqCode.make(upstream,name);
                    orator.sendMessage(msg);
                    while (klass==null) {
                        if (codeq.isEmpty())
                            orator.logger().log("NetLoader::slurp ,%s,%d -- %s",hashCode(),upstream.kelly.id,name);
                        slurp();
                        klass = findLoadedClass(name);
                    }
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException( "NetLoader::useNet - interupted" );
            }
            if (resolve) resolveClass(klass);
            return klass;
        }

        public boolean requested(String name) {
            return requests.add( name );
        }

        byte[] getBytecode(String name) {
            // fixme::completeness -- should handle delegation to the upstream ...
            return codeMap.get( name );
        }
        /** return the class with binary name name if it is loaded, else null */
        public Class<?> checkClass(String name) {
            try { return Class.forName(name,false,this); } catch (Exception ex) { return null; }
        }
    }




    public static abstract class Definitive extends ClassLoader {
        public Definitive() {}
        public Definitive(ClassLoader parent) { super( parent ); }
        public ClassLoader target = getParent();
        public Pattern skip = Pattern.compile( "java.*|sun.*" );
        public final HashMap<String,Class> map = new HashMap();
        private Class nullClass = NullClass.class;
        public HashSet<String> skipMap = new HashSet();

        public void loadedByTarget(String name,Class klass, Exception exception) {}
        public void skip(Class ... klasses) {
            for (Class klass : klasses) skipMap.add( klass.getName() );
        }

        public static void cmpout(java.util.Collection map, Object key) {
            StringBuilder txt = new StringBuilder();
            txt.append( String.format( "cmp searching for key %s\n", key ) );
            for (Object kk : map) {
                txt.append( String.format( "cmp: %5b -- %s\n", key.equals( kk ), kk ) );
            }
            System.out.println( txt );
        }

        private boolean dbg;
        public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if ( skip.matcher( name ).matches() || skipMap.contains( name ) )
                return super.loadClass( name, resolve );
            if (dbg)
                cmpout(skipMap,name);
            Class klass;
            synchronized (map) { klass = map.get( name ); }
            if (klass == null) {
                klass = nullClass;
                try                  { klass = findClass( name ); }
                catch (Exception ex) {
                    try { klass = target.loadClass( name ); } catch (Exception ey) {}
                    if (klass != nullClass) loadedByTarget( name, klass , ex);
                }
                synchronized (map) { map.put( name, klass ); }
            }
            if (dbg) System.out.format( "Loader::klass %50s --> %s\n",
                                klass == nullClass ? null : klass.getClassLoader(), name );
            if (klass == nullClass) throw new ClassNotFoundException(name);
            if (resolve) super.resolveClass( klass );
            return klass;
        }

    }

    /** a proxy class loader -- grabs the bytecode from it's class loader and defines new classes */
    public static class Proxy extends Definitive {
        public Proxy() { super( Proxy.class.getClassLoader() ); }
        public Proxy(ClassLoader parent) { super( parent ); }

        public Class<?> findClass(String name) throws ClassNotFoundException {
            InputStream in = target.getResourceAsStream( resourceName( name ) );
            if (in==null)
                throw new ClassNotFoundException();
            DynArray.bytes bytes = org.srlutils.Files.readStream( in );
            Files.tryClose( in );
            if (bytes == null) throw new ClassNotFoundException();
            return defineClass( name, bytes.vo, 0, bytes.size );
        }

        public void loadedByTarget(String name, Class klass, Exception ex) {
            throw new Error( "Proxy::loadClass -- define failed, but load succeeded ...\n"
                    + "don't think this should ever happen -- " + name, ex );
        }
    }

    private static class NullClass {}
    public static String resourceName(String className) { return className.replace( '.', '/' ) + ".class"; }

    // demo classes for testing class loading
    private static class Foo {}
    private static class Payload {
        private Payload() { print( this ); print( new Foo() ); }
    }
    private static void print(Object obj) { System.out.format( "%50s -- %s\n", obj.getClass(), obj.getClass().getClassLoader() ); }

    public static void main(String [] args) throws Exception {
        Proxy proxy = new Proxy();
        String name = Payload.class.getName();
        new Payload();
        Class klass = proxy.loadClass( name );
        Constructor ctor = klass.getDeclaredConstructor();
        ctor.setAccessible( true );
        ctor.newInstance();
    }

}
