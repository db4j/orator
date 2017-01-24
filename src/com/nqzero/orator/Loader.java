// copyright 2017 nqzero - see License.txt for terms

package com.nqzero.orator;

import com.esotericsoftware.kryo.io.Output;
import com.nqzero.orator.Orator.NetClass;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import org.srlutils.DynArray;
import org.srlutils.Files;

public class Loader {
    public static boolean dbg = false;

    public static class NetLoader extends ClassLoader {
        public boolean dbg = true;
        public ConcurrentHashMap<String,byte[]> codeMap = new ConcurrentHashMap();
        public BlockingQueue<Admin.ReplyCode> codeq = new LinkedBlockingQueue();
        /** a non-definitive set of requested class names - could leave requests in the set after definition
         *    or fail to indicate a requested item. Note: iteration may fail
         */
        final HashSet<String> requests = new HashSet();
        public Orator orator;
        /**
         * the upstream nest to use for requesting class bytecode
         * the nest.kelly isn't used for serialization
         */
        public OratorUtils.Nest upstream;

        
        public NetLoader(Orator orator) { super( NetLoader.class.getClassLoader() ); this.orator = orator; }
        public NetLoader init(OratorUtils.Nest upstream) {
            this.upstream = upstream;
            return this;
        }
        public Class<?> defineClass(NetClass nc) {
            orator.logger.log( "NetLoader::define,%d -- %8d --> %40s --> %8d bytes\n",
                    upstream.kelly.id, nc.classID, nc.name, nc.code.length );
            Class klass = null;
            klass = defineClass( nc.name, nc.code, 0, nc.code.length );
            codeMap.put( nc.name, nc.code );
            synchronized( requests ) { requests.remove( nc.name ); }
            return klass;
        }
        public Class<?> loadClass(String name,boolean resolve) throws ClassNotFoundException {
            return loadClass( name, resolve, true );
        }

        public Class<?> loadClass(String name,boolean resolve,boolean useNet) throws ClassNotFoundException {
            Class klass = null;
            try { klass = super.loadClass( name, false ); } catch (ClassNotFoundException ex) {}
            if (klass == null && useNet) {
                Output buf = new Output(new byte[2048]);
                Admin.ReqCode admin = new Admin.ReqCode().set( name );
                admin.nest = upstream;
                admin.msgID = 1;
                orator.check();
                Message msg = admin.wrap( buf, orator.kryo2() ); // user loop, down
                try {
                    orator.send.sendq.put( msg );
                    do {
                        // fixme:race-condition -- another task could trigger addCode()
                        Admin.ReplyCode reply = codeq.take();
                        for (NetClass nc : reply.klasses)
                            if (nc != null) defineClass(nc);
                        klass = findLoadedClass( name );
                        orator.logger.log( "NetLoader::useNet,%d -- waiting for %s --> %s\n",
                                upstream.kelly.id, name, klass );
                    } while ( klass == null );
                } catch (InterruptedException ex) {
                    throw new RuntimeException( "NetLoader::useNet - interupted" );
                }
            }
            if (resolve) resolveClass( klass );
            return klass;
        }

        public boolean requested(String name) {
            synchronized( requests ) { return requests.add( name ); }
        }

        byte[] getBytecode(String name) {
            // fixme::completeness -- should handle delegation to the upstream ...
            return codeMap.get( name );
        }
    }

    /** return the class with binary name name if it is loaded, else null */
    public static Class<?> checkClass(String name) {
        try { return Class.forName( name ); } catch (Exception ex) { return null; }
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


        public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if ( skip.matcher( name ).matches() || skipMap.contains( name ) )
                return super.loadClass( name, resolve );
            if (false) Orator.cmpout( skipMap, name );
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
            if (klass == nullClass) throw new ClassNotFoundException();
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
