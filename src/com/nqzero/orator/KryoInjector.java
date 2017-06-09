package com.nqzero.orator;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;


public class KryoInjector {
    Kryo kryo;
    public FieldInjector injector;

    public KryoInjector() {
        this(new Kryo());
    }

    public KryoInjector(Kryo $kryo) {
        kryo = $kryo;
        injector = new FieldInjector(kryo);
    }

    public <TT> TT copy(TT src,TT dst,boolean copyTrans) {
        Dual<TT> dual = new Dual(src,dst,copyTrans);
        kryo.copyShallow(dual,injector);
        return dual.dst;
    }
    
    public static <KK extends Kryo> KK init(KK kryo) {
        FieldInjector injector = new FieldInjector(kryo);
        kryo.register(Dual.class,injector);
        return kryo;
    }
    public static <TT,UU extends TT> UU copy(Kryo kryo,TT src,UU dst,boolean copyTrans) {
        Dual<TT> dual = new Dual(src,dst,copyTrans);
        Serializer ser = kryo.getSerializer(Dual.class);
        if (! (ser instanceof FieldInjector))
            init(kryo);
        kryo.copyShallow(dual);
        return dst;
    }
    
    
    public static class FieldInjector<TT> extends FieldSerializer<Dual<TT>> {
        public FieldInjector(Kryo kryo) {
            super(kryo,Dual.class);
        }
	public Dual<TT> copy(Kryo kryo,Dual<TT> orig) {
            TT src = orig.src, copy = orig.dst;
            kryo.reference(src);
            kryo.reference(copy);

            Serializer ser = kryo.getSerializer(src.getClass());
            if (!(ser instanceof FieldSerializer))
                throw new RuntimeException("copying into an object is only supported for FieldSerializer types");
            FieldSerializer<TT> dummy = (FieldSerializer<TT>) ser;
            
            FieldSerializer.CachedField [] fields;
            int i,n;

            if (orig.copyTrans)
                for (fields=dummy.getTransientFields(),i=0,n=fields.length; i < n; i++)
                    fields[i].copy(src, copy);

            for (fields=dummy.getFields(),i=0,n=fields.length; i < n; i++)
                fields[i].copy(src, copy);

            return orig;
	}
    }
    public static class Dual<TT> {
        boolean copyTrans;
        TT src, dst;
        Dual(TT $src,TT $dst,boolean $copyTrans) { src=$src; dst=$dst; copyTrans=$copyTrans; }
    }
    static class Demo {
        public static class Dummy {
            int v1;
            transient int v2;
            String stuff;
            void init() {
                stuff = "hello world";
                v1 = 7;
                v2 = -33;
            }
            void print() {
                System.out.format("dummy ... %4d %4d    %-22s\n",v1,v2,stuff);
            }
        }
        public static class Fool extends Dummy {
            void print() {
                System.out.format("fools ... %4d %4d    %-22s\n",v1,v2,stuff);
            }
            void stuff() {
                System.out.format("stuff ... %4d %4d    %-22s\n",v1,v2,stuff);
            }
        }
        public static void main(String [] args) {
            KryoInjector ki = new KryoInjector();
            Kryo kryo = new Kryo();
            Kryo kryo2 = new Kryo();
            Kryo kryo3 = new Kryo();
            Kryo kryo4 = new Kryo();
            kryo.register(Dual.class,new FieldInjector(kryo));
            Dummy r1=new Dummy(), r3, r4=new Dummy();
            Fool f1=new Fool(), f2=new Fool(), f3=new Fool(), f4=new Fool(), f5=new Fool();

            r1.init();
            r3 = kryo.copyShallow(r1);


            r1.print();
            r3.print();
            ki.copy(r1,r4,false).print();
            ki.copy(r1,f1,true).print();
            copy(kryo2,r1,f2,true).stuff();
            copy(kryo2,r1,f3,false).print();
            init(kryo3);
            copy(kryo3,r1,f4,false).print();

            try {
                copy(kryo4,r1,f5,false).print();
                throw new Exception("this should never happen");
            }
            catch (Exception ex) { System.out.println("exception as expected"); }
        }
    }
    public static void main(String [] args) { Demo.main(args); }
}
