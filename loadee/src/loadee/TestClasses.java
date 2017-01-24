// copyright 2017 nqzero - see License.txt for terms

package loadee;

public class TestClasses {

    public static class Basic {
        public String words = "hello world";
        public int val = 77;
        public String toString() { return words + val; }
    }

    public static void main(String [] args) throws Exception {
        System.out.println( Basic.class.getName() );
        System.out.println( Basic.class.newInstance() );
    }

}
