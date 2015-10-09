import org.apache.log4j.Logger;

/**
 * Created by hp on 4/22/15.
 */
public class StringCloneTest {

    private static Logger logger = Logger.getLogger(StringCloneTest.class);

    public static void main(String[] args) throws Exception {
        String a = "abc";
        String b = a;
        a = "ccc";
        System.out.println(a + ", " + b);
        logger.info(a + ", " + b);
    }
}
