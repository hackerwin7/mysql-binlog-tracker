import org.apache.log4j.Logger;

/**
 * Created by hp on 4/21/15.
 */
public class ExceptionCatchTest {

    private static Logger                           logger                      = Logger.getLogger(ExceptionCatchTest.class);

    public static void throwEx() throws Exception{
        throw new Exception("throw exception");
    }

    public static void main(String[] args) throws Exception {
        try {
            int a = 1 / 0;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.info("throwing......");
            throwEx();
        }
    }
}
