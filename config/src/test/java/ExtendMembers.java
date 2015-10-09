import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractConf;

/**
 * Created by hp on 4/20/15.
 */
public class ExtendMembers extends AbstractConf {

    @Override
    public void load() throws Exception {
        super.load();
        mysqlAddr = "hello...";
    }

    @Override
    public void statis() throws Exception {
        super.statis();
    }

    public static void main(String[] args) throws Exception {
        ExtendMembers em = new ExtendMembers();
        em.load();
        em.statis();

        AbstractConf ac = new ExtendMembers();
        ac.load();
        ac.statis();
    }
}
