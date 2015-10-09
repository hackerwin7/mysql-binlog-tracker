/**
 * Created by hp on 4/21/15.
 */
public class StaticMember {
    public static int si = 0;
    public int i = 0;

    public static void main(String[] args) throws Exception {
        StaticMember.si = 4;
        StaticMember sm = new StaticMember();
        System.out.println(sm.si);
    }
}
