/**
 * Created by hp on 4/20/15.
 */
public class AA extends BasicA {

    public void change() {
        super.change();
        m = 11111111;
        n = 22222222;
    }

    public void print() {
        super.print();
    }

    public static void main(String[] args) {
        BasicA ba = new AA();
        ba.change();
        ba.print();
    }
}
