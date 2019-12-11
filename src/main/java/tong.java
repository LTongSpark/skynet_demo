import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class tong {
    public static void main(String[] args) {
        Date dt = new Date();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String nowTime = df.format(dt);
        java.sql.Timestamp buydate = java.sql.Timestamp.valueOf(nowTime);
        System.out.println(buydate);
    }
}
