import java.util.Date;

public class UserTask {
    public static void main(String[] args) {
        String a = "123" ;
        String b ="234" ;
        Long.valueOf(a) ;
        Date date = new Date() ;
        System.out.println(date.getTime());
        System.out.println(Long.valueOf(a) > Long.valueOf(b));
    }
}
