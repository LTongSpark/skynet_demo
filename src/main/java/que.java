import java.util.LinkedList;
import java.util.Queue;

public class que {
    public static void main(String[] args) {
        //add()和remove()方法在失败的时候会抛出异常(不推荐)
        Queue<String> queue = new LinkedList<String>();
        //添加元素
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");
        queue.offer("d");
        queue.offer("e");

        while (true){
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("poll="+queue.poll()); //返回第一个元素，并在队列中删除

            if(queue.size() ==0){
                System.exit(-1);
            }
        }

    }
}
