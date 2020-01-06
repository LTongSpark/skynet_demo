import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import static java.util.concurrent.Executors.*;

public class tong {
    private  final static ExecutorService pool = newCachedThreadPool() ;
    public static void main(String[] args) {
         pool.execute(new Runnable() {
             @Override
             public void run() {
                 SparkAppHandle handle = null ;
                 try {
                     handle = new SparkLauncher()
                             .setSparkHome("/usr/local/spark")
                             .setAppResource("/usr/local/spark/spark-demo.jar")
                             .setMainClass("com.learn.spark.SimpleApp")
                             .setMaster("yarn")
                             .setDeployMode("cluster")
                             .setConf("spark.app.id", "11222")
                             .setConf("spark.driver.memory", "2g")
                             .setConf("spark.akka.frameSize", "200")
                             .setConf("spark.executor.memory", "1g")
                             .setConf("spark.executor.instances", "32")
                             .setConf("spark.executor.cores", "3")
                             .setConf("spark.default.parallelism", "10")
                             .setConf("spark.driver.allowMultipleContexts", "true")
                             .setVerbose(true).startApplication(new SparkAppHandle.Listener() {
                                 //这里监听任务状态，当任务结束时（不管是什么原因结束）,isFinal（）方法会返回true,否则返回false
                                 @Override
                                 public void stateChanged(SparkAppHandle sparkAppHandle) {
                                     if (sparkAppHandle.getState().isFinal()) {
                                         System.out.println("");
                                     }
                                     System.out.println("state:" + sparkAppHandle.getState().toString());
                                 }
                                 @Override
                                 public void infoChanged(SparkAppHandle sparkAppHandle) {
                                     System.out.println("Info:" + sparkAppHandle.getState().toString());
                                 }
                             });
                 } catch (IOException e) {
                     e.printStackTrace();
                 }

             }

         });




    }
}
