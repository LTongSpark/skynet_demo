import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object spark_demo {
  def main(args: Array[String]): Unit = {
      // 10.12.133.145,post,/app2/index.xml
      val conf = new SparkConf().setMaster("local[*]").setAppName("tong")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(conf,Seconds(2))


    val data: DStream[String] = ssc.textFileStream("d:\\test.txt")
    data.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
