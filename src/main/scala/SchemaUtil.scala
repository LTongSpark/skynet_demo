import java.sql.{Blob, Clob, Date, Ref, Struct, Timestamp}

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


object SchemaUtil {
  def createDataFrame(spark: SparkSession ,inputDataFrame:DataFrame): DataFrame ={
    var outputDataFrame = inputDataFrame
    import org.apache.spark.sql.functions._

    //创建udf函数 处理array类型的数据
    val arrayToStr = udf((str: Any) => {
      str match {
        case p: Seq[StructType] => p.asInstanceOf[Seq[StructType]].mkString(",")
        case p: Seq[String] => p.asInstanceOf[Seq[String]].mkString(",")
        case p: String => {
          p.toString.replaceAll("\r\n|\n" ,"")
        }
        case p: Timestamp => p.toString.substring(0,p.toString.lastIndexOf('.'))
        case p: Object =>p.toString.replace("\r\n" ,"")
        case p: java.lang.Double =>p.toString.substring(0,p.toString.lastIndexOf("."))
        case p: java.lang.Byte =>p.toString.replace("\r\n" ,"")
        case p: Date => p.toString.replace("\r\n" ,"")
        //oracle
        case p: Blob =>p.toString.replace("\r\n" ,"")
        case p: Clob =>p.toString.replace("\r\n" ,"")
        case p: Struct =>p.toString.replace("\r\n" ,"")
        case p: Ref =>p.toString.replace("\r\n" ,"")
        case p: java.sql.Array =>p.toString.replace("\r\n" ,"")
        case p: java.sql.ResultSet =>p.toString.replace("\r\n" ,"")
        // case p: oracle.sql.BFILE =>p.toString
        //case p: oracle.sql.ROWID =>p.toString

        case _ => null
      }
    })

    outputDataFrame.schema.foreach(structname => {
      val typename = structname.dataType.simpleString
      if (typename.startsWith("array")) {
        outputDataFrame = outputDataFrame.withColumn(structname.name, arrayToStr(outputDataFrame(structname.name)))
      } else if (typename.startsWith("struct")) {
        outputDataFrame = outputDataFrame.withColumn(structname.name, to_json(outputDataFrame(structname.name)))
      } else if (typename.startsWith("map")) {
        outputDataFrame = outputDataFrame.withColumn(structname.name, to_json(col(structname.name)))
      }else if (typename.startsWith("timestamp")){
        outputDataFrame = outputDataFrame.withColumn(structname.name, arrayToStr(outputDataFrame(structname.name)))
      }else if (typename.startsWith("double")){
        outputDataFrame = outputDataFrame.withColumn(structname.name, arrayToStr(outputDataFrame(structname.name)))
      }else if (typename.startsWith("binary")){
        outputDataFrame = outputDataFrame.withColumn(structname.name, arrayToStr(outputDataFrame(structname.name)))
      }else if (typename.startsWith("date")){
        outputDataFrame = outputDataFrame.withColumn(structname.name, arrayToStr(outputDataFrame(structname.name)))
      }else if (typename.startsWith("string")){
        outputDataFrame = outputDataFrame.withColumn(structname.name, arrayToStr(outputDataFrame(structname.name)))
      }
    })
    outputDataFrame
  }
  def partition(count:Long ,partion:Array[String]): (Int ,Long) ={
    var number:Long = 0
    var numPartition:Int = 0
    if(count < 10000){  //18400
      number = 1
    }else if(count >= 10000 && count < 100000){
      number = count /10000
    }else if(count >= 100000 && count < 10000000){
      number = count /100000
    }

    val limit = count /number
    if(limit * number == count){
      numPartition = number.toInt
    }else{
      numPartition = (number + 1l).toInt
    }
    (numPartition ,limit)

  }

}
