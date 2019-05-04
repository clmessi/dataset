package structuredStreaming

import org.apache.spark.sql.SparkSession

/**
  * Created by dell on 2019/5/4.
  */
object wordCount {
  def main(args: Array[String]): Unit = {

    val spark  = SparkSession.builder().master("local[2]").appName("structuredStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")
    import spark.implicits._
    val lines = spark.readStream.format("socket").option("host","192.168.11.24").option("port",9999).load()
     val words =   lines.as[String].flatMap(_.split(" "))
      val wordCount = words.groupBy("value").count()
   println("----------------------------")
//    wordCount.rdd.foreach(println(_))
    println("------------------------")
    val query = wordCount.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }

}
