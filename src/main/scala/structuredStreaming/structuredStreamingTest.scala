package structuredStreaming
package com.ultrapower.spark.structure
import org.apache.spark.sql.SparkSession
/**
  * Created by zhongzhenkai on 2018/8/19.
  */
object structureStreamingTest {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.6.0");

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("StructuredNetworkWordCount").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "192.168.11.24")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
