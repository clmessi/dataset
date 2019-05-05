package structuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object FileInputStructuredStreamingTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val lines = spark.readStream
      .option("sep", ";")
      .schema(userSchema)
      .csv("c:\\sql_data\\fileData\\")

    val query = lines.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
