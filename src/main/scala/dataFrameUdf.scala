import org.apache.spark.sql.SparkSession

/**
  * Created by dell on 2019/5/3.
  */
object dataFrameUdf {
  def main(args: Array[String]): Unit = {
    newDfUdf()
  }

  /**
    * 新版本(Spark2.x)DataFrame udf示例
    */
  def newDfUdf() {
    val spark = SparkSession.builder().appName("newDfUdf").master("local").getOrCreate()

    // 构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))

    //创建测试df
    val userDF = spark.createDataFrame(userData).toDF("name", "age")
    import org.apache.spark.sql.functions._
    //注册自定义函数（通过匿名函数）
    val strLen = udf((str: String) => str.length())
    //注册自定义函数（通过实名函数）
    val udf_isAdult = udf(isAdult _)

    //通过withColumn添加列
    userDF.withColumn("name_len", strLen(col("name"))).withColumn("isAdult", udf_isAdult(col("age"))).show
    //通过select添加列
    userDF.select(col("*"), strLen(col("name")) as "name_len", udf_isAdult(col("age")) as "isAdult").show

//    +-----+---+--------+-------+
//    | name|age|name_len|isAdult|
//    +-----+---+--------+-------+
//    |  Leo| 16|       3|  false|
//      |Marry| 21|       5|   true|
//      | Jack| 14|       4|  false|
//      |  Tom| 18|       3|  false|
//      +-----+---+--------+-------+
//
//    +-----+---+--------+-------+
//    | name|age|name_len|isAdult|
//    +-----+---+--------+-------+
//    |  Leo| 16|       3|  false|
//      |Marry| 21|       5|   true|
//      | Jack| 14|       4|  false|
//      |  Tom| 18|       3|  false|
//      +-----+---+--------+-------+


    //关闭
    spark.stop()
  }
  def isAdult(age:Int):Boolean ={
    if(age>18){

      true
    }else{
      false
    }

  }
}
