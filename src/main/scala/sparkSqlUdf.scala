import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by dell on 2019/5/3.
  */
object sparkSqlUdf {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("testFilter")
    val spark =  SparkSession.builder().config(conf).getOrCreate()

    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))

    val userDF = spark.createDataFrame(userData).toDF("name","age")
    userDF.show()

    userDF.createOrReplaceTempView("users")

    spark.udf.register("strLen",(str:String)=>str.length)
    spark.udf.register("isAdult",isAdult _)
    spark.sql("select name,age,strLen(name) as name_length,isAdult(age) as isAdult from users").show(false)
//    +-----+---+-----------+
//    |name |age|name_length|
//    +-----+---+-----------+
//    |Leo  |16 |3          |
//      |Marry|21 |5          |
//      |Jack |14 |4          |
//      |Tom  |18 |3          |
//      +-----+---+-----------+


//    +-----+---+-----------+-------+
//    |name |age|name_length|isAdult|
//    +-----+---+-----------+-------+
//    |Leo  |16 |3          |false  |
//      |Marry|21 |5          |true   |
//      |Jack |14 |4          |false  |
//      |Tom  |18 |3          |false  |
//      +-----+---+-----------+-------+

  }

  def isAdult(age:Int):Boolean ={
    if(age>18){

      true
    }else{
      false
    }

  }
}
