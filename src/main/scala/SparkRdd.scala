import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by dell on 2019/4/22.
  */
object SparkRdd {
  case class  per(id:Int, name:String,age:Int)

  def main(args: Array[String]): Unit = {
    //使用rdd, 过滤 ------->读文件， 判断和过滤
    //spark配置
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("testFilter")
    val spark =  SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //读取文件
    val rdd = spark.sparkContext.textFile("c:\\sql_data\\a.txt")
    val rdd2 = rdd.map(_.split(",")).map(arr=>{
      val id=arr(0).toInt
      val name=arr(1)
      val age=arr(2).toInt
      per(id,name,age)
    })

    //通过编程设置Schema
    val schemaString ="id name age"
    val field =schemaString.split(" ").map(fieldname=> fieldname match {
      case "id" =>StructField(fieldname,IntegerType,nullable = false);
      case "name" =>StructField(fieldname,StringType,nullable = true);
      case "age" =>StructField(fieldname,IntegerType,nullable = true);
    })
    val schema = StructType(field)

    val rowRDD = rdd.map(x=>x.split(",")).map(row =>Row(row(0).toInt,row(1),row(2).toInt))
    val peopelDF = spark.createDataFrame(rowRDD,schema)

    peopelDF.rdd.map(x=>x.getAs[Int]("age")).collect().foreach(println(_))
    peopelDF.show()

    val df = rdd2.toDF()
    val dsAs = df.as[per]
    val ds = rdd2.toDS()
    rdd2
    /**
      * ---+----+---+
      | id|name|age|
      +---+----+---+
      |  1|   a| 23|
      |  2|   b| 56|
      |  3|   c| 89|
      |  4|   d|100|
      |  5|lisi| 90|
      |  6|dage| 88|
      +---+----+---+
      */
    df.cache()

    val ds2 = ds.filter(x=>(x.age>30 && x.id%3==0))
    println("****************")
    ds2.show()
    println("*****************")
    //变换，过滤:  age >30 && id%3=0
    val  df2=df.filter(
      row=> row.get(2).asInstanceOf[Int] > 30
        && row.get(0).asInstanceOf[Int] %3==0)
    df2.show()
    /**
      * +---+----+---+
        | id|name|age|
        +---+----+---+
        |  3|   c| 89|
        |  6|dage| 88|
        +---+----+---+
      */

    //使用spark sql 查询
    df.createTempView("testTable")
    val  df3=spark.sql("select * from testTable where" +
      " age > 30 and id %3 ==0")
    df3.show()
    /**
      * +---+----+---+
        | id|name|age|
        +---+----+---+
        |  3|   c| 89|
        |  6|dage| 88|
        +---+----+---+
      */
  }
}
