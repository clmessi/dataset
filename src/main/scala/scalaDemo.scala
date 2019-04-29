//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.{SparkConf, SparkContext}
///**
//  * Created by dell on 2019/4/19.
//  */
//object DataSetDemo {
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("DataSet")
//    val sc  = new SparkContext(conf)
//    val spark = SparkSession.builder().appName("dataSet").master("local[2]").getOrCreate()
//    val rddString = spark.read.textFile("c:\\sql_data")
//    import spark.implicits._
////    val jsonString  = spark.read.json("c:\\sql_json")
////
////    //1:创建RDD
//////    val rddString = sc.textFile("C:\\sql_data")
////    //2：创建schema
////    jsonString.printSchema()
////    jsonString.createOrReplaceTempView("people")
////    val part_people = spark.sql("select * from people")
////    part_people.foreach(x=>println(x))
////    jsonString.select("name").show(4)
//
//
//
//
//
//
//    //**************************读取text文件创建dataFrame******************************
//    val schemaString = "name age phone"
//    val fields = schemaString.split(" ").map {
//      filedName => StructField(filedName, StringType, nullable = true)
//    }
//    val schema = StructType(fields)
//    //3：数据转成Row
//    val rowRdd = rddString.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1), attributes(2)))
//    //创建DF
//    val df = rowRdd.toDF()
//    df.createOrReplaceTempView("person")
//    val person = spark.sql("select * from person")
//    person.show(3)
////
////    val df = spark.createDataFrame(rowRdd,schema)
////    val personDF = spark.createDataFrame(rowRdd, schema)
//    //    personDF.show(5)
//  }
//
//}
