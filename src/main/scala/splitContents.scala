import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode

/**
  * Created by dell on 2019/5/3.
  */
object splitContents {
  case class Contents(name:String,content:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("testSplitContents")
    val spark =  SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    //读取文件
    val rdd = spark.sparkContext.textFile("c:\\sql_data\\splitContents.txt")
    var trans_rdd = rdd.map(x=>x.split(",")).map(x=>{
      Contents(x(0).toString,x(1).toString)
        }
      )
     val df =  trans_rdd.toDF()

    df.createOrReplaceTempView("test_contents")
    val df1 = spark.sql("select name,concat_ws(',',collect_set(content)) as contents from test_contents group by name");
    df1.show()
//    +-----+--------------------+
//    | name|            contents|
//    +-----+--------------------+
//    |  zzk|i am zzk ,come fr...|
//    |messi|nice to meet you ...|
//    +-----+--------------------+

    //***********************一列数据拆分成多列*************************
    val df2 = df1.withColumn("splitcol",split(col("contents"),",")).select(
      col("splitcol").getItem(0).as("content1"),
      col("splitcol").getItem(1).as("content2"),
      col("splitcol").getItem(2).as("content3")
    ).drop("splitcol");
    df2.show(true)
//    +--------------------+------------------+-------------------+
//    |content1            |content2          |content3           |
//    +--------------------+------------------+-------------------+
//    |i am zzk            |come from shandong|oh nice to meet you|
//    |nice to meet you too|i am messi        |come from argentina|
//    +--------------------+------------------+-------------------+


    //***********************一行数据拆分多行*************************
    val df3 = df1.withColumn("arrayCol",split(col("contents"),",")).withColumn("expCol",explode(col("arrayCol")));
    df3.show(false)
//    +-----+---------------------------------------------------+-------------------------------------------------------+--------------------+
//    |name |contents                                           |arrayCol                                               |expCol              |
//    +-----+---------------------------------------------------+-------------------------------------------------------+--------------------+
//    |zzk  |i am zzk ,come from shandong,oh nice to meet you   |[i am zzk , come from shandong, oh nice to meet you]   |i am zzk            |
//    |zzk  |i am zzk ,come from shandong,oh nice to meet you   |[i am zzk , come from shandong, oh nice to meet you]   |come from shandong  |
//    |zzk  |i am zzk ,come from shandong,oh nice to meet you   |[i am zzk , come from shandong, oh nice to meet you]   |oh nice to meet you |
//    |messi|nice to meet you too,i am messi,come from argentina|[nice to meet you too, i am messi, come from argentina]|nice to meet you too|
//    |messi|nice to meet you too,i am messi,come from argentina|[nice to meet you too, i am messi, come from argentina]|i am messi          |
//    |messi|nice to meet you too,i am messi,come from argentina|[nice to meet you too, i am messi, come from argentina]|come from argentina |
//    +-----+---------------------------------------------------+-------------------------------------------------------+--------------------+

  }


}
