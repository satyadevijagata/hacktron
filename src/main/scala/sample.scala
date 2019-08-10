import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.DataFrame


object ProductAnalysis {
  def main(args: Array[String]) {
    /*val conf: SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val dataset1: RDD[String] = sc.textFile("data/file.csv")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val k=sqlContext.createDataFrame(dataset1) */

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val sc = spark.sparkContext
    // val dataset_in: RDD[String] = sc.textFile("data/file.csv")


    // val df = dataset1.map(x => x.split(",")).map(y => Row(y(0), y(1), y(2)))


    // val df1 = spark.createDataFrame(df, schema)


    val product_df = BuildDataFrame("data/Product.txt" , "|",spark,"data/Product.json")

    val customer_df = BuildDataFrame("data/Customer.txt" , "|",spark,"data/Customer.json")
    val Sales_df = BuildDataFrame("data/Sales.txt", "|",spark,"data/Sales.json")
    val Refund_df = BuildDataFrame("data/Refund.txt", "|",spark,"data/Refund.json")
    product_df.show()
    customer_df.show()
    Sales_df.show()
    Refund_df.show()
    val distri_sales=product_df.join(Sales_df,product_df("1Product_id") ===Sales_df("3product_id")
      , "inner")
    distri_sales.show()
      val distri_sales2=distri_sales.groupBy("2product_name","3product_type").agg(count("1transaction_id").as("sales_distirubtion"))

    distri_sales2.coalesce(1).write.csv("data/salesDistribution_cnt.txt")
    val distri_non_refund=Sales_df.join(Refund_df,Sales_df("1transaction_id") ===Refund_df("2original_transaction_id")
      , "left").filter(year(to_date(Sales_df("4timestamp"))).geq(lit(2013)))
    distri_non_refund.show()



    // df2.show()
  }
  def BuildDataFrame(path: String,delimiter:String,spark: SparkSession,JsonPath: String) : DataFrame = {
    //Dataset.foreach(println)
    val df_schema=spark.read.json(JsonPath)
    df_schema.printSchema()


    val df=spark.read.format("csv")
      .option("sep", delimiter)
      .option("inferSchema", "false")
      .option("header", "false")
      .schema(df_schema.schema)
      .load(path)
    df

  }



}