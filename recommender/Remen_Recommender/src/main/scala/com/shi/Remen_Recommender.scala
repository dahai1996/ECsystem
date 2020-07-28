package com.shi
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class MongoConfig (uri: String, db: String)
case class Rating (userId: Int, productId: Int, score: Double, timestamp: Int)

/**
 *      热门商品推荐
 * 热门商品定义：近期评分次数多的
 * 共三个功能：
 * 1。热门商品总统计
 * 2.近期热门商品统计
 * 3.平均评分最高的优质商品
 *
 * 思路：
 * 1.从原始数据表Rating中得到数据
 * 2.根据需求对Rating表进行sql查询，得到需要的数据。如有需求，进行相关的处理
 * 3.将新数据重新导入到MongoDB中，建立新表进行存储
 */

object Remen_Recommender {
  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"



  def main (args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[1]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

    // 从原始数据表加载数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //创建临时表
    ratingDF.createOrReplaceTempView("ratings")

    //1.历史热门商品定义：评分个数来多少   count productId
    val rateMoreProductsDF = spark.sql("select productId,count(productId) as count from ratings group by productId order by count desc")
    storeDFInMongoDB(rateMoreProductsDF,RATE_MORE_PRODUCTS)

    //2.通过日期，找出近期热门商品，把时间转换一下方便比较
    //创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册udf，转换时间
    spark.udf.register("changeDate",(x:Int)=>simpleDateFormat.format(new Date(x*1000L)).toInt)
    //把原始数据转换成 productId,score,yearmoth
    val ratingOfYearMonthDF = spark.sql("select productId,score,changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF = spark.sql("select productId, count(productId) as count, yearmonth from ratingOfMonth group by yearmonth, productId order by yearmonth desc, count desc")
    //把处理好的df存到mongodb, 表名字：近期热门商品，RATE_MORE_RECENTLY_PRODUCTS
    storeDFInMongoDB(rateMoreRecentlyProductsDF,RATE_MORE_RECENTLY_PRODUCTS)

    // 3. 优质商品统计，商品的平均评分，productId，avg
    val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    storeDFInMongoDB( averageProductsDF, AVERAGE_PRODUCTS )

    spark.stop()
  }
//存储数据到db的方法，参数：df，表名
  def storeDFInMongoDB (df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
