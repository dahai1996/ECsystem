import Lixian_Recommender.MONGODB_RATING_COLLECTION
import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/*
* 模型评估
*
* */
object ALSTrainer {

  def getRMSE (model: MatrixFactorizationModel, testRDD: RDD[Rating]):Double = {
    val userProducts = testRDD.map{item=>
      (item.user,item.product)
    }
    val predictRating = model.predict(userProducts)

    //计算rmse
    //处理实际评分和预测评分的数据格式
    val observed = testRDD.map{item=>
      ((item.user,item.product),item.rating)
    }
    val predict = predictRating.map{item=>
      ((item.user,item.product),item.rating)
    }
    sqrt(
    observed.join(predict).map{case ((userId,productId),(actual,pre))=>
      val err=actual-pre
      err*err
    }.mean()
    )
  }

  def adjustALSParams (trainRDD: RDD[Rating], testRDD: RDD[Rating]) = {
    //遍历各项 参数
    val result = for( rank <- Array(5,10,20,50);lambda <-Array(1,0.1,0.01))
      yield
      {
        val model = ALS.train(trainRDD, rank, 10, lambda)
        val rmse = getRMSE(model, testRDD)
        (rank, lambda, rmse)
      }
    println(result.minBy(_._3))
  }

  def main (args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

    //加载原始数据
    val ratingRDD=spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map{rating=>
        Rating(rating.userId,rating.productId,rating.score)//提前转换为模型需要的类型
      }.cache() //持久化在内存

    //数据集切分为训练集和测试机
    val splits = ratingRDD.randomSplit(Array(0.8,0.2))
    val trainRDD = splits(0)
    val testRDD = splits(1)

    //核心实现：输出最优参数
    adjustALSParams(trainRDD,testRDD)

    spark.stop()
  }
}
