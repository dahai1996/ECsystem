import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

case class ProductRating (userId: Int, productId: Int, score: Double, timestamp: Int)
case class MongoConfig (uri: String, db: String)

//定义一个标准推荐对象
case class Recommendation(productId:Int,score:Double)
//定义用户的推荐列表
case class UserRecs(userId:Int,recs:Seq[Recommendation])
//定义商品相似度列表
case class ProductRecs(protectId:Int,recs:Seq[Recommendation])
/*离线推荐
* 算法：协同过滤算法
* 源数据：rating表
* 功能：
* 1.计算出用户推荐表：USER_RECS （用户，【商品，评分】） 即用户对每个商品的评分
* 2.计算出商品相似度表：ProductRecs （商品，【商品，相似度】）  即商品和其他商品相似度表
* 思路：
* 1.从原始数据表Rating中得到数据
* 2.使用协同过滤ALS训练出模型
* 3.使用模型的predict（）方法，得出用户对每个商品的评分（预测，真实皆有）。为了得到所有用户的数据，
*   传入参数需要为所有用户与所有商品的笛卡尔积
* 4.使用模型的productFeatures（）方法，得出商品特征向量。为了得出所有商品两两互相的相似度，
*   传入参数为所有商品向量与自身的笛卡尔积，并去除自身向乘
*
* 5.保留相似度>0.4的，并排序之后，存入mongodb
* */
object Lixian_Recommender {

  //monggo中的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS="UserRecs"
  val PRODUCT_RECS="ProductRecs"
  val USER_MAX_RECOMMENDATION=20

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
        (rating.userId,rating.productId,rating.score)
      }.cache() //持久化在内存

    //提取出用户和商品的数据集
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()

    //开始处理
    //1.训练模型
      //模型参数，Rating类型，需要转换
    val trainData=ratingRDD.map(x=>Rating(x._1,x._2,x._3))
      //模型参数
    val (rank,iterations,lambda) = (5,10,0.01)
    val model = ALS.train(trainData,rank,iterations,lambda )
    //2.获得预测评分矩阵，得到用户的推荐列表
      //需要用户和商品的笛卡尔积，
    val userProducts = userRDD.cartesian(productRDD)
    //得到预测矩阵
    val preRating = model.predict(userProducts) //参数：用户id，商品id，返回值：预测评分
    //从预测矩阵得到推荐列表：即对评分进行排序
    val userRecs = preRating.filter(_.rating>0)
        .map{ rating=>
          (rating.user,(rating.product,rating.rating))
        } //转化为 (k,v)对
        .groupByKey()
        .map{ case (userId,recs)=>
          UserRecs(userId,recs.toList.sortWith(_._2>_._2).
            take(USER_MAX_RECOMMENDATION).
            map(x=>Recommendation(x._1,x._2)))
        }
        .toDF()
    //写入MongoDB
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //3.利用商品的特征向量，计算商品的相似度列表
    //RDD[(Int, Array[Double])] => RDD[(Int, DoubleMatrix)]
    val productFeatures = model.productFeatures.map{case (producatId,features)=>
      (producatId, new DoubleMatrix(features))
    }
    //两两配对，计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
        .filter{case (a,b)=>
          a._1 != b._1
        }//去掉与自己的笛卡尔配对
    //计算
        .map{case (a,b)=>
          val simScore = consinSim(a._2,b._2) //抽为方法
          (a._1,(b._1,simScore)) // kv队，a的商品id为k
        }
        .filter{_._2._2 > 0.4}
        .groupByKey()
        .map{ case (productId,recs)=>
          ProductRecs(productId,recs.toList.sortWith(_._2>_._2).
            take(USER_MAX_RECOMMENDATION).
            map(x=>Recommendation(x._1,x._2)))
        }
        .toDF()
    //写入MongoDB
    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  def consinSim(product1:DoubleMatrix,product2:DoubleMatrix): Double ={
    product1.dot(product2)/(product1.norm2()*product2.norm2())
  }
}
