import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}
import org.apache.spark.ml.feature.{PCA, StopWordsRemover, Tokenizer, Word2Vec, Word2VecModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Main {
  var K = 19
  var VECTOR_SIZE = 10
  var WORD_2_VEC_MIN_COUNT = 2


  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val tweetsFile = args(0) // "data/training.1600000.processed.noemoticon.csv"
    val stopWordsFile = args(1) // "data/stop-word-list.txt"

    val tweets = readTweets(spark.sparkContext.textFile(tweetsFile))
    val stopWords = spark.sparkContext.textFile(stopWordsFile)
    val tweetsDataFrame = tweets.toDF("tweets")
    val stopWordsDataFrame = stopWords.toDF("stopwords")
    val filteredTweets = filterStopWords(tweetsDataFrame, stopWordsDataFrame)
    val tweetVectors = generateWord2VecVectors(filteredTweets).persist()
    val tweetVectorsWithPCA = doPrincipleComponentAnalysis(tweetVectors)
    val (kMeansModel, kMeansPredictions) = generateKMeansPredictions(tweetVectorsWithPCA)

    //    writeOutToFile(kMeansPredictions)
    val infoDataFrame = kMeansPredictions.select("pca-features", "kmeans-prediction")
    infoDataFrame.withColumn("pca-features", stringify($"pca-features")).write.mode("overwrite").csv("pca_info.csv")

    val (clusterInfoList, silhouette) = clusterResults(kMeansModel, kMeansPredictions)

    // Turn off info logging before printing results
    spark.sparkContext.setLogLevel("WARN")
    printResults(clusterInfoList, silhouette)
    spark.stop()
  }

  def readTweets(lines: RDD[String]): RDD[String] = {
    lines map { line =>
      val sections = line.split(',').drop(5)
      val tweetString = sections.reduce((a, b) =>  a + "," + b)
      tweetString.stripPrefix("\"").stripSuffix("\"").trim
    }
  }

  def filterStopWords(tweets: DataFrame, stopWords: DataFrame): DataFrame = {
    val tokenizer: Tokenizer = new Tokenizer()
      .setInputCol("tweets")
      .setOutputCol("tweets-tokens")

    val remover = new StopWordsRemover()
      .setStopWords(stopWords.select("stopwords").collect().map(_.getString(0)))
      .setInputCol("tweets-tokens")
      .setOutputCol("filtered-tweets")

    val tokenized: DataFrame = tokenizer.transform(tweets)
    val filtered: DataFrame = remover.transform(tokenized)

    filtered
  }


  def generateWord2VecVectors(filteredTokenizedTweets: DataFrame): DataFrame = {
    val word2Vec = new Word2Vec()
      .setInputCol("filtered-tweets")
      .setOutputCol("tweet-vector")
      .setVectorSize(VECTOR_SIZE)
      .setMinCount(WORD_2_VEC_MIN_COUNT)

    val model = word2Vec.fit(filteredTokenizedTweets)
    val tweetVectors = model.transform(filteredTokenizedTweets)

    tweetVectors
  }


  def generateKMeansPredictions(tweetVectors: DataFrame): (KMeansModel, DataFrame) = {
    val kMeans = new KMeans()
      .setK(K)
      .setFeaturesCol("tweet-vector")
      .setPredictionCol("kmeans-prediction")

    val kMeansModel = kMeans.fit(tweetVectors)
    val kMeansPredictions = kMeansModel.transform(tweetVectors)
    (kMeansModel, kMeansPredictions)
  }


  def clusterResults(kMeansModel: KMeansModel, kMeansPredictions: DataFrame): (Array[(Int, Long, Array[Double], RDD[(String, Int)])], Double) = {
    val clusterCenters = kMeansModel.clusterCenters
    val clusterIDs = Array.range(0, K)

    val clusterInfoList = clusterIDs map { id =>
      val clusterCenter = clusterCenters(id)
      val clusterRows = kMeansPredictions.select("*")
        .filter(kMeansPredictions("kmeans-prediction") === id)

      // Get all tokens from all tweets in a cluster
      val tokens = clusterRows.select("filtered-tweets").rdd
        .map { case (r: Row) => r.getList(0).toArray}
        .flatMap(list => list)

      val tokenAndFreq = tokens.groupBy(identity) map {
        case (token: String, arr: Iterable[AnyRef]) => (token, arr.size)
      } filter(!_._1.isEmpty)

      val tokenSortedByFreq = tokenAndFreq.sortBy(-_._2)

      (id, clusterRows.count(), clusterCenter.toArray, tokenSortedByFreq)
    }

    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("tweet-vector")
      .setPredictionCol("kmeans-prediction")
    val silhouette = evaluator.evaluate(kMeansPredictions)

    (clusterInfoList, silhouette)
  }

  def doPrincipleComponentAnalysis(tweetsVectors: DataFrame): DataFrame = {
    val pca = new PCA()
      .setInputCol("tweet-vector")
      .setOutputCol("pca-features")
      .setK(2)

    val pcaModel = pca.fit(tweetsVectors)
    val pcaDataFrame = pcaModel.transform(tweetsVectors)
//    pcaDataFrame.select("pca-features").show(20, false)

    pcaDataFrame
  }



  val stringify: UserDefinedFunction = udf((vs: DenseVector) => vs match {
    case null => null
    case _    => s"""[${vs.toArray.mkString(",")}]"""
  })

//  def writeOutToFile(clusteredTweetsVectors: DataFrame): Unit = {
//    val infoDataFrame = clusteredTweetsVectors.select("pca-features", "kmeans-prediction")
//    infoDataFrame.withColumn("pca-features", stringify($"pca-features")).write.csv("pca_info.csv")
//  }


  /** Pad string with spaces upto specified length */
  def padStr(string: String, length: Int): String = {
    string.padTo(length, ' ')
  }


  def printResults(clusterInfoList: Array[(Int, Long, Array[Double], RDD[(String, Int)])], silhouette: Double) = {
    println(s"Silhouette = $silhouette\n")
    clusterInfoList foreach {
      case (clusterID: Int, clusterSize: Long, clusterCenter: Array[Double], tokenSortedByFreq: RDD[(String, Int)]) =>
        val topFiveTokens = tokenSortedByFreq.take(5)
        println(s"====== Results for Cluster $clusterID ======")
        println(s"Cluster Center: ${clusterCenter.mkString("(", ", ", ")")}")
        println(s"Cluster Size: $clusterSize")
        println("Top 5 tokens: ")
        println(padStr("Token", 15) + "Frequency")
        topFiveTokens foreach {
          case (token: String, freq: Int) => println(padStr(s"$token", 15) + s"$freq")
        }
        println("======================\n")
    }

    println(s"K: $K")
    println(s"VECTOR_SIZE: $VECTOR_SIZE")
    println(s"WORD_2_VEC_MIN_COUNT: $WORD_2_VEC_MIN_COUNT")
  }

}