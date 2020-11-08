import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object Assignment2 extends Assignment2 {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Assignment2")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  sc.setLogLevel("WARN")

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile(args(0)) // "src/QA_data.csv"
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored).persist()
    val initialSample = sampleVectors(vectors)
    val means = kmeans(initialSample, vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class Assignment2 extends Serializable {

  /** Languages */
  val Domains =
    List(
      "Machine-Learning", "Compute-Science", "Algorithm", "Big-Data", "Data-Analysis", "Security", "Silicon Valley", "Computer-Systems",
      "Deep-learning", "Internet-Service-Providers", "Programming-Language", "Cloud-services", "Software-Engineering", "Embedded-System", "Architecture")


  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def DomainSpread = 50000
  assert(DomainSpread > 0)

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria, if changes of all centriods < kmeansEta, stop*/
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              parentId =       if (arr(2) == "") None else Some(arr(2).toInt),
              score =          arr(3).toInt,
              tags =           if (arr.length >= 5) Some(arr(4).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
    // Extract questions
    val questions = postings
      .filter(_.postingType == 1)
      .map(p => (p.id, p))

    // Extract answers
    val answers = postings
      .filter(_.postingType == 2)
      .filter(_.parentId.isDefined)
      .map(p => (p.parentId, p))

    // Convert  RDD[(Option[Int], Posting)] to  RDD[(Int, Posting)]
    val unwrappedAnswers = answers.map {
      case (Some(parentId), answer) => (parentId, answer)
    }

    // Join questions and answers
    val questionsAndAnswers = questions.join(unwrappedAnswers)

    // Group by Question ID
    questionsAndAnswers.groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(groupedPostings: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {
    val questionAndAnswers: RDD[(Posting, Iterable[Posting])] = groupedPostings.flatMap(x => x._2).groupByKey()
    val questionAndMaxScore = questionAndAnswers map {
      case (question: Posting, answers: Iterable[Posting]) => (question, answers.map(_.score).max)
    }

    questionAndMaxScore
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scoredPostings: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
    scoredPostings map {
      case (question: Posting, maxScore: Int) =>
        val domainIndex: Int = question.tags match {
          case Some(tag: String) => Domains.indexOf(tag) + 1
          case _ => -1
        }
        (domainIndex * DomainSpread, maxScore)
    } filter {
      case (domainValue: Int, _: Int) => domainValue > 0
    }
  }

  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {
    vectors.takeSample(withReplacement = false, kmeansKernels)
  }


  /** Main kmeans computation */
  @tailrec final def kmeans(centers: Array[(Int, Int)], vectors: RDD[(Int, Int)], debug: Boolean = false, currIter: Int = 1): Array[(Int, Int)] = {
    // Cluster points
    val clusters = vectors.map(v => (findClosest(v, centers), v)).groupByKey()
    val indexAndNewCenters = clusters map {
      case (index: Int, points: Iterable[(Int, Int)]) => (index, averageVectors(points))
    }

    // Update centers of the new clusters
    val newCenters = centers.clone()

    indexAndNewCenters.collect().foreach {
      case (index: Int, newCenter: (Int, Int)) => newCenters.update(index, newCenter)
    }

    // Recurse or terminate
    val distanceFromOldCenters = euclideanDistance(centers, newCenters)
    val reachedConvergence = converged(distanceFromOldCenters)
    val reachedMaxIterations = currIter >= kmeansMaxIterations

    if (debug) printDebug(currIter, distanceFromOldCenters, newCenters, centers, reachedConvergence, reachedMaxIterations)

    if (reachedConvergence || reachedMaxIterations) {
      return newCenters
    }

    kmeans(newCenters, vectors, debug, currIter + 1)
  }


  def clusterResults(centers: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[((Int, Int), Int, Int, Int)] = {
    val clusters = vectors.map(v => (findClosest(v, centers), v)).groupByKey().collect()

    val clusterInfo = clusters map {
      case (centerIndex: Int, points: Iterable[(Int, Int)]) =>
        val center = centers(centerIndex)
        val size = points.size
        val medianScore = computeMedian(points)
        val averageScore = points.map(_._2).sum / points.size
        (center, size, medianScore, averageScore)
    }

    clusterInfo
  }


  def printResults(clusterInfo: Array[((Int, Int), Int, Int, Int)]): Unit = {
    println(
      padStr("Centroid", 15) +
      padStr("Cluster Size", 15) +
      padStr("Median Score", 15) +
      padStr("Average Score", 15)
    )

    clusterInfo.sorted foreach {
      case (center: (Int, Int), size: Int, median: Int, average: Int) =>
        println(
          padStr(s"$center", 15) +
          padStr(s"$size", 15) +
          padStr(s"$median", 15) +
          padStr(s"$average", 15)
        )
    }
  }


  /** Pad string with spaces upto specified length */
  def padStr(string: String, length: Int): String = {
    string.padTo(length, ' ')
  }

  /** Print debug information */
  def printDebug(currIter: Int, currDistance: Double, currCenters: Array[(Int, Int)], oldCenters: Array[(Int, Int)], reachedConvergence: Boolean, reachedMaxIterations: Boolean) = {
    println(padStr("Current Iteration: ", 30) + s"$currIter")
    println(padStr("Current Distance: ", 30) + s"$currDistance")
    println(padStr("Reached Convergence: ", 30) + s"$reachedConvergence")
    println(padStr("Reached Max Iterations: ", 30) + s"$reachedMaxIterations")
    println(padStr("Min Distance to terminate: ", 30) + s"$kmeansEta")
    println("Center Updates:")

    for (i <- 0 until kmeansKernels) {
      val oldCenter = oldCenters(i)
      val newCenter = currCenters(i)
      val distanceChanged = euclideanDistance(newCenter, oldCenter)
      println(padStr(s"\t$oldCenter", 15) +
        " to" +
        padStr(s"\t$newCenter", 15) +
        s"\tDistance Updated: $distanceChanged"
      )
    }
  }

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) = distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  def computeMedian(a: Iterable[(Int, Int)]) = {
    val s = a.map(x => x._2).toArray
    val length = s.length
    val (lower, upper) = s.sortWith(_<_).splitAt(length / 2)
    if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }
}
