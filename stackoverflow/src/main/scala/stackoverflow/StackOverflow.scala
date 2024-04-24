package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Logger, Level}

import annotation.tailrec
import scala.reflect.ClassTag
import scala.util.Properties.isWin
import scala.io.Source
import scala.io.Codec

object Aliases:
  type Question = Posting
  type Answer = Posting
  type QID = Int
  type HighScore = Int
  type LangIndex = Int
import Aliases.*

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object StackOverflow extends StackOverflow:

  // Reduce Spark logging verbosity
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit =
    val inputFileLocation: String = "/stackoverflow/stackoverflow.csv"
    val resource = getClass.getResourceAsStream(inputFileLocation)
    val inputFile = Source.fromInputStream(resource)(Codec.UTF8)
 
    val lines   = sc.parallelize(inputFile.getLines().toList)
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)//.sample(true, 0.1, 0)
    val vectors = vectorPostings(scored)
//    assert(vectors.count() == 1042132, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable:

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if arr(2) == "" then None else Some(arr(2).toInt),
              parentId =       if arr(3) == "" then None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if arr.length >= 6 then Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */

  /**
    * Filter the postings with postingType = 1, which means that those are questions, and map them to a pair
    * (QID, Question)
    * Filter the postings with postingType = 2, which means that those are answers, and also filter out the answers that
    * don't have a parentId. Then map them to a pair (QID, Answer). Calling get on the parentId won't throw an exception
    * because the parentIds that are None have been filtered out
    * Perform a join between the questions and answers by the QIDs. The condition basically is
    * question.id = answer.parentId. We use an inner join since we don't care about questions with no answers
    * Group questions and answers by the QID. We basically get pairs of questions and all their respective answers
    */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] =
    val questions = postings.filter(_.postingType == 1).map(posting => (posting.id, posting)).asInstanceOf[RDD[(QID, Question)]]
    val answers = postings.filter(posting =>
      posting.postingType == 2
      && (posting.parentId match {
        case Some(id) => true
        case None => false
      })
    ).map(posting => (posting.parentId.get, posting)).asInstanceOf[RDD[(QID, Answer)]]
    val joined = questions.join(answers)
    joined.groupByKey


  /** Compute the maximum score for each posting */

  /**
    * First, we need to transform the pairs from (QID, Iterable[(Question, Answer)]) to (QID, Array[Answer])
    * By using mapValues on "grouped" we are mapping each iterable and by using map on each iterable we are mapping each
    * pair (Question, Answer) to just the answer, and then we convert the iterable of answers to an array, thus getting
    * Iterable[(Question, Answer)] -> Array[Answer]
    * Then we map each Array[Answer] to the highest score from those answers
    * Now, since we have two pair RDDs with QID as key, we perform a join between the grouped questions and answers and
    * the high scores. We get an RDD of pairs where the keys are QIDs and the values are pairs themselves like
    * (Iterable[(Question, Answer)], HighScore)
    * We want to get and RDD of pairs (Question, HighScore) so we do more mapping. We get the new keys from the
    * iterables of questions and answers. We can simply get the question from the first QA pair since we know that the
    * questions are all the same within each iterable. The values become high scores
    */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] =

    def answerHighScore(as: Array[Answer]): HighScore =
      var highScore = 0
      var i = 0
      while i < as.length do
        val score = as(i).score
        if score > highScore then
          highScore = score
        i += 1
      highScore

    val groupedAnswers = grouped.mapValues(questionsAndAnswers => {
      val answers = questionsAndAnswers.map{ case (question, answer) => answer}
      answers.toArray
    })
    val idsWithHighScores = groupedAnswers.mapValues(answerHighScore)
    val joined = grouped.join(idsWithHighScores)
    val questionsAndScore = joined.map{ case (id, (questionsAndAnswers, highScore)) =>
      (questionsAndAnswers.head._1, highScore)
    }
    questionsAndScore


  /** Compute the vectors for the kmeans */

  /**
    * First we filter out the questions that don't have a tag indicating the language
    * Then we map the remaining pairs that do have a language. From the questions we get their indexes and multiply them
    * by the langSpread. The results become the keys. Again, calling get won't throw an error since the questions
    * without languages have been dropped. The high scores are still the values
    */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] =
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] =
      tag match
        case None => None
        case Some(lang) =>
          val index = ls.indexOf(lang)
          if (index >= 0) Some(index) else None

    val filteredQuestions = scored.filter{ case (question, highScore) =>
      firstLangInTag(question.tags, langs) match {
        case Some(value) => true
        case None => false
      }
    }
    val scoredIndexes = filteredQuestions.map{ case (question, highScore) =>
      val spreadIndex = firstLangInTag(question.tags, langs).get * langSpread
      (spreadIndex, highScore)
    }
    scoredIndexes


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] =

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] =
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for i <- 0 until size do
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next()

      var i = size.toLong
      while iter.hasNext do
        val elt = iter.next()
        val j = math.abs(rnd.nextLong()) % i
        if j < size then
          res(j.toInt) = elt
        i += 1

      res

    val res =
      if langSpread < 500 then
        // sample the space regardless of the language
        vectors.distinct.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey().flatMap(
          (lang, vectors) => reservoirSampling(lang, vectors.iterator.distinct, perLang).map((lang, _))
        ).collect()

    assert(res.length == kmeansKernels, res.length)
    res


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] =
    // TODO: Compute the groups of points that are the closest to each mean,
    // and then compute the new means of each group of points. Finally, compute
    // a Map that associate the old `means` values to their new values

    // For each vector, find the mean that it belongs to and associate in a pair said mean with the vector
    val meansToVectors = vectors.map(vector => (findClosest(vector, means), vector))
    // Group the vectors, so that you have a mean and the vectors that belong in it
    val groupedVectorsByMean = meansToVectors.groupByKey
    // Then, simply map the vectors of a mean to the new mean. You get a map like oldMean -> newMean
    val newMeansMap: scala.collection.Map[(Int, Int), (Int, Int)] = groupedVectorsByMean.mapValues(averageVectors)
      .collect.toMap
    val newMeans: Array[(Int, Int)] = means.map(oldMean => newMeansMap(oldMean))
    val distance = euclideanDistance(means, newMeans)

    if debug then
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for idx <- 0 until kmeansKernels do
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")

    if converged(distance) then
      newMeans
    else if iter < kmeansMaxIterations then
      kmeans(newMeans, vectors, iter + 1, debug)
    else
      if debug then
        println("Reached max iterations!")
      newMeans




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double =
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double =
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while idx < a1.length do
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    sum

  /** Return the center that is the closest to `p` */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): (Int, Int) =
    var bestCenter: (Int, Int) = null
    var closest = Double.PositiveInfinity
    for center <- centers do
      val tempDist = euclideanDistance(p, center)
      if tempDist < closest then
        closest = tempDist
        bestCenter = center
    bestCenter


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) =
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while iter.hasNext do
      val item = iter.next()
      comp1 += item._1
      comp2 += item._2
      count += 1
    ((comp1 / count).toInt, (comp2 / count).toInt)




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] =
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs => // vs represents a cluster. So, for each cluster do the following:
      // Group by the index, you get Map[LangIndex, (LangIndex, HighScore)]
      // Map the values to a 1, you get Map[LangIndex, 1]
      // Sum the values, you get a map like language -> occurrences of said language
      val indexToOccurrences = vs.groupMapReduce{ case (index, highScore) => index }(_ => 1)(_+_)
      // Get the most common language measured by its number of occurrences
      val mostCommonLanguage = indexToOccurrences.maxBy(_._2)
      // Get the index of the most common language. Divide by the langSpread to get the original index
      val indexOfMostCommonLanguage = mostCommonLanguage._1 / langSpread
      // Get the number of occurrences of the most common language
      val occurrencesOfMostCommonLanguage = mostCommonLanguage._2
      // Sum the occurrences of each language to get the total occurrences. Use foldLeft so that the return value is not
      // required to be a pair as the original members of the map
      val totalOccurrences = indexToOccurrences.foldLeft(0){ case (acc, (index, occurrences)) => acc + occurrences }
      // Map to get an Iterable[HighScore] then convert it to a sorted array
      val highScores = vs.map{case (index, highScore) => highScore}.toArray.sorted
      // Get the median of all the high scores
      val medScore = if (highScores.length % 2 == 0) {
        val secondIndex = highScores.length / 2
        val firstIndex = secondIndex - 1
        (highScores(firstIndex) + highScores(secondIndex)) / 2
      } else {
        val index = highScores.length / 2
        highScores(index)
      }

      val langLabel: String   = langs(indexOfMostCommonLanguage) // most common language in the cluster
      val langPercent: Double = (occurrencesOfMostCommonLanguage / totalOccurrences) * 100 // percent of the questions in the most common language
      val clusterSize: Int    = vs.size
      val medianScore: Int    = medScore

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)

  def printResults(results: Array[(String, Double, Int, Int)]): Unit =
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for (lang, percent, size, score) <- results do
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")