import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

import scala.util.Random

object FindRectanglesInGrid {
  case class Point(x: Int, y: Int)
  val slope: (Point, Point) => Double = (p1, p2) => (p2.y - p1.y).toDouble / (p2.x - p1.x)
  val dist: (Point, Point) => Double = (p1, p2) => Math.pow(p2.x - p1.x, 2) + Math.pow(p2.y - p1.y, 2)
  val EPSILON = 1e-10
  val spark: SparkSession = SparkSession
    .builder()
    .appName("find-rectangles-in-grid")
    .master("local[6]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  def timeIt[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result: R = block
    val t1 = System.nanoTime()
    println(f"Elapsed time: ${(t1 - t0)/1000000000d}%.2f s")
    result
  }

  def main(args: Array[String]): Unit = {
    val n: Int = 5000
    Random.setSeed(0)
    val nextPoint: Unit => Int = (_ => (Random.nextDouble() * 1000).round.toInt)
    val points: RDD[Point] = spark.sparkContext
        .parallelize(Seq.fill(n) {
          (nextPoint(), nextPoint())
        })
          .map{ case (x: Int, y: Int) => Point(x, y) }
//    val points: RDD[Point] = spark.sparkContext.parallelize(Seq(
//      Point(0, 0), Point(1, 1), Point(-1, 1), Point(0, 2), Point(1, 3), Point(2, 2)
//    ))
    val resultA: Long = timeIt {
      solnA(points)
    }
    println(s"# of rectangles: ${resultA}")
//    val resultB: Long = timeIt {
//      solnB(points)
//    }
//    println(s"# of rectangles: ${resultB}")
    val resultsC: Long = timeIt {
      solnC(points)
    }
    print(s"# of rectangles: ${resultsC}")
  }


  def solnA(points: RDD[Point]): Long = {
    points.cache()
    val lines: RDD[(Double, (Point, Point, Double))] = points
      .cartesian(points)
      .filter { case (p1: Point, p2: Point) => dist(p1, p2) > EPSILON }
      .map { case (p1: Point, p2: Point) => (p1, p2, slope(p1, p2)) }  // calculate slope
      .filter { case (p1: Point, p2: Point, slope: Double) => slope > 0 && p1.x < p2.x }  // remove possible repetitions in data
      .map { case (p1: Point, p2: Point, m: Double) => (m, (p1, p2, dist(p1, p2))) }
    lines.partitionBy(new HashPartitioner(100))

    val rects = lines.join(lines)
      .filter { case (m: Double, ((p1: Point, p2: Point, dist1: Double), (p3: Point, p4: Point, dist2: Double))) =>
        (Math.abs(dist1 - dist2) < EPSILON) &&
          (p1.y < p3.y) &&
          (slope(p1, p3) + m < EPSILON) &&
          (dist(p1, p3) > EPSILON)
      }
//    rects.collect().foreach(println)
    return rects.count()
  }

  def solnB(points: RDD[Point]): Long = {
    points.cache()
    val lines: RDD[(Int, (Point, Point, Double))] = points
      .cartesian(points)
      .filter { case (p1: Point, p2: Point) => dist(p1, p2) > EPSILON }
      .map { case (p1: Point, p2: Point) => (p1, p2, slope(p1, p2)) }  // calculate slope
      .filter { case (p1: Point, p2: Point, slope: Double) => p1.x < p2.x }  // remove possible repetitions in data
      .map { case (p1: Point, p2: Point, m: Double) => ((p1.x + p1.y) * (p1.x - p1.y) * p1.x * p2.y, (p1, p2, m)) }
    val pointsSet: Set[Point] = points.collect().toSet
    val linesA = lines.filter { case (_, (_, _, m)) => m > 0 }
    val linesB = lines.filter { case (_, (_, _, m)) => m < 0 }
    val searchPoints = linesA.join(linesB)
      .filter { case (_, ((p1a, p2a, ma), (p1b, p2b, mb))) => ma + mb <= EPSILON && p1a == p1b }
      .map { case (_, ((p1, p2a, _), (_, p2b, _))) => Point(p2a.x + p2b.x - p1.x, p2a.y + p2b.y - p1.y) }
    searchPoints.collect().count(pointsSet.contains).toLong
  }

  def solnC(points: RDD[Point]): Long = {
    val df: DataFrame = points.toDF("x1", "y1")
    val lines: DataFrame = df.crossJoin(df.toDF("x2", "y2"))
      .filter(not('x1 == 'x2 && 'y1 == 'y2))
      .filter(($"x1" < $"x2") || ('x1 = 'x2 && 'y1 < 'y2))
      .withColumn("length", pow($"x1" - $"x2", 2.0) + pow($"y1" - $"y2", 2.0))
      .withColumn("x", ($"x1" + $"x2") / 2.0)
      .withColumn("y", ($"y1" + $"y2") / 2.0)
    lines.groupBy("x", "y", "length")
      .count()
      .select($"count")
      .collect()
      .map { row: Row => row.getAs[Long](0) * (row.getAs[Long](0)-1) / 2 }
      .sum
  }
}
