import scala.math.random
import spark._
import SparkContext._

object wc {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "SparkPi",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = if (args.length > 1) args(1).toInt else 2
    val file=spark.textFile("env")
    val counts=file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    counts.saveAsTextFile("Output");
    System.exit(0)
}
}