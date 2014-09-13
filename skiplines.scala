import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object skiplines {
  def map1(line:String):(String) = {
    
    
    var key = new String
    var value = new String

    if(line.charAt(0)=='#')
    key = ""
    else
    key = line

    (key)
  }


  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "Skiplines",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = 2
    val file=spark.textFile(args(1))

    val mout=file.map(
      line => map1(line)
       )
    
    
    mout.saveAsTextFile(args(2));
    System.exit(0)
}
}