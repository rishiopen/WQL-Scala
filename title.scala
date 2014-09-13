import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object title {
  def map1(line:String):(String,String) = {
    //val pat1 = new Regex("resource/(\\w+)>")
    
    
    var key = new String
    var value = new String
    var arr = line.split(" ")
    

    var si=arr(0).indexOf("resource/")
    var ei=arr(0).lastIndexOf('>')
      key = arr(0).substring(si+9,ei).replaceAll("_"," ")
    (key,value)
  }


  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "Category",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = if (args.length > 1) args(1).toInt else 2
    val file=spark.textFile("title.txt")

    val mout=file.map(
      line => map1(line)
       ).cache()
    
      
    mout.saveAsTextFile("out_title");
    System.exit(0)
}
}