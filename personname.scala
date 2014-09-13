import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object personname {
  def map1(line:String):(String,String) = {
   
    
    var key = new String
    var value = new String


    val arr = line.split(" ")
    
    if(arr.length>1)
    {
      var si=arr(0).indexOf("resource/")
      var ei=arr(0).lastIndexOf('>')
      key = arr(0).substring(si+9,ei).replaceAll("_"," ")
    }
    key=key.replaceAll("_"," ")
    
    (key,value)
  }


  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "Category",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = 2
    val file=spark.textFile(args(1))

    val mout=file.map(
      line => map1(line)
       ).cache()
    val rout = mout.reduceByKey(_+_)
      
    rout.saveAsTextFile(args(2));
    System.exit(0)
}
}