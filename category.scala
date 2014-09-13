import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object category {
  def map1(line:String):(String,String) = {
    
    
    var key = new String
    var value = new String
try{
    val arr = line.split(" ")
    if(arr.length>1)
    {
      var si=arr(0).indexOf("resource/")
      var ei=arr(0).lastIndexOf('>')
      key = arr(0).substring(si+9,ei).replaceAll("_"," ")
      si=arr(2).lastIndexOf('/')
      ei=arr(2).indexOf('>')
      value = arr(2).substring(si+1,ei).replaceAll("_"," ")


    }

    key = key.replaceAll(",",";")
    value = value.replaceAll(",",";")
    }
    catch{
    case e: Exception => println("**** "+ line  + "****")
  }




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

    val rout = mout.reduceByKey(_+"\t"+_)

    

      
    rout.saveAsTextFile(args(2));
    System.exit(0)
}
}