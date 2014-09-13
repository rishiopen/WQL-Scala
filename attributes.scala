import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object attributes {
  def map1(line:String):(String) = {
   
    
    var key = new String
    var value = new String
    
    try{
      var si=line.indexOf(',')
      var temp = line.substring(si+1,line.length()-1)
      val arr = temp.split("\t")
      for (i <- 0 until arr.length){
        if(arr(i).indexOf(':')>0){
          value = arr(i).substring(0,arr(i).indexOf(':'))
          key = value + "\n"        
		}      
       }
     }
    catch{
    case e: Exception => println("**** "+ line  + "****")
  }

    (key)
  }
  def map2(line:String):(String,String) = {


    (line,"")

  }
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "Category",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = 2
    
    val file=spark.textFile(args(1))
    val mout1=file.map(
      line => map1(line)
       )
    val mout2=mout1.map(line => map2(line))
    val rout = mout2.reduceByKey(_+_)      
    rout.saveAsTextFile(args(2));
    System.exit(0)
}
}