import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object finalkeyvalue {
  def map1(line:String):(String,String) = {
   
    
    var key = new String
    var value = new String
    
    try{
      var ei=line.indexOf(",")
      if(ei>0){
      key = line.substring(1,ei).replaceAll("_"," ")

      value = line.substring(ei+1,line.length()-1).replaceAll("_"," ")
    }
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
    
    val file1=spark.textFile("out_1")
    val mout1=file1.map(
      line => map1(line)
       )

    val file2=spark.textFile("out_2")
    val mout2=file2.map(
      line => map1(line)
       )

    val file3=spark.textFile("out_3")
    val mout3=file3.map(
      line => map1(line)

       )

    val file4=spark.textFile("out_4")
    val mout4=file4.map(
      line => map1(line)
       )

    val file5=spark.textFile("out_5")
    val mout5=file5.map(
      line => map1(line)
       )

    val out = mout1.union(mout2).union(mout3).union(mout4).union(mout5).reduceByKey(_+"\t"+_)



    //val rout = mout.reduceByKey(_+","+_)
      
    out.saveAsTextFile(args(1));

   

    System.exit(0)
}
}