import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object infoproperty {
  def map1(line:String):(String,String) = {
    //val pat = new Regex("resource/(\\w+)")
    //val category1 = new Regex("ontology/(\\w+)")
    val name = new Regex("foaf/0.1/(\\w+)")
       
    
    var key = new String
    var value = new String
    var Name = new String
    
    


    try {

    val arr = line.split(" ")

    if(arr.length>1)
    {
      var si=arr(0).indexOf("resource/")
      var ei=arr(0).lastIndexOf('>')
      key = arr(0).substring(si+9,ei).replaceAll("_"," ")
     
    }
    
    
    (name findAllIn line).matchData foreach(m => Name=(m.subgroups mkString ""))  
    
    //println(Name.toString)
    if(Name.toString.equals("name"))            // Name
    {
    var si=line.indexOf('"')
    var ei=line.lastIndexOf('"')
    var temp= line.substring(si+1,ei)
    value= Name+":"+temp
    }
    else
    if(Name.toString.equals("homepage"))            // homepage
    {
      //println("---------------")
      value = Name + ":"+ arr(2)
    }
    else
    if(arr(1).indexOf('#')>=0)                  // Type,Lat,Long
    {
    var si=arr(1).indexOf('#')
    var ei=arr(1).indexOf('>',si+1)
    var temp = arr(1).substring(si+1,ei)
    if(line.indexOf('"')>0)
    {
    si = line.indexOf('"')
    ei = line.lastIndexOf('"')
    value = temp +":"+ line.substring(si+1,ei)
    }
    else
    {
    si=arr(2).lastIndexOf('/')
    ei=arr(2).indexOf('>')
    value = temp +":"+arr(2).substring(si+1,ei)
    }
    }
    else                                          // Ontology
    {
    var si=arr(1).lastIndexOf('/')
    var ei=arr(1).indexOf('>')
    var ontology = arr(1).substring(si+1,ei)

    if(arr(2).indexOf("resource")>0)              // With resource tag
    { 
      si=arr(2).lastIndexOf('/')
      ei=arr(2).indexOf('>')
      var temp= arr(2).substring(si+1,ei)
      value= ontology+":"+ temp
    }
    else                                                    // With some string
    { 
      //println(ontology)
      si=line.indexOf('"')
      ei=line.lastIndexOf('"')
      var temp= line.substring(si+1,ei)
      value= ontology+":"+ temp
      //println(value)
      }
    }
    
    

  
    value=value.replaceAll("_"," ")

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

    //val rout = mout.reduceByKey(_+";"+_)

      
    rout.saveAsTextFile(args(2));
    System.exit(0)
}
}