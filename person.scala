import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object person {
  def map1(line:String):(String,String) = {
    //val pat = new Regex("resource/(\\w+)")
    val category1 = new Regex("foaf/0.1/(\\w+)")
    val category2 = new Regex("elements/1.1/(\\w+)")
    val category3 = new Regex("#(\\w+)>")
    val category4 = new Regex("ontology/(\\w+)>")

       
    
    var key = new String
    var value = new String
    var val1 = new String
    var val2 = new String
    var val3 = new String
    var val4 = new String    
    try{
    val arr = line.split(" ")
    if(!arr(0).equals(""))
    {
      var si=arr(0).indexOf("resource/")
      var ei=arr(0).lastIndexOf('>')
      key = arr(0).substring(si+9,ei).replaceAll("_"," ")
     // key = key.replaceAll(";",",")
    }

    //var Some(k) = pat findFirstMatchIn line
    
    (category1 findAllIn line).matchData foreach(m => val1=(m.subgroups mkString ""))
    (category2 findAllIn line).matchData foreach(m => val2=(m.subgroups mkString ""))
    (category3 findAllIn line).matchData foreach(m => val3=(m.subgroups mkString ""))
    (category4 findAllIn line).matchData foreach(m => val4=(m.subgroups mkString ""))
    
    

    if(val1.toString.equals("name")||val1.toString.equals("surname")||val1.toString.equals("givenName"))
    {
    val si=line.indexOf('"')
    val ei=line.lastIndexOf('"')
    val temp= line.substring(si+1,ei)
    value= val1 + ":"+temp
    }
    else
    if(val2.toString.equals("description"))
    { 
      val si=line.indexOf('"')
      val ei=line.lastIndexOf('"')
      val temp= line.substring(si+1,ei)
      value= val2+":"+ temp
    }
    else
    if(val3.toString.equals("type"))
    {
      value= val3+":"+val1
    } 
    else
    if(val4.toString.equals("birthPlace")||val4.toString.equals("deathPlace"))
    {
      val si=arr(2).lastIndexOf('/')
      val ei=arr(2).indexOf('>')
      value = val4+":"+arr(2).substring(si+1,ei)
    }
    else
    if(val4.toString.equals("birthDate")||val4.toString.equals("deathDate"))
    {
      val si=arr(2).indexOf('"')
      val ei=arr(2).indexOf('"',si+1)
      value = val4+":"+arr(2).substring(si+1,ei)
    }
    //if(!k.toString().equals(""))
    //key=k.toString().substring(9);
    value = value.replaceAll("_"," ")

    key = key.replaceAll(",",";")
    value = value.replaceAll(",",";")
  }
    catch{
    case e: Exception => println("**** "+ line  + "****")
  }


    //value = value.replaceAll(";",",")

    
    (key,value)
  }


  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "Category",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices =  2
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