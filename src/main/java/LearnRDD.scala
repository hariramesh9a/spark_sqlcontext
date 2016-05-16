import org.apache.spark
import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import org.apache.spark.SparkConf

object LearnRDD {

  def main(args: Array[String]): Unit = {
    
    //var configMap:Map[String,String]=Map("FileName"->"src/main/resources/hello.txt","OutputFileName"-> "src/main/resources/output.csv")

    //basics()
    filterandcount()
  }
  
  def basics(): Unit = {
    val sparkcon = new SparkContext("local[8]", "Learn RDD")
    var mydata = sparkcon.textFile("src/main/resources/employees.csv")
    val neededline = mydata.filter { line => line.contains("Shi") }
    val words = neededline.flatMap { x => x.split(",") }.filter { y => y.contains("Shi") }
    val count = words.map(x => (x, 1)).reduceByKey { case (a, b) => a + b }
    count.coalesce(1).saveAsTextFile("src/main/resources/output")    
  }
  
  
  def filterandcount(): Unit = {
//    using error log in resources
    val conf=new SparkConf().setAppName("Filter and Count").setMaster("local[8]")
    val sparkcon=new SparkContext(conf)
    val errorlog=sparkcon.textFile("src/main/resources/hello.txt")
    val splitRDD=errorlog.flatMap { x => x.split(":")}
    println(splitRDD.first().contains("error"))

    val words=splitRDD.filter { x => x.contains("error") }
    val countwords=words.map { (x) => (x,1) }
    val counts=countwords.reduceByKey({case(x,y)=>(x+y)}).foreach(println)
    
    //println("Number of errors: "+onlyerror.count())
    //now get key and values
  
    
    
  }

}



