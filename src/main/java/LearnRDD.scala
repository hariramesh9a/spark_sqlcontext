import org.apache.spark
import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.hive.jdbc.HiveDriver

object LearnRDD {

  def main(args: Array[String]): Unit = {

    //var configMap:Map[String,String]=Map("FileName"->"src/main/resources/hello.txt","OutputFileName"-> "src/main/resources/output.csv")
    //Uncomment below comments one by one to see the results.
    //basics()
    //filterandcount()
    //findMaxCity()
    findCityandCountandPersistHiveRemotely()
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
    val conf = new SparkConf().setAppName("Filter and Count").setMaster("local[8]").set("spark.cores.max", "10")
    val sparkcon = new SparkContext(conf)
    val errorlog = sparkcon.textFile("src/main/resources/hello.txt")
    val errrlog = errorlog.filter { line => line.split(":")(0).contains("error") }
    val splitRDD = errrlog.map { x => x.split(":")(0) + ':' + x.split(":")(2) }
    val countwords = splitRDD.map { (x) => (x, 1) }
    var nl = System.getProperty("line.separator")
    val counts = countwords.reduceByKey({ case (x, y) => (x + y) }).foreach(println)
    println("Hari" + nl + "Hari")
    //println("Number of errors: "+onlyerror.count())
    //now get key and values
  }

  //find most players born city just using RDD
  def findMaxCity(): Unit = {
    val conf = new SparkConf().setAppName("Player Cities").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val mydata = sc.textFile("src/main/resources/Master.csv")
    println(mydata.map(line => line.split(",")(6)).map { x => (x, 1) }.reduceByKey({ case (x, y) => x + y }).sortBy(f => f._2, false).first())
  }

  //eof most players born city

  //Use spark and persisit to hive
  def findCityandCountandPersistHiveRemotely(): Unit = {
    val conf = new SparkConf().setAppName("Save result to hive remotely").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val mydata = sc.textFile("src/main/resources/Master.csv")
    val myRDD = mydata.map { mycity => mycity.split(",")(6) }.map { x => (x, 1) }.reduceByKey({ case (x, y) => x + y }).sortBy(f => f._2, true).sample(false, .001)
    var drivername: String = "org.apache.hadoop.hive.jdbc.HiveDriver"
    myRDD.foreachPartition {
      mydata=>
     var con: Connection = DriverManager.getConnection("jdbc:hive2://ehplbigi1002.nwie.net:2181,ehplbigi1003.nwie.net:2181,ehplbigi1005.nwie.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2", "ramesh2", "Ilovespark007")
     var stmt=con.createStatement()
      var tableName: String = "baseballplayers"
      for (eachrow <-mydata)
      {   
        var query="insert into ramesh2.baseballplayers values ('"+ eachrow._1  +"','"+ eachrow._2 +"')"
        stmt.executeUpdate(query)
     }
     con.close()
    }
  }

  //eof use spark and persist to Hive

}



