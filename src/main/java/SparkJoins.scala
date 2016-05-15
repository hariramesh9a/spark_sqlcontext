
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField


object SparkJoins {
  
  def main(args: Array[String]): Unit = {
  
    var sc= new SparkContext("local[4]","Connect to remote hive")
    val data="src/main/resources/hello.txt"
    val distData=sc.textFile(data)
    val coll=distData.map(_.split(",")).map(r=>r(1))
    println(coll.count())
    coll.foreach(println)
    println(coll.toDebugString)

    
  }
  
  
}