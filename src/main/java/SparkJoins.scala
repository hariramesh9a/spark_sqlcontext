import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

//using spark sql join: join population.csv and countrycode.txt and see whihc country had max poulation in which year
object SparkJoins {

  def main(args: Array[String]): Unit = {
   
  country_count()
  }
  
  
   def country_count(): Unit = {
     val conf=new SparkConf().setAppName("count countries").setMaster("local[8]")
     val cont=new SparkContext(conf)
     
     var file=cont.textFile("src/main/resources/population.csv", 4)
     var words=file.flatMap { x => x.split(",")(0) }.map { line => (line,1) }.reduceByKey{case(x,y)=>x+y}
     println(words.first())    
     
     
     
     
     
     
    
  }
   
   def join():Unit={
      val conf = new SparkConf().setMaster("local[8]").setAppName("Joins")
    val sc = new SparkContext(conf)
    val sqlContext= new SQLContext(sc)
    val countryData = "src/main/resources/countrycode.txt"
    val popData = "src/main/resources/population.csv"
    val countrycodedata = sqlContext.read.format("com.databricks.spark.csv") 
    val cd=countrycodedata.schema(StructType(Array(StructField("code",StringType,true),StructField("seq",StringType,false)))).load(countryData)//.registerTempTable("countrycode")
    val cn=sqlContext.read.format("com.databricks.spark.csv").option("Header", "true").option("inferSchema", "true").load(popData)//.registerTempTable("pop")
    println(cn.printSchema())
    println(cd.printSchema())
    cn.join(cd, cd.col("code").equalTo(cn("code"))).groupBy("country").agg(min("Value").as("min_val")).coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("src/main/resources/test")
  
   }

}