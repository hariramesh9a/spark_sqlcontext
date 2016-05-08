import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object testSpark {

	def main(args: Array[String]): Unit = {

			var sc= new SQLContext(new SparkContext("local[4]","My first pgm"))


					val df = sc.read.format("com.databricks.spark.csv").option("header", "true")
					.option("inferSchema", "true").load("src/main/resources/sample.csv")
					println( df.groupBy("county").max("eq_site_limit").show())   
					println(df.groupBy("eq_site_limit").max("eq_site_limit").show() )
					df.groupBy("county").max("eq_site_limit").write.format("com.databricks.spark.csv").option("header","true").save("src/main/resources/sample_result.csv")
	
	
					val df_new = sc.read.format("com.databricks.spark.csv").option("header", "true")
					.option("inferSchema", "true").load("src/main/resources/sample_result.csv")
	     
      	println(df_new.show())
	}

}