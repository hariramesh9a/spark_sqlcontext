
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.hadoop.hive._



object TestSparKnHive {

  
  def main(args: Array[String]): Unit = {
    var drivername:String="org.apache.hadoop.hive.jdbc.HiveDriver"
    var sc= new SQLContext(new SparkContext("local[4]","Connect to remote hive"))
    val jdbcDF = sc.read.format("jdbc").option("driver", drivername) .options(
					Map("url" -> "jdbc:hive2://ehplbigi1002.nwie.net:2181,ehplbigi1003.nwie.net:2181,ehplbigi1005.nwie.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2","dbtable" -> "ramesh.test")).load()
							jdbcDF.show()
  }
}