package miniaicoding.spark.sparksql

import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkSQLMySQL {
  def main(args: Array[String]): Unit = {
    val sparkSxn: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark SQL with MySQL")
      .getOrCreate()
    sparkSxn.sparkContext.setLogLevel("ERROR")
    try {
      val dbDriver = "com.mysql.jdbc.Driver"
      val dbUrl = "jdbc:mysql://localhost:3306/mysql"
      val dBuser = "root"
      val dBpass = ""
      val srcTable ="user"
      val sourceDf = sparkSxn.read.format("jdbc")
        .option("driver", dbDriver)
        .option("url", dbUrl)
        .option("dbtable", srcTable)
        .option("user", dBuser)
        .option("password", dBpass)
        .load()
      sourceDf.show()

      // Sedning to DHFS file

    } catch {
      case exObj : Throwable => println("Database Issue ", exObj)
    }
  }
}