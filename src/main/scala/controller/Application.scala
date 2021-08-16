package controller

import models.AirportData
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, count, first, max}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object Application {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("airport").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    val schema = Encoders.product[AirportData].schema
    val inputDS: Dataset[AirportData] = sparkSession.read.schema(schema).option("header", "true").csv("src/main/resources")
      .as[AirportData]

    //val win = Window.partitionBy(col("username"), col("airportCode"))

   /* inputDS.withColumn("distinct", count("*").over(win)).distinct().show(false)

    inputDS.withColumn("distinct", count("username").over(win))
      .withColumn("visitTime", max("visitTime")).dropDuplicates("username").show(false)
*/
    val groupedDS = inputDS.groupBy("username", "airportCode")
      .agg(max("visitTime").alias("visitTime"))
      .groupBy("airportCode")
      .agg(max("visitTime").alias("recent visit"),
      count("airportCode").alias("amount of visit"))
      .orderBy(asc("amount of visit"))
      .filter(col("amount of visit").>(0))
      .limit(5)

    groupedDS.show(false)
  }
}
