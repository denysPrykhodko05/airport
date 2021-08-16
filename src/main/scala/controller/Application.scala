package controller

import constants.Constants.{AIRPORT_CODE_COLUMN, USERNAME_COLUMN, VISIT_TIME_COLUMN}
import models.AirportData
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, count, max}
import org.apache.spark.sql.{Dataset, Encoders, Row, SaveMode, SparkSession}

object Application {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("airport").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    val schema = Encoders.product[AirportData].schema
    val inputDS: Dataset[AirportData] = sparkSession.read.schema(schema).option("header", "true").csv("src/main/resources/input")
      .as[AirportData]

    //Approach with window
    val processedDS = processWithWindow(inputDS)

    //Approach with groupBy
    //val processedDS = processWithGroupBy(inputDS)

    processedDS.show(false)

    processedDS.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("src/main/resources/output")
  }

  private def processWithWindow(inputDS: Dataset[AirportData]): Dataset[Row]={
    val win = Window.partitionBy(col(USERNAME_COLUMN), col(AIRPORT_CODE_COLUMN))

    inputDS.repartition(col(USERNAME_COLUMN), col(AIRPORT_CODE_COLUMN))
      .withColumn("recentTime", max(VISIT_TIME_COLUMN).over(win))
      .withColumn("amount of visit", count("*").over(win))
      .dropDuplicates("recentTime")
      .filter(col("amount of visit").<(6))
      .groupBy( AIRPORT_CODE_COLUMN)
      .agg(max("recentTime").alias("recentTime"),
        count(AIRPORT_CODE_COLUMN).alias("amount of visit"))
      .orderBy(col("amount of visit").asc)
      .filter(col("amount of visit").>(0))
      .limit(5)
  }
  private def processWithGroupBy(inputDS: Dataset[AirportData]): Dataset[Row]={
    inputDS.groupBy(USERNAME_COLUMN, AIRPORT_CODE_COLUMN)
      .agg(max(VISIT_TIME_COLUMN).alias(VISIT_TIME_COLUMN),
        count(AIRPORT_CODE_COLUMN).alias("amount of visit"))
      .filter(col("amount of visit").<(6))
      .groupBy(AIRPORT_CODE_COLUMN)
      .agg(max(VISIT_TIME_COLUMN).alias("recent visit"),
        count(AIRPORT_CODE_COLUMN).alias("amount of visit"))
      .orderBy(asc("amount of visit"))
      .filter(col("amount of visit").>(0))
      .limit(5)
  }

}
