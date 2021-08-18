package utils

import constants.Constants.{AIRPORT_CODE_COLUMN, USERNAME_COLUMN, VISIT_TIME_COLUMN}
import models.AirportData
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, count, max}

object ProcessUtils {
  def processWithWindow(inputDS: Dataset[AirportData]): Dataset[Row]={
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
  def processWithGroupBy(inputDS: Dataset[AirportData]): Dataset[Row]={
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
