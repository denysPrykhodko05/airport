package utils

import models.AirportData
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

object FileUtils {
  def readData(path: String)(sparkSession: SparkSession): Dataset[AirportData] = {
    import sparkSession.implicits._
    val schema = Encoders.product[AirportData].schema
    sparkSession.read.schema(schema).option("header", "true").csv(path)
      .as[AirportData]
  }

  def writeData[A](dataset: Dataset[A],path: String): Unit = {
    dataset.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(path)
  }
}
