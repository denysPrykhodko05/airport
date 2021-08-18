package application

import models.AirportData
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import utils.ProcessUtils.processWithWindow

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

}
