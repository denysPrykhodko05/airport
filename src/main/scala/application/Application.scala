package application

import models.AirportData
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import utils.FileUtils.{readData, writeData}
import utils.ProcessUtils.processWithWindow

object Application {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("airport").master("local[*]").getOrCreate()
    val inputDS = readData("src/main/resources/input")(sparkSession)

    //Approach with window
    val processedDS = processWithWindow(inputDS)

    //Approach with groupBy
    //val processedDS = processWithGroupBy(inputDS)

    processedDS.show(false)
    writeData(processedDS,"src/main/resources/output")
  }

}
