import com.holdenkarau.spark.testing.{DatasetSuiteBase, SharedSparkContext}
import models.AirportData
import org.apache.spark.sql.Dataset
import org.junit.Before
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import utils.{FileUtils, ProcessUtils}

@RunWith(classOf[JUnitRunner])
class ProcessUtilsTest extends FlatSpec with SharedSparkContext with DatasetSuiteBase with BeforeAndAfterAll{

  @Before
  override def beforeAll(): Unit = super.beforeAll()

  it should "output correct after process with window" in{
    val inputDS: Dataset[AirportData] = FileUtils.readData("src/test/resources/input/input.csv")(spark)

    val expected = spark.read.option("header", "true").csv("src/test/resources/output")
    val actual = ProcessUtils.processWithWindow(inputDS)

    assertDataFrameDataEquals(expected,actual)
  }

  it should "output correct after process with groupBy" in {
    val inputDS: Dataset[AirportData] = FileUtils.readData("src/test/resources/input")(spark)

    val expected = spark.read.option("header", "true").csv("src/test/resources/output")
    val actual = ProcessUtils.processWithGroupBy(inputDS)

    assertDataFrameDataEquals(expected,actual)
  }

}
