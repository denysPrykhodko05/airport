# Airports data processor

There are two data processing approaches implemented here with window aggregation functions
~~~~
val win = Window.partitionBy(col(USERNAME_COLUMN), col(AIRPORT_CODE_COLUMN))

    inputDS.repartition(col(USERNAME_COLUMN), col(AIRPORT_CODE_COLUMN))
      .withColumn("recentTime", max(VISIT_TIME_COLUMN).over(win))
      .withColumn("amount of visit", count("*").over(win))
...
~~~~
and using groupBy.

~~~~
inputDS.groupBy(USERNAME_COLUMN, AIRPORT_CODE_COLUMN)
      .agg(max(VISIT_TIME_COLUMN).alias(VISIT_TIME_COLUMN),
        count(AIRPORT_CODE_COLUMN).alias("amount of visit"))
...
~~~~

Also these two methods are covered by tests.

~~~~
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
~~~~

The input and output files for this application are located in resources folder.