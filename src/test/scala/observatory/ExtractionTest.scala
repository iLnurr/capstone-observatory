package observatory

import org.apache.spark.sql.Encoder
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

trait ExtractionTest extends FunSuite {
  import SparkHelper._

  private val stationsPath = "/stations.csv"
  private val tempPath = "/1975.csv"

  test("test should be invoked") {
    println("ok")
  }

  test("correct read rdd from resource") {
    val rdd = rddFrom(stationsPath)
    println()
    val stationsCount = rdd.count()
    println(stationsCount)
    assert(stationsCount == 29444, "Stations count in file `/stations.csv` should be 29444")
  }

  test("correct read stations dataframe from csv") {
    val df = stationsDF(stationsPath)
    println(df.first())
    df.printSchema()
    println()
    val stationsCount = df.count()
    println(stationsCount)
    assert(stationsCount == 29444, "Stations count in file `/stations.csv` should be 29444")
  }

  test("correct read stations dataset from csv") {
    val ds = stationsDS(stationsPath)
    println(ds.schema)
    println(ds.first())
    println()
    val stationsCount = ds.count()
    println(stationsCount)
    assert(stationsCount == 29444, "Stations count in file `/stations.csv` should be 29444")
  }

  test("correct read temp DF from csv") {
    val df = tempDF(tempPath)
    println(df.first())
    df.printSchema()
    println()
    val tempCount = df.count()
    println(tempCount)
    assert(tempCount == 2190974, "Row count in file `/1975.csv` should be 2190974")
  }

  test("correct read temp DS from csv") {
    val ds = tempDS(tempPath)
    println(ds.first())
    ds.printSchema()
    println()
    val tempCount = ds.count()
    println(tempCount)
    assert(tempCount == 2190974, "Row count in file `/1975.csv` should be 2190974")
  }
}