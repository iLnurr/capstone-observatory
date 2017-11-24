package observatory

import java.nio.file.Paths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SparkHelper {

  val log = Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._


  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  def rddFrom(stationsFile: String): RDD[String] = {
    spark.sparkContext.textFile(fsPath(stationsFile))
  }

  //STATION
  object Station {
    def schema = StructType(
      Seq(
        StructField("stn",StringType,nullable = false),
        StructField("wban",StringType,nullable = true),
        StructField("lat",DoubleType,nullable = false),
        StructField("lon",DoubleType,nullable = false)
      )
    )
  }
  case class Station(stn: Option[String], wban: Option[String], lat: Option[Double], lon: Option[Double])
  def stationsDF(stationsFile: String): DataFrame = {
    spark.read.schema(Station.schema).csv(fsPath(stationsFile))
  }
  def stationsDS(stationsFile: String): Dataset[Station] = {
    spark.read.schema(Station.schema).csv(fsPath(stationsFile)).as[Station]
  }

  //TEMPERATURE
  object TempContainer {
    def schema = StructType(
      Seq(
        StructField("stn",StringType,nullable = false),
        StructField("wban",StringType,nullable = true),
        StructField("month",IntegerType,nullable = false),
        StructField("day",IntegerType,nullable = false),
        StructField("temperatureFh",DoubleType,nullable = false)
      )
    )
  }
  case class TempContainer(stn: Option[String], wban: Option[String], month: Option[Int], day: Option[Int], temperatureFh: Option[Double]) {
    def toCelsius() = (temperatureFh.get - 32) / 1.8
  }
  def tempDF(tempFile: String): DataFrame = {
    spark.read.schema(TempContainer.schema).csv(fsPath(tempFile))
  }
  def tempDS(tempFile: String): Dataset[TempContainer] = {
    spark.read.schema(TempContainer.schema).csv(fsPath(tempFile)).as[TempContainer]
  }

}
