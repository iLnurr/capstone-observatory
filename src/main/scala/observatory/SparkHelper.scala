package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SparkHelper {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._


  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[6]")
      .appName(this.getClass.getSimpleName)
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
  object StationContainer {
    def schema = StructType(
      Seq(
        StructField("id",StringType,nullable = false),
        StructField("lat",DoubleType,nullable = false),
        StructField("lon",DoubleType,nullable = false)
      )
    )
  }
  case class StationContainer(id: String, lat: Double, lon: Double)
  def stationsDF(stationsFile: String): DataFrame = {
    spark.read.schema(StationContainer.schema).csv(fsPath(stationsFile))
  }
  def stationsDS(stationsFile: String): Dataset[StationContainer] = {
    spark.read.csv(fsPath(stationsFile))
      .select(
        concat_ws("~", coalesce('_c0, lit("")), '_c1).alias("id"),
        '_c2.alias("lat").cast(DoubleType),
        '_c3.alias("lon").cast(DoubleType)
      )
      .where('_c2.isNotNull && '_c3.isNotNull && '_c2 =!= 0.0 && '_c3 =!= 0.0)
      .as[StationContainer]
  }

  //TEMPERATURE
  object TempContainer {
    def schema = StructType(
      Seq(
        StructField("id",StringType,nullable = false),
        StructField("day",IntegerType,nullable = false),
        StructField("month",IntegerType,nullable = false),
        StructField("temperature",DoubleType,nullable = false)
      )
    )
  }
  case class TempContainer(id: String, day: Int, month: Int, temperature: Double)
  def tempDF(tempFile: String): DataFrame = {
    spark.read.schema(TempContainer.schema).csv(fsPath(tempFile))
  }
  def tempDS(tempFile: String): Dataset[TempContainer] = {
    spark.read.csv(fsPath(tempFile))
      .select(
        concat_ws("~", coalesce('_c0, lit("")), '_c1).alias("id"),
        '_c3.alias("day").cast(IntegerType),
        '_c2.alias("month").cast(IntegerType),
        (('_c4 - 32) / 9 * 5).alias("temperature").cast(DoubleType)
      )
      .where('_c4.between(-200, 200))
      .as[TempContainer]
  }

  case class StationDateContainer(year: Int, month: Int, day: Int){
    def toLocalDate = LocalDate.of(year, month, day)
  }

  // StationsAndTempJoined
  case class StationsAndTempJoined(id: String, lat: Double, lon: Double, day: Int, month: Int, temperature: Double)

}
