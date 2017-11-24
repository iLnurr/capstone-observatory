package observatory

import java.time.LocalDate
import org.apache.log4j.{Level, Logger}

/**
  * 1st milestone: data extraction
  */
object Extraction {
  import SparkHelper._
  val log = Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stations = stationsDS(stationsFile)
    val temps = tempDS(temperaturesFile)

    temps.joinWith(stations, temps.col("id") === stations.col("id"), "inner").map { t =>
      val tempContainer = t._1
      val station = t._2
      (
        StationDate(year, tempContainer.month, tempContainer.day),
        Location(station.lat, station.lon),
        tempContainer.temperature
      )
    }.collect().map(t => (t._1.toLocalDate, t._2, t._3))
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val rdd = spark.sparkContext.parallelize(records.toSeq).map { t =>
      t._2 -> t._3
    }.persist()
    val result = rdd
      .mapValues(t => (t, 1))
      .reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .mapValues{
        case (temp, numb) => temp / numb
      }
    result.collect()
  }

}
