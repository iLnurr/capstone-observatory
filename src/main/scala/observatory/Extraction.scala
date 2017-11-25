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

    val j = stations.join(temps, usingColumn = "id").as[StationsAndTempJoined].map { t =>
      (
        StationDate(year, t.month, t.day),
        Location(t.lat, t.lon),
        t.temperature
      )
    }

    j.collect().map(t => (t._1.toLocalDate, t._2, t._3))
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records
      .par
      .groupBy(_._2)
      .mapValues(
        l => l.foldLeft(0.0)(
          (t,r) => t + r._3) / l.size
      )
      .seq
  }

}
