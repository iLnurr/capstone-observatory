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
    val stations = stationsDS(stationsFile).filter(s => s.stn.isDefined && s.lat.isDefined && s.lon.isDefined).collect()
    val temps = tempDS(temperaturesFile).filter(tc => tc.stn.isDefined && tc.day.isDefined && tc.month.isDefined && tc.temperatureFh.isDefined)
    val r = temps.map { tc =>
      (LocalDate.of(year, tc.month.get.toInt,  tc.day.get.toInt),
        stations.find(s => s.stn == tc.stn).map(s => Location(s.lat.get.toDouble, s.lon.get.toDouble)),
        tc.toCelsius())
    }.filter(t => t._2.isDefined)
      .map(t => (t._1, t._2.get, t._3))

    r.collect()
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    ???
  }

}
