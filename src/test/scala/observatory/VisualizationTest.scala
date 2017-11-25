package observatory


import observatory.Extraction._
import observatory.checkers.visCheck
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait VisualizationTest extends FunSuite with Checkers with TimeCheck {
  import SparkHelper._
  import Visualization._
  import math._

  private val stationsPath = "/stations.csv"
  private val tempPath = "/1975.csv"
  private val year = 1975

  private lazy val records = locateTemperatures(year, stationsPath, tempPath)
  private lazy val temperatures = locationYearlyAverageRecords(records)

  val location = Location(37,119)
  val locationAntiPod = Location(-37, -61)

  val location2 = Location(22.55, 23.783)

  test("distance") {
    assert(location == location.copy(), "location equal to self should be true")
    assert(location != location2, "location should not be equal to other")

    println(location)
    println(getAntipode(location))
    assert(getAntipode(location) == locationAntiPod, "func getAntipod should work correctly")

    assert(distance(location, location) == 0.0)
    assert(distance(location, locationAntiPod).toInt == 20015)
    assert(
      distance(location, locationAntiPod.copy(lat = - 38)).toInt == distance(location, locationAntiPod.copy(lat = -36)).toInt
    )
  }

  val p = 2
  def weight(other: Location): Temperature = {
    1 / pow(distance(location, other), p)
  }

  test("inverse distance weight") {
    println(weight(location))
    println(weight(locationAntiPod))
    println(weight(location2))
    println(weight(location.copy(lat = location.lat - 1)))
    println(weight(location.copy(lat = location.lat + 1)))
    println(weight(location.copy(lon = location.lon - 1)))
  }

  test("predict temperature should work correctly and fast") {
    val resords = locateTemperatures(year, stationsPath, tempPath)
    val temperatures = locationYearlyAverageRecords(records)

    val predicted = withTimeChecking(predictTemperature(temperatures, location))
    println(predicted)

    val tlSolution = withTimeChecking(visCheck.predictTemperature(temperatures, location))
    println(tlSolution)
  }

  private val points: Iterable[(Temperature, Color)] = {
    Iterable(
      ( 60, Color(255, 255, 255)),
      ( 32, Color(255,   0,   0)),
      ( 12, Color(255, 255,   0)),
      (  0, Color(  0, 255, 255)),
      (-15, Color(  0,   0, 255)),
      (-27, Color(255,   0, 255)),
      (-50, Color( 33,   0, 107)),
      (-60, Color(  0,   0,   0))
    )
  }
  test("interpolate") {
    val shuffled = points
    val sorted = shuffled.toSeq.sortBy(_._1)
    println(sorted)

    assert(interpolateColor(points,  60) == Color(255,255,255))
    assert(interpolateColor(points,   0) == Color(  0,255,255))
    assert(interpolateColor(points, -60) == Color(  0,  0,  0))

    println(interpolateColor(points, 6))

    println(interpolateColor(points, -6))

  }

  test("interpolate - tl") {
    val shuffled = points
    val sorted = shuffled.toSeq.sortBy(_._1)
    println(sorted)

    assert(visCheck.interpolateColor(points,  60) == Color(255,255,255))
    assert(visCheck.interpolateColor(points,   0) == Color(  0,255,255))
    assert(visCheck.interpolateColor(points, -60) == Color(  0,  0,  0))

    println(visCheck.interpolateColor(points, 6))

    println(visCheck.interpolateColor(points, -6))

  }

}
