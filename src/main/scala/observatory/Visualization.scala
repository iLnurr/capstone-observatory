package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.math._

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    // https://en.wikipedia.org/wiki/Inverse_distance_weighting
    val p = 2
    def weight(other: Location): Temperature = {
      1 / pow(distance(location, other), p)
    }
    var weights = 0.0
    val u = temperatures.map { t =>
      val other = t._1
      val temp = t._2
      if (other == location) {
        0.0
      } else {
        val w = weight(other)
        weights += w
        w * temp
      }
    }.sum
    u / weights
  }

  // fi == lat and
  // theta = lon and
  // central angle = cAngle
  // https://en.wikipedia.org/wiki/Great-circle_distance
  def distance(loc1: Location, loc2: Location) = {
    val earthRadius = 6371 // km
    val cAngle = if (loc1 == loc2) {
      0
    } else if (getAntipode(loc1) == loc2) {
      Pi
    } else {
      acos((sin(toRadians(loc1.lat)) * sin(toRadians(loc2.lat))) + (cos(toRadians(loc1.lat)) * cos(toRadians(loc2.lat)) * cos(toRadians(loc1.lon - loc2.lon))))
    }
    cAngle * earthRadius
  }

  // https://ru.wikipedia.org/wiki/Географические_координаты
  def getAntipode(location: Location): Location = {
    location.copy(lat = -location.lat, lon = - (180 - abs(location.lon)))
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    // y - new Color
    // x - value
    // y0 - color before (bottom)
    // x0 - temp before
    // y1 - color after
    // x1 - temp after
    // y = y0 * (1 - (x - x0) / (x1 - x0)) + y1 * ((x - x0) / (x1 - x0))
    // https://en.wikipedia.org/wiki/Linear_interpolation

    def interpolate(y0: Color, x0: Temperature, y1: Color, x1: Temperature, x: Temperature) = {
      val xPart = (x - x0) / (x1 - x0)
      def eval(col0: Int, col1: Int): Int = col0 match {
        case _ if col0 == col1 =>
          col0
        case _ =>
          round((col0 * (1 - xPart)) + (col1 * xPart)).toInt
      }
      Color(eval(y0.red, y1.red), eval(y0.green, y1.green), eval(y0.blue, y1.blue))
    }

    val sorted = points.toList.sortBy(_._1)

    @scala.annotation.tailrec
    def find(list: List[(Temperature, Color)], before: (Temperature, Color)): ((Temperature, Color), (Temperature, Color)) = list match {
      case after :: Nil =>
        if (before._1 <= value && value <= after._1) before -> after
        else throw new IllegalArgumentException("not founded")
      case after :: xs =>
        if (before._1 <= value && value <= after._1) before -> after
        else find(xs, after)
    }

    val ((x0, y0),(x1, y1)) = find(sorted, sorted.head)

    interpolate(y0, x0, y1, x1, value)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    ???
  }

}

