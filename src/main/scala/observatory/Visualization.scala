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
    ???
  }


  // fi == lat and
  // theta = lon and
  // central angle = cAngle
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

  def getAntipode(location: Location): Location = {
    location.copy(lat = -location.lat, lon = - (180 - abs(location.lon)))
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    ???
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

